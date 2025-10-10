from woocommerce import API
import json
import os, csv, shutil, logging, requests, mimetypes, glob
import logging
import html
import re
from datetime import datetime
from typing import Dict, Tuple, List, Optional, Any
from PIL import Image
from bs4 import BeautifulSoup
import time


def load_settings():
    """
    Завантаження конфігурації з settings.json
    """
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "settings.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Помилка: файл конфігурації не знайдено за шляхом: {config_path}")
        return None
    except json.JSONDecodeError:
        print(f"❌ Помилка: файл конфігурації пошкоджений: {config_path}")
        return None



def get_wc_api(settings):
    """
    Завантаження конфігурації з settings.json
    """
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "settings.json")
    with open(config_path, "r", encoding="utf-8") as f:
        settings = json.load(f)

    wcapi = API(
        url=settings["url"],
        consumer_key=settings["consumer_key"],
        consumer_secret=settings["consumer_secret"],
        version="wc/v3",
        timeout=120,
        query_string_auth=True
    )
    return wcapi

def check_version():
    wcapi = get_wc_api()
    response = wcapi.get("system_status")
    if response.status_code == 200:
        data = response.json()
        print("WooCommerce version:", data.get("environment", {}).get("version"))
    else:
        print("Error:", response.status_code, response.text)



def setup_new_log_file():
    """
    Перейменовує існуючий лог-файл та налаштовує новий,
    використовуючи шлях з налаштувань.
    """
    settings = load_settings()
    if not settings or "paths" not in settings or "main_log_file" not in settings["paths"]:
        print("❌ Не знайдено шлях до лог-файлу в налаштуваннях.")
        return

    current_log_path = os.path.join(os.path.dirname(__file__), "..", settings["paths"]["main_log_file"])
    log_dir = os.path.dirname(current_log_path)
    
    os.makedirs(log_dir, exist_ok=True)
    
    if os.path.exists(current_log_path):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name, file_extension = os.path.splitext(os.path.basename(current_log_path))
        new_log_path = os.path.join(log_dir, f"{file_name}_{timestamp}{file_extension}")
        try:
            os.rename(current_log_path, new_log_path)
            print(f"✅ Старий лог-файл перейменовано на {os.path.basename(new_log_path)}")
        except OSError as e:
            print(f"❌ Помилка при перейменуванні лог-файлу: {e}")

    logging.basicConfig(
        filename=current_log_path,
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filemode='a'
    )
    logging.info("--- Новий сеанс логування розпочато ---")

def log_message_to_existing_file():
    """
    Налаштовує логування для дописування в існуючий файл,
    використовуючи шлях з налаштувань.
    """
    settings = load_settings()
    if not settings or "paths" not in settings or "main_log_file" not in settings["paths"]:
        print("❌ Не знайдено шлях до лог-файлу в налаштуваннях.")
        return

    current_log_path = os.path.join(os.path.dirname(__file__), "..", settings["paths"]["main_log_file"])
    log_dir = os.path.dirname(current_log_path)
    
    os.makedirs(log_dir, exist_ok=True)

    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            filename=current_log_path,
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            filemode='a'
        )
    logging.info("--- Повідомлення додано до існуючого логу ---")



def check_csv_data(profile_id):
    """
    Перевіряє CSV-файл на відповідність правилам, визначеним у settings.json.
    
    Args:
        profile_id (str): ID профілю перевірки з 'validation_profiles' в settings.json.
    
    Returns:
        bool: True, якщо перевірка пройшла успішно, інакше False.
    """
    # 1. Завантаження налаштувань
    # Цей блок відповідає за завантаження конфігурації з файлу settings.json
    # та перевіряє, чи існує вказаний профіль валідації.
    # Якщо налаштування не завантажено або профіль відсутній, функція завершує роботу.
    
    log_message_to_existing_file()
    
    try:
        with open(os.path.join(os.path.dirname(__file__), "..", "config", "settings.json"), "r", encoding="utf-8") as f:
            settings = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"❌ Помилка при завантаженні конфігурації: {e}")
        return False
        
    profiles = settings.get("validation_profiles", {})
    if profile_id not in profiles:
        logging.error(f"❌ Не знайдено профіль валідації з ID '{profile_id}' в settings.json.")
        return False
    
    # 2. Отримання даних профілю
    # Отримуємо шлях до файлу та правила валідації для обраного профілю.
    profile = profiles[profile_id]
    csv_path_relative = profile.get("path")
    validation_rules = profile.get("rules")
    
    if not csv_path_relative or validation_rules is None:
        logging.error("❌ Неповні дані в профілі валідації.")
        return False
        
    # 3. Перевірка наявності файлу
    # Формуємо повний шлях до файлу та перевіряємо його існування на диску.
    base_dir = os.path.join(os.path.dirname(__file__), "..")
    full_csv_path = os.path.join(base_dir, csv_path_relative)
    
    if not os.path.exists(full_csv_path):
        logging.error(f"❌ Помилка: файл для перевірки не знайдено.")
        return False
        
    logging.info(f"🔎 Початок перевірки файлу")
    
    # 4. Читання та валідація даних
    # Відкриваємо файл та починаємо ітерацію по його вмісту.
    try:
        with open(full_csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            try:
                headers = next(reader)
            except StopIteration:
                logging.error("❌ Файл порожній. Відсутні заголовки.")
                return False
            
            # 5. Оновлена логіка перевірки заголовків
            # Перевіряємо, чи всі очікувані колонки присутні у заголовках файлу
            rule_columns = list(validation_rules.keys())
            headers_set = set(headers)
            for col_name in rule_columns:
                if col_name not in headers_set:
                    logging.error(f"❌ Помилка: очікувана колонка '{col_name}' відсутня у файлі.")
                    return False
            
            # Створюємо словник для швидкого доступу до індексів колонок
            header_map = {name: index for index, name in enumerate(headers)}
            
            # 6. Валідація кожного рядка
            for i, row in enumerate(reader):
                row_number = i + 2
                if not row or all(not col.strip() for col in row):
                    logging.info(f"✅ Рядок {row_number} порожній або містить лише пробіли. Пропускаю.")
                    continue
                
                # 7. Валідація кожного поля, яке є в правилах
                for col_name, rule_type in validation_rules.items():
                    try:
                        col_index = header_map.get(col_name)
                        if col_index is None:
                            # Це мало бути спіймано на етапі 5, але це додаткова перестраховка
                            continue
                        
                        if col_index >= len(row):
                            logging.error(f"❌ Рядок {row_number}: Рядок коротший за кількість очікуваних колонок.")
                            return False
                        
                        value = row[col_index].strip()

                        # 7.0. Перевірка на обов’язковість заповнення
                        if rule_type == "not_empty":
                            if not value:
                                logging.error(f"❌ Рядок {row_number}, колонка '{col_name}': поле не повинно бути порожнім.")
                                return False
                            continue  # не перевіряємо далі
                        
                        # 7.1. Валідація цілих чисел
                        if rule_type == "integer":
                            if not value:
                                logging.error(f"❌ Рядок {row_number}, колонка '{col_name}': очікується ціле число, але поле порожнє.")
                                return False
                            if not value.lstrip('-').isdigit():
                                logging.error(f"❌ Рядок {row_number}, колонка '{col_name}': очікується ціле число, але отримано '{value}'.")
                                return False

                        # 7.2. Валідація значень зі списку
                        elif isinstance(rule_type, list):
                            if not value:
                                logging.error(f"❌ Рядок {row_number}, колонка '{col_name}': очікується одне зі значень {rule_type}, але поле порожнє.")
                                return False
                            if value not in rule_type:
                                logging.error(f"❌ Рядок {row_number}, колонка '{col_name}': очікується одне зі значень {rule_type}, але отримано '{value}'.")
                                return False
                        
                        # 7.3. Валідація формату дати-часу
                        elif rule_type == "datetime":
                            if value:
                                try:
                                    datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
                                except ValueError:
                                    logging.error(f"❌ Рядок {row_number}, колонка '{col_name}': невірний формат дати-часу. Очікується 'YYYY-MM-DDTHH:MM:SS', але отримано '{value}'.")
                                    return False

                        # 7.4. Валідація цілих чисел (допускає порожнє поле)
                        if rule_type == "integer_or_empty":
                            if value == "":
                                continue  # Порожнє значення — дозволене
                            if not value.lstrip('-').isdigit():
                                logging.error(f"❌ Рядок {row_number}, колонка '{col_name}': очікується ціле число або порожнє поле, але отримано '{value}'.")
                                return False

                        # 7.5. Валідація чисел з плаваючою комою (float) (допускає порожнє поле)
                        elif rule_type == "float_or_empty":
                            if value == "":
                                continue  # дозволяємо пусте поле

                            # Дозволяємо європейський формат з комою — замінюємо на крапку
                            normalized_value = value.replace(",", ".")
                            try:
                                float(normalized_value)
                            except ValueError:
                                logging.error(
                                    f"❌ Рядок {row_number}, колонка '{col_name}': очікується число (float) або порожнє поле, "
                                    f"але отримано '{value}'."
                                )
                                return False
                                    
                    except (ValueError, IndexError):
                        logging.error(f"❌ Непередбачена помилка в рядку {row_number}. Перевірка зупинена.")
                        return False

    except Exception as e:
        logging.error(f"❌ Виникла невідома помилка під час читання CSV: {e}", exc_info=True)
        return False
        
    logging.info(f"✅ Перевірка файлу пройшла успішно.")
    return True

def get_config_path(filename):
    """Повертає повний шлях до файлу конфігурації."""
    # Припускаємо, що config знаходиться на один рівень вище від scr
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_dir = os.path.abspath(os.path.join(current_dir, '..', 'config'))
    return os.path.join(config_dir, filename)



def load_attributes_csv():
    """
    Завантажує правила заміни атрибутів з attribute.csv (гібридна блочна структура).
    Повертає:
    1. replacements_map: Словник {col_index: {original_value: new_value}} для швидкого пошуку.
    2. raw_data: Список сирих рядків для збереження структури файлу.
    """
    attribute_path = get_config_path('attribute.csv')
    replacements_map = {}
    raw_data = []          
    
    # Стандартний заголовок
    default_header = ["column_number", "attr_site_name", "atr_a", "atr_b", "atr_c", "atr_d", "atr_e", "atr_f", "atr_g", "atr_h", "atr_i"]
    current_col_index = None # Відстежуємо поточний блок

    try:
        with open(attribute_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            try:
                header = next(reader)
                raw_data.append(header)
                max_row_len = len(header)
            except StopIteration:
                return {}, [default_header]

            for row in reader:
                # Нормалізуємо довжину рядка
                row = row[:max_row_len] + [''] * (max_row_len - len(row))
                raw_data.append(row)
                
                # 1. Якщо це рядок-заголовок (наприклад, "27",,,,,)
                if row and row[0].strip().isdigit():
                    try:
                        current_col_index = int(row[0].strip())
                        if current_col_index not in replacements_map:
                            replacements_map[current_col_index] = {}
                    except ValueError:
                        current_col_index = None
                        continue
                
                # 2. Якщо це рядок-правило (наприклад, ,,,чорний,,)
                elif current_col_index is not None and len(row) >= 3:
                    
                    # Стандартизоване значення знаходиться в колонці 1 (attr_site_name)
                    new_value = row[1].strip() 
                    
                    # Переглядаємо всі значення постачальників (починаючи з індексу 2)
                    for original in row[2:]:
                        original = original.strip().lower()
                        if original:
                            # Ключ - оригінал (lower), Значення - заміна (з attr_site_name)
                            replacements_map[current_col_index][original] = new_value

        return replacements_map, raw_data
    
    except FileNotFoundError:
        logging.warning(f"Файл атрибутів 'attribute.csv' не знайдено. Буде створено новий.")
        return {}, [default_header]
    except Exception as e:
        logging.error(f"Виникла помилка при завантаженні attribute.csv: {e}")
        return {}, [default_header]

def save_attributes_csv(raw_data):
    """
    Зберігає оновлені сирі дані у attribute.csv.
    """
    attribute_path = get_config_path('attribute.csv')
    try:
        # 'newline=''' важливий для коректного збереження CSV на різних ОС
        with open(attribute_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(raw_data)
        logging.info("Файл атрибутів attribute.csv оновлено.")
    except Exception as e:
        logging.error(f"Помилка при збереженні файлу атрибутів attribute.csv: {e}")



def load_category_csv():
    """
    Завантажує правила заміни категорій з category.csv.
    Повертає:
    1. category_map: Словник {supplier_id: {(name1, name2, name3): category_value}}
    2. raw_data: Список сирих рядків для збереження структури файлу.
    """
    category_path = get_config_path('category.csv')
    category_map = {}
    raw_data = []          
    
    default_header = ["postachalnyk", "name_1", "name_2", "name_3", "category"]
    current_supplier_id = None

    try:
        with open(category_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            try:
                header = next(reader)
                raw_data.append(header)
                max_row_len = len(header)
            except StopIteration:
                return {}, [default_header]

            for row in reader:
                row = row[:max_row_len] + [''] * (max_row_len - len(row))
                raw_data.append(row)
                
                # 1. Якщо це рядок-заголовок постачальника (наприклад, "1",,,,)
                if row and row[0].strip().isdigit():
                    try:
                        current_supplier_id = int(row[0].strip())
                        if current_supplier_id not in category_map:
                            category_map[current_supplier_id] = {}
                    except ValueError:
                        current_supplier_id = None
                        continue
                
                # 2. Якщо це рядок-правило (наприклад, ,"Ляльки","Кукли","Надувні","Надувні ляльки")
                elif current_supplier_id is not None and len(row) >= 5:
                    
                    # Ключі для мапи: (name_1, name_2, name_3) - всі в нижньому регістрі
                    key_tuple = (
                        row[1].strip().lower(), 
                        row[2].strip().lower(), 
                        row[3].strip().lower()
                    )
                    
                    # Значення: category (без зміни регістру)
                    category_value = row[4].strip()
                    
                    category_map[current_supplier_id][key_tuple] = category_value

        return category_map, raw_data
    
    except FileNotFoundError:
        logging.warning(f"Файл категорій 'category.csv' не знайдено. Буде створено новий.")
        return {}, [default_header]
    except Exception as e:
        logging.error(f"Виникла помилка при завантаженні category.csv: {e}")
        return {}, [default_header]

def save_category_csv(raw_data):
    """
    Зберігає оновлені сирі дані у category.csv.
    """
    category_path = get_config_path('category.csv')
    try:
        with open(category_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(raw_data)
        logging.info("Файл категорій category.csv оновлено.")
    except Exception as e:
        logging.error(f"Помилка при збереженні файлу категорій category.csv: {e}")

def load_poznachky_csv():
    """
    Завантажує статичний список позначок з poznachky.csv.
    Повертає:
    1. poznachky_list: Список унікальних позначок (у нижньому регістрі).
    """
    poznachky_path = get_config_path('poznachky.csv')
    poznachky_list = []
    
    try:
        with open(poznachky_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            try:
                # Пропускаємо заголовок "Poznachky"
                next(reader) 
            except StopIteration:
                return []

            for row in reader:
                if row and row[0].strip():
                    # Зберігаємо позначки у нижньому регістрі для універсального порівняння
                    poznachky_list.append(row[0].strip().lower())

        # Сортуємо від найдовших до найкоротших, щоб знайти найкраще співпадіння
        poznachky_list.sort(key=len, reverse=True)
        
        return poznachky_list
    
    except FileNotFoundError:
        logging.warning(f"Файл позначок 'poznachky.csv' не знайдено.")
        return []
    except Exception as e:
        logging.error(f"Виникла помилка при завантаженні poznachky.csv: {e}")
        return []



def clear_directory(folder_path: str):
    """Очищає або створює директорію."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path, exist_ok=True)
        return
    for item in os.listdir(folder_path):
        path = os.path.join(folder_path, item)
        try:
            if os.path.isfile(path) or os.path.islink(path):
                os.unlink(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
        except Exception as e:
            logging.error(f"❌ Не вдалося видалити {path}: {e}")

def move_gifs(src: str, dest: str) -> int:
    """Переміщує всі GIF із src у dest."""
    moved = 0
    os.makedirs(dest, exist_ok=True)
    for root, _, files in os.walk(src):
        for f in files:
            if f.lower().endswith('.gif'):
                src_path = os.path.join(root, f)
                rel = os.path.relpath(src_path, src)
                dest_path = os.path.join(dest, rel)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                shutil.move(src_path, dest_path)
                moved += 1
    logging.info(f"🟣 Переміщено {moved} GIF-файлів.")
    return moved

def convert_to_webp_square(src: str, dest: str) -> int:
    """
    Конвертує JPG/PNG → WEBP, вирівнює зображення до квадрату
    та коректно обробляє прозорість (RGBA / палітрові P-зображення).
    """
    import os
    import logging
    from PIL import Image

    converted = 0

    for root, _, files in os.walk(src):
        rel = os.path.relpath(root, src)
        out_dir = os.path.join(dest, rel)
        os.makedirs(out_dir, exist_ok=True)

        for f in files:
            if not f.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                continue

            try:
                img_path = os.path.join(root, f)
                img = Image.open(img_path)

                # 🔹 Конвертація кольорового режиму (для уникнення warning)
                if img.mode == "P":
                    img = img.convert("RGBA")
                elif img.mode not in ("RGB", "RGBA"):
                    img = img.convert("RGB")

                w, h = img.size
                max_side = max(w, h)

                # 🔹 Якщо зображення має альфа-канал — створюємо прозоре полотно
                if img.mode == "RGBA":
                    canvas = Image.new("RGBA", (max_side, max_side), (255, 255, 255, 0))
                else:
                    canvas = Image.new("RGB", (max_side, max_side), (255, 255, 255))

                # Центрування
                canvas.paste(img, ((max_side - w) // 2, (max_side - h) // 2))

                # 🔹 Збереження у форматі WEBP
                new_name = os.path.splitext(f)[0] + '.webp'
                out_path = os.path.join(out_dir, new_name)
                canvas.save(out_path, 'webp', quality=90)

                converted += 1

            except Exception as e:
                logging.error(f"❌ WEBP-конвертація '{f}' не вдалася: {e}")

    logging.info(f"🟢 WEBP-конвертовано {converted} зображень.")
    return converted

def download_product_images(url: str, sku: str, category: str, base_path: str, cat_map: Dict[str, str]) -> List[str]:
    """Завантажує всі зображення товару з URL."""
    cat_slug = cat_map.get(category.strip()) or category.strip().lower().replace(' ', '_').replace(',', '')
    dest = os.path.join(base_path, cat_slug)
    os.makedirs(dest, exist_ok=True)

    try:
        page = requests.get(url, timeout=10)
        page.raise_for_status()
    except Exception as e:
        logging.warning(f"⚠️ Не вдалося завантажити сторінку {url}: {e}")
        return []

    soup = BeautifulSoup(page.content, 'html.parser')
    links = {a.get('href') for a in soup.find_all('a', class_='thumb_image_container') if a.get('href')}
    files = []

    for i, img_url in enumerate(links, 1):
        try:
            r = requests.get(img_url, timeout=10)
            r.raise_for_status()
            mime = r.headers.get('Content-Type')
            ext = mimetypes.guess_extension(mime) or '.jpg'
            fname = f"{sku}-{i}{ext}"
            with open(os.path.join(dest, fname), 'wb') as f:
                f.write(r.content)
            files.append(fname)
        except Exception:
            continue

    # 🟢 Логування результату
    logging.info(f"📸 Завантажено {len(files)} зображень для SKU {sku}: {', '.join(files)}")

    return files

def sync_webp_column(sl_path: str, webp_path: str, col_index: int, sku_index: int) -> int:
    """Оновлює колонку WEBP/GIF-списків у CSV."""
    with open(sl_path, 'r', encoding='utf-8') as f:
        reader = list(csv.reader(f))
    if not reader:
        return 0

    header, *rows = reader
    sku_map = {}
    for root, _, files in os.walk(webp_path):
        for f in files:
            if '-' in f and f.lower().endswith(('.webp', '.gif')):
                sku = f.split('-')[0]
                sku_map.setdefault(sku, []).append(f)

    updated = 0
    for row in rows:
        if len(row) <= max(col_index, sku_index):
            row.extend([''] * (max(col_index, sku_index) + 1 - len(row)))
        sku = row[sku_index].strip()
        if sku in sku_map:
            row[col_index] = ', '.join(sorted(sku_map[sku]))
            updated += 1
    with open(sl_path, 'w', encoding='utf-8', newline='') as f:
        csv.writer(f).writerows([header] + rows)
    logging.info(f"🔁 Оновлено {updated} SKU у колонці WEBP.")
    return updated

def copy_to_site(src: str, dest: str):
    """Копіює WEBP/GIF до фінальної директорії з правами."""
    uid, gid = 33, 33
    fperm, dperm = 0o644, 0o755
    copied = 0

    for root, _, files in os.walk(src):
        rel = os.path.relpath(root, src)
        out_dir = os.path.join(dest, rel)
        os.makedirs(out_dir, mode=dperm, exist_ok=True)
        for f in files:
            if not f.lower().endswith(('.webp', '.gif')):
                continue
            src_f = os.path.join(root, f)
            dst_f = os.path.join(out_dir, f)
            shutil.copy2(src_f, dst_f)
            try:
                os.chown(dst_f, uid, gid)
                os.chmod(dst_f, fperm)
                copied += 1
            except PermissionError:
                logging.warning(f"⚠️ Немає прав для зміни власника {dst_f}")
    logging.info(f"📦 Скопійовано {copied} файлів у {dest}.")
    return copied








def _process_batch_update(wcapi: Any, batch_data: List[Dict[str, Any]], errors_list: List[str]) -> int:
    """Виконує пакетний запит 'update' до WooCommerce API."""
    
    payload = {"update": batch_data}
    
    try:
        logging.info(f"Надсилаю пакет на оновлення ({len(batch_data)} товарів)...")
        
        response = wcapi.post("products/batch", data=payload) 
        
        if response.status_code == 200:
            result = response.json()
            updated_count = len(result.get('update', []))
            
            # Детальне логування помилок, які повернув API
            api_errors = result.get('errors', [])
            if api_errors:
                for err in api_errors:
                    err_msg = f"API-Помилка (ID: {err.get('id', 'N/A')}): {err.get('message', 'Невідома помилка')}"
                    errors_list.append(err_msg)
                    logging.error(err_msg)
                
            logging.info(f"✅ Пакет оновлено. Успішно оброблено API: {updated_count} товарів. Помилок у пакеті: {len(api_errors)}")
            return updated_count
        else:
            err_msg = f"❌ Критична помилка API ({response.status_code}) при пакетному оновленні. Помилка: {response.text[:200]}..."
            errors_list.append(err_msg)
            logging.critical(err_msg)
            return 0
            
    except Exception as e:
        err_msg = f"❌ Непередбачена помилка під час відправки пакету: {e}"
        errors_list.append(err_msg)
        logging.critical(err_msg, exc_info=True)
        return 0
    

def find_media_ids_for_sku(wcapi, sku: str, uploads_path: str) -> List[Dict[str, Any]]:
    """
    Знаходить усі зображення для SKU у uploads_path та повертає список ID для WooCommerce.
    Підтримує різні розширення та підпапки.
    """
    def _get_media_id_by_filename(filename: str) -> int | None:
        """Внутрішня функція: шукає ID медіа за slug або title"""
        import requests

        file_slug = os.path.splitext(filename)[0]

        # Пошук за slug
        try:
            response = wcapi.get("media", params={'search': file_slug, 'per_page': 1, 'orderby': 'slug'})
            if response.status_code == 200:
                items = response.json()
                if items:
                    item = items[0]
                    if item.get('slug') == file_slug or item.get('title', {}).get('rendered') == filename:
                        return item['id']
        except Exception as e:
            logging.error(f"Помилка при пошуку медіа ID для {filename} (slug): {e}")

        # Пошук за точним title
        try:
            response = wcapi.get("media", params={'search': filename, 'per_page': 1, 'orderby': 'title'})
            if response.status_code == 200:
                items = response.json()
                if items:
                    item = items[0]
                    if item.get('title', {}).get('rendered') == filename:
                        return item['id']
        except Exception as e:
            logging.error(f"Помилка при пошуку медіа ID для {filename} (title): {e}")

        logging.warning(f"⚠️ Медіа ID для файлу '{filename}' не знайдено.")
        return None

    media_ids = []
    pattern = os.path.join(uploads_path, '**', f'{sku}*.*')
    files = glob.glob(pattern, recursive=True)
    for file_path in files:
        filename = os.path.basename(file_path)
        media_id = _get_media_id_by_filename(filename)
        if media_id:
            media_ids.append({"id": media_id})

    if not media_ids:
        logging.warning(f"⚠️ SKU {sku}: Не знайдено зображень у '{uploads_path}'")
    return media_ids



# --- ДОПОМІЖНА ФУНКЦІЯ ДЛЯ ПАКЕТНОГО ЗАПИСУ (Створення) (без змін) ---
def _process_batch_create(wcapi: Any, batch_data: List[Dict[str, Any]], errors_list: List[str]) -> int:
    """Виконує пакетний запит 'create' до WooCommerce API."""
    
    payload = {"create": batch_data}
    
    try:
        logging.info(f"Надсилаю пакет на створення ({len(batch_data)} товарів)...")
        response = wcapi.post("products/batch", data=payload) 
        
        if response.status_code == 200:
            result = response.json()
            created_count = len(result.get('create', []))
            
            api_errors = result.get('errors', [])
            if api_errors:
                for err in api_errors:
                    err_msg = f"API-Помилка при створенні: SKU '{err.get('data', {}).get('resource_id', 'N/A')}', Code: {err.get('code', 'N/A')}: {err.get('message', 'Невідома помилка')}"
                    errors_list.append(err_msg)
                    logging.error(err_msg)
                
            logging.info(f"✅ Пакет створено. Успішно оброблено API: {created_count} товарів. Помилок у пакеті: {len(api_errors)}")
            return created_count
        else:
            err_msg = f"❌ Критична помилка API ({response.status_code}) при пакетному створенні. Помилка: {response.text[:200]}..."
            errors_list.append(err_msg)
            logging.critical(err_msg)
            return 0
            
    except Exception as e:
        err_msg = f"❌ Непередбачена помилка під час відправки пакету: {e}"
        errors_list.append(err_msg)
        logging.critical(err_msg, exc_info=True)
        return 0
    

def _clean_text(value: str) -> str:
    """Очищує HTML-теги, кодування і зайві пробіли."""
    if not value:
        return ""
    value = html.unescape(str(value))  # розкодовує &#8211; → –
    value = re.sub(r"<.*?>", "", value)  # видаляє HTML-теги
    return value.strip()

def export_product_by_id():
    """
    Експортує всі дані товару за введеним ID у /csv/input/ID_tovar.csv.
    Виправлено HTML-кодування, екранування CSV і додано переклади WPML.
    """
    log_message_to_existing_file()
    settings = load_settings()
    if not settings:
        logging.error("❌ Не вдалося завантажити налаштування.")
        return

    wcapi = get_wc_api(settings)
    if not wcapi:
        logging.error("❌ Не вдалося створити об'єкт WooCommerce API.")
        return

    product_id = input("Введіть ID товару для експорту: ").strip()
    if not product_id.isdigit():
        logging.error("❌ Некоректний ID товару.")
        return
    product_id = int(product_id)

    output_dir = "/var/www/scripts/update/csv/input"
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, "ID_tovar.csv")

    start_time = time.time()
    try:
        # === Основні дані товару ===
        response = wcapi.get(f"products/{product_id}", params={"context": "edit"})
        if response.status_code != 200:
            logging.error(f"❌ Помилка {response.status_code}: {response.text}")
            return
        product = response.json()
        if not isinstance(product, dict):
            logging.error(f"❌ Некоректна структура відповіді API для товару ID {product_id}")
            return

        row = {"id": product_id}

        # === Основні поля ===
        for key, value in product.items():
            if isinstance(value, dict):
                for subkey, subval in value.items():
                    row[f"{key}.{subkey}"] = _clean_text(subval)
            elif isinstance(value, list):
                if key == "meta_data":
                    for meta in value:
                        k = meta.get("key")
                        v = meta.get("value")
                        if k:
                            row[f"Мета: {k}"] = _clean_text(v)
                elif key == "categories":
                    row["categories"] = ", ".join([_clean_text(v.get("name", "")) for v in value])
                elif key == "tags":
                    row["tags"] = ", ".join([_clean_text(v.get("name", "")) for v in value])
                elif key == "images":
                    for idx, img in enumerate(value, start=1):
                        row[f"image_{idx}_id"] = img.get("id", "")
                        row[f"image_{idx}_src"] = img.get("src", "")
                        row[f"image_{idx}_name"] = _clean_text(img.get("name", ""))
                        row[f"image_{idx}_alt"] = _clean_text(img.get("alt", ""))
                        row[f"image_{idx}_title"] = _clean_text(img.get("title", ""))
                        row[f"image_{idx}_caption"] = _clean_text(img.get("caption", ""))
                        row[f"image_{idx}_description"] = _clean_text(img.get("description", ""))
                else:
                    row[key] = ", ".join(map(_clean_text, map(str, value)))
            else:
                row[key] = _clean_text(value)

        # === Переклади WPML ===
        try:
            wpml_resp = wcapi.get(f"products/{product_id}/translations")
            if wpml_resp.status_code == 200:
                translations = wpml_resp.json()
                for lang, tr in translations.items():
                    if isinstance(tr, dict):
                        row[f"wpml_{lang}_id"] = tr.get("id", "")
                        row[f"wpml_{lang}_name"] = _clean_text(tr.get("name", ""))
                        row[f"wpml_{lang}_slug"] = tr.get("slug", "")
                        row[f"wpml_{lang}_status"] = tr.get("status", "")
        except Exception as e:
            logging.warning(f"⚠️ Неможливо отримати WPML переклади: {e}")

        # === Запис у CSV ===
        file_exists = os.path.exists(csv_path)
        file_is_empty = not file_exists or os.path.getsize(csv_path) == 0

        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=row.keys(), quoting=csv.QUOTE_ALL)
            if file_is_empty:
                writer.writeheader()
            writer.writerow(row)

        elapsed = int(time.time() - start_time)
        logging.info(f"✅ Експортовано товар ID {product_id} ({len(row)} полів) → {csv_path} за {elapsed} сек.")

    except Exception as e:
        logging.error(f"❌ Помилка під час експорту: {e}", exc_info=True)





def update_image_seo_by_sku():
    """
    Оновлює SEO-атрибути зображень товару за SKU.
    - Оновлює alt/title через WooCommerce (wc/v3 products PUT).
    - Оновлює caption/description через WP REST API (wp/v2/media/{id}) з Basic Auth,
      використовуючи credentials з settings.json: 'login' і 'pass'.
    """
    logging.basicConfig(level=logging.INFO)
    print("🖼️ Запускаю оновлення SEO-атрибутів зображень...")

    settings = load_settings()
    if not settings:
        logging.critical("❌ Не вдалося завантажити налаштування.")
        return

    # --- Параметри для WooCommerce API (wc/v3) ---
    try:
        wcapi = get_wc_api(settings)
    except Exception as e:
        logging.critical(f"❌ Не вдалося створити об'єкт WooCommerce API: {e}")
        return

    base_url = settings.get("url", "").rstrip("/")
    api_key = settings.get("consumer_key")
    api_secret = settings.get("consumer_secret")

    wp_login = settings.get("login")   # username або логін
    wp_pass = settings.get("pass")     # application password або пароль

    if not base_url or not api_key or not api_secret:
        logging.critical("❌ Неповні налаштування API (url, consumer_key або consumer_secret).")
        return

    # 1) Введення SKU
    sku = input("🔍 Введіть SKU товару: ").strip()
    if not sku:
        print("❌ SKU не введено.")
        return

    # 2) Отримуємо товар по SKU
    try:
        resp = wcapi.get("products", params={"sku": sku})
        if resp.status_code != 200:
            logging.error(f"❌ WooCommerce products GET returned {resp.status_code}: {resp.text[:200]}")
            print(f"❌ Помилка при пошуку товару (статус {resp.status_code}). Перевір логи.")
            return
        products = resp.json()
        if not products:
            print(f"❌ Товар зі SKU {sku} не знайдено.")
            return
        product = products[0]
    except Exception as e:
        logging.error(f"Помилка при запиті до WooCommerce: {e}", exc_info=True)
        print("❌ Помилка при з'єднанні з WooCommerce.")
        return

    product_name = product.get("name", "").strip()
    product_id = product.get("id")
    image_list: List[Dict[str, Any]] = product.get("images", [])  # список dict з keys: id, src, name, alt

    if not product_name:
        print("❌ Не знайдено назви товару.")
        return

    if not image_list:
        print(f"❌ Товар {product_name} не має прив'язаних зображень у відповіді WC API.")
        return

    print(f"✅ Знайдено товар: {product_name}")
    print(f"🖼️ Знайдено {len(image_list)} прив'язаних зображень. Починаю оновлення...")

    seo_data = {
        "title": product_name,
        "alt": f"Купити товар {product_name} в секс-шопі Eros.in.ua",
        "caption": f"{product_name} – інноваційна секс-іграшка для вашого задоволення",
        "description": f"{product_name} купити в інтернет-магазині Eros.in.ua. Великий вибір секс-іграшок, низька ціна, швидка безкоштовна доставка."
    }

    # --- Допоміжна функція: знайти media id по filename через WP REST API (search) ---
    def find_media_id_by_filename(filename: str) -> int:
        """
        Повертає media id або None. Працює через /wp-json/wp/v2/media?search=<filename>
        Потрібна аутентифікація, якщо WP закритий. Ми спробуємо без auth першим, потім з auth.
        """
        search_url = f"{base_url}/wp-json/wp/v2/media"
        params = {"search": filename, "per_page": 10}
        headers = {"Accept": "application/json"}

        # Спроба без auth
        try:
            r = requests.get(search_url, params=params, headers=headers, timeout=15, verify=True)
            if r.status_code == 200:
                items = r.json()
                for it in items:
                    src = it.get("source_url", "") or it.get("guid", {}).get("rendered", "")
                    if filename.lower() in (os.path.basename(src).lower()):
                        return it.get("id")
            # якщо не вдалось або порожньо — спробуємо з auth якщо є
        except Exception as e:
            logging.debug(f"find_media_id_by_filename (no auth) error: {e}")

        if wp_login and wp_pass:
            try:
                r = requests.get(search_url, params=params, headers=headers, auth=(wp_login, wp_pass), timeout=15, verify=True)
                if r.status_code == 200:
                    items = r.json()
                    for it in items:
                        src = it.get("source_url", "") or it.get("guid", {}).get("rendered", "")
                        if filename.lower() in (os.path.basename(src).lower()):
                            return it.get("id")
                else:
                    logging.debug(f"find_media_id_by_filename (auth) status {r.status_code}: {r.text[:200]}")
            except Exception as e:
                logging.debug(f"find_media_id_by_filename (auth) exception: {e}")

        return None

    # --- Основний цикл оновлення ---
    updated = 0
    failed = 0

    # We'll batch update product images alt/title via product PUT if possible
    # Prepare a copy of current images with alt changes to minimize number of product PUTs.
    wc_images_update = []
    for img in image_list:
        media_id = img.get("id")
        src = img.get("src") or ""
        filename = os.path.basename(src) if src else None

        # Prefer media_id; if missing, try to find by filename
        if not media_id and filename:
            found_id = find_media_id_by_filename(filename)
            if found_id:
                media_id = found_id
                logging.info(f"Знайдено media_id {media_id} по файлу {filename}")
            else:
                logging.warning(f"Не знайдено media record для {filename}. Пропускаю.")
                continue

        if media_id:
            # For WooCommerce product update we will set alt and name (title)
            wc_images_update.append({"id": media_id, "alt": seo_data["alt"], "name": seo_data["title"]})

    # If we have any image updates for WooCommerce — send one PUT to products/{id}
    if wc_images_update and product_id:
        try:
            resp_put = wcapi.put(f"products/{product_id}", {"images": wc_images_update})
            if resp_put.status_code == 200:
                logging.info("✅ WooCommerce: alt/title оновлено через products PUT")
                updated += len(wc_images_update)
            else:
                logging.error(f"❌ WooCommerce products PUT returned {resp_put.status_code}: {resp_put.text[:300]}")
                # don't return — try per-media WP updates below
        except Exception as e:
            logging.error(f"Помилка при WooCommerce products PUT: {e}")

    # Now, attempt to update caption/description via wp/v2/media for each image
    for img in image_list:
        media_id = img.get("id")
        src = img.get("src") or ""
        filename = os.path.basename(src) if src else None

        if not media_id:
            if filename:
                media_id = find_media_id_by_filename(filename)
                if media_id:
                    logging.info(f"Знайдено media_id {media_id} для {filename} через пошук.")
                else:
                    logging.warning(f"Не вдалося знайти media_id для {filename}. Пропускаю WP media update.")
                    failed += 1
                    continue
            else:
                logging.warning("Зображення не має src та id — пропускаю.")
                failed += 1
                continue

        media_endpoint = f"{base_url}/wp-json/wp/v2/media/{media_id}"
        update_data = {
            "title": seo_data["title"],
            "alt_text": seo_data["alt"],
            "caption": seo_data["caption"],
            "description": seo_data["description"]
        }

        # Треба авторизація для wp/v2/media (Application Password або user/pass)
        if not wp_login or not wp_pass:
            logging.warning("⚠️ В settings.json не знайдені 'login' та 'pass' — пропускаю оновлення caption/description через wp/v2/media.")
            failed += 1
            continue

        try:
            r = requests.put(media_endpoint, auth=(wp_login, wp_pass), json=update_data, timeout=20, verify=True)
            if r.status_code == 200:
                print(f"✅ Оновлено медіа ID {media_id} ({filename if filename else ''})")
                updated += 1
            else:
                logging.error(f"❌ Помилка оновлення ID {media_id}. Статус: {r.status_code}. Помилка: {r.text[:300]}")
                failed += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"Критична помилка запиту для {media_id}: {e}")
            failed += 1

    print(f"🎯 Завершено. Успішно оновлено: {updated}, не вдалося: {failed}.")




def translate_texts_deepl_batch(texts, target_lang="RU", api_key=None, api_url=None):
    """
    Перекладає список текстів через DeepL API одним запитом.
    Повертає список перекладених текстів.
    """
    if not api_key:
        logging.error("API ключ DeepL не вказано!")
        return texts

    if not api_url:
        api_url = "https://api-free.deepl.com/v2/translate"

    try:
        response = requests.post(
            api_url,
            data={
                "auth_key": api_key,
                "text": texts,
                "target_lang": target_lang
            },
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        return [t["text"] for t in data["translations"]]
    except Exception as e:
        logging.error(f"Помилка пакетного перекладу через DeepL: {e}")
        return texts

def translate_csv_to_ru(batch_size=20):
    """
    Перекладає ключові колонки CSV з української на російську через DeepL.
    Використовує шляхи та ключі з settings.json.
    Працює пакетами, щоб уникнути блокування API.
    """
    log_message_to_existing_file()
    logging.info("🚀 Початок перекладу CSV на російську...")

    settings = load_settings()
    if not settings:
        logging.error("❌ Неможливо завантажити settings.json")
        return

    input_path = settings["paths"].get("csv_path_sl_new_prod")
    output_path = settings["paths"].get("csv_path_sl_new_prod_ru")
    api_key = settings.get("deepl_api_key")
    api_url = settings.get("DEEPL_API_URL", "https://api-free.deepl.com/v2/translate")

    if not all([input_path, output_path, api_key]):
        logging.error("❌ В settings.json не вказані всі необхідні параметри (шляхи або deepl_api_key)")
        return

    # Твої реальні колонки для перекладу
    columns_to_translate = ["name", "content", "excerpt", "rank_math_focus_keyword"]

    try:
        with open(input_path, mode='r', encoding='utf-8') as f_in, \
             open(output_path, mode='w', encoding='utf-8', newline='') as f_out:

            reader = csv.DictReader(f_in)
            headers = reader.fieldnames
            if not headers:
                logging.error("❌ Файл порожній або немає заголовків.")
                return

            writer = csv.DictWriter(f_out, fieldnames=headers)
            writer.writeheader()

            batch_rows = []
            batch_texts = []

            for idx, row in enumerate(reader, start=2):
                texts_to_translate = []
                for col in columns_to_translate:
                    texts_to_translate.append(row.get(col, ""))

                batch_rows.append(row)
                batch_texts.append(texts_to_translate)

                # Переклад пакетом
                if len(batch_rows) >= batch_size:
                    # Транспонуємо список текстів для DeepL (потрібно надсилати рядки окремо)
                    flattened_texts = [t for sublist in batch_texts for t in sublist]
                    translated_flat = translate_texts_deepl_batch(flattened_texts, api_key=api_key, api_url=api_url)

                    # Розпаковуємо назад у рядки
                    for i, r in enumerate(batch_rows):
                        for j, col in enumerate(columns_to_translate):
                            r[col] = translated_flat[i * len(columns_to_translate) + j]
                            logging.info(f"Рядок {idx - len(batch_rows) + i + 2}: колонка '{col}' перекладена.")
                        writer.writerow(r)

                    batch_rows = []
                    batch_texts = []
                    time.sleep(1)  # пауза між пакетами

            # Переклад залишку рядків
            if batch_rows:
                flattened_texts = [t for sublist in batch_texts for t in sublist]
                translated_flat = translate_texts_deepl_batch(flattened_texts, api_key=api_key, api_url=api_url)
                for i, r in enumerate(batch_rows):
                    for j, col in enumerate(columns_to_translate):
                        r[col] = translated_flat[i * len(columns_to_translate) + j]
                        logging.info(f"Рядок {idx - len(batch_rows) + i + 2}: колонка '{col}' перекладена.")
                    writer.writerow(r)

        logging.info(f"✅ Переклад завершено. Файл збережено: {output_path}")

    except FileNotFoundError:
        logging.error(f"❌ Вхідний файл не знайдено: {input_path}")
    except Exception as e:
        logging.error(f"❌ Непередбачена помилка при перекладі CSV: {e}")