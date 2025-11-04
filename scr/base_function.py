from woocommerce import API
import json
import os, csv, shutil, logging, requests, mimetypes, glob
import logging
import html
import re
import mysql.connector
from datetime import datetime
from typing import Dict, Tuple, List, Optional, Any
from PIL import Image
from bs4 import BeautifulSoup
import time
import pymysql


# --- ЗАГАЛЬНІ ФУНКЦІЇ ---
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

# --- ПІДКЛЮЧЕННЯ ДО WOOCOMMERCE API ---
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
        query_string_auth=False  # Змінив з True 👈 використовує Basic Auth (рекомендовано) 
    )
    return wcapi

# --- ПЕРЕВІРКА ВЕРСІЇ WOOCOMMERCE ---
def check_version():
    wcapi = get_wc_api()
    response = wcapi.get("system_status")
    if response.status_code == 200:
        data = response.json()
        print("WooCommerce version:", data.get("environment", {}).get("version"))
    else:
        print("Error:", response.status_code, response.text)

# --- ЛОГУВАННЯ (Створення нового файлу)---
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

# --- ЛОГУВАННЯ (Запис в існуючий файл)---
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
            #level=logging.DEBUG,
            format='%(asctime)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            filemode='a'
        )
    logging.info("--- Повідомлення додано до існуючого логу ---")

# --- ПЕРЕВІРКА CSV ---
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

# --- РОБОТА З ФАЙЛАМИ КОНФІГУРАЦІЇ (attribute.csv, category.csv, poznachky.csv) ---
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

# --- ОБРОБКА ЗОБРАЖЕНЬ ---   
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

# --- ЗАВАНТАЖЕННЯ ЗОБРАЖЕНЬ ТОВАРУ ---
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

# --- СИНХРОНІЗАЦІЯ КОЛОНКИ WEBP У CSV ---
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

# --- КОПІЮВАННЯ ФАЙЛІВ У ФІНАЛЬНУ ДИРЕКТОРІЮ ---
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

# --- ДОПОМІЖНА ФУНКЦІЯ ДЛЯ ПАКЕТНОГО ЗАПИСУ (Оновлення) ---
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

# --- Глобальна HTTPS-сесія та кеш ---
_session = requests.Session()
_media_cache: Dict[str, int] = {}  # slug -> id

def _get_media_id_by_filename_sql(db_conf, filename):
    """
    Миттєвий пошук ID медіафайлу напряму у базі WordPress.
    Працює у 50-100 разів швидше за REST.
    """
    try:
        clean_name = re.sub(r'-\d+x\d+(?=\.)', '', filename)
        file_slug = os.path.splitext(clean_name)[0]

        conn = pymysql.connect(
            host=db_conf["host"],
            user=db_conf["user"],
            password=db_conf["password"],
            database=db_conf["database"],
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            sql = """
            SELECT ID 
            FROM wp_posts 
            WHERE post_name=%s AND post_type='attachment' 
            LIMIT 1;
            """
            cursor.execute(sql, (file_slug,))
            result = cursor.fetchone()
            conn.close()

            if result:
                media_id = result["ID"]
                logging.debug(f"✅ SQL-знайдено медіа '{file_slug}' → ID: {media_id}")
                return media_id
            else:
                logging.warning(f"⚠️ SQL: медіа '{file_slug}' не знайдено.")
                return None
    except Exception as e:
        logging.error(f"❌ SQL-помилка при пошуку медіа '{filename}': {e}", exc_info=True)
        return None

# --- ДОПОМІЖНА ФУНКЦІЯ ДЛЯ Пошуку Media IDs ---
def find_media_ids_for_sku(wcapi, sku: str, uploads_path: str) -> List[Dict[str, Any]]:
    """
    Знаходить усі зображення для SKU у uploads_path та повертає список ID для WooCommerce.
    - Використовує persistent HTTPS-сесію (_session)
    - Має кеш для вже знайдених slug
    - Ігнорує технічні копії (-150x150, -300x300, ...)
    - GIF ставить останнім
    """
    # --- Завантажуємо конфіг ---
    settings = load_settings()
    if not settings:
        logging.error("❌ Не вдалося завантажити settings.json — пошук зображень пропущено.")
        return []

    db_conf = settings.get("db", {})
    if not db_conf:
        logging.error("❌ У settings.json відсутній блок 'db'.")
        return []


    def _get_media_id_by_filename(wcapi, filename: str) -> int | None:
        """
        Пошук ID медіафайлу у WordPress через REST API /wp/v2/media.
        Використовує публічний запит, при 401/403 — повтор з ключами WooCommerce.
        Має кешування результатів і rate-limit 0.1 с.
        """
        try:
            # 1️⃣ Нормалізуємо ім’я
            clean_name = re.sub(r'-\d+x\d+(?=\.)', '', filename)
            file_slug = os.path.splitext(clean_name)[0]

            # 2️⃣ Якщо вже в кеші — не запитуємо
            if file_slug in _media_cache:
                logging.debug(f"♻️ Кешовано медіа '{file_slug}' → ID: {_media_cache[file_slug]}")
                return _media_cache[file_slug]

            wp_media_url = f"{wcapi.url.rstrip('/')}/wp-json/wp/v2/media"
            params = {"search": file_slug, "per_page": 5}

            # 3️⃣ Пробуємо без авторизації
            response = _session.get(wp_media_url, params=params, timeout=15)

            # 4️⃣ Якщо закрито — повтор з WooCommerce ключами
            if response.status_code in (401, 403):
                logging.debug(f"🔒 REST API потребує auth для '{file_slug}', пробую з ключами WooCommerce...")
                response = _session.get(
                    wp_media_url,
                    params=params,
                    auth=(wcapi.consumer_key, wcapi.consumer_secret),
                    timeout=15
                )

            # 5️⃣ Обробляємо відповідь
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and data:
                    # Пошук точного збігу slug
                    exact_match = next((item for item in data if item.get("slug", "").lower() == file_slug.lower()), None)
                    item = exact_match or data[0]

                    media_id = item.get("id")
                    media_slug = item.get("slug", "")
                    media_url = item.get("source_url", "")
                    media_title = item.get("title", {}).get("rendered", "")

                    if media_slug.lower() != file_slug.lower():
                        logging.warning(f"⚠️ '{file_slug}' знайдено неточно: повернено '{media_slug}' (ID {media_id})")
                    else:
                        logging.debug(f"✅ Точний збіг '{file_slug}' → ID: {media_id} | Назва: {media_title} | URL: {media_url}")

                    _media_cache[file_slug] = media_id
                    time.sleep(0.1)  # невелика пауза, щоб не перевантажувати WP
                    return media_id
                else:
                    logging.warning(f"⚠️ Медіа '{file_slug}' не знайдено у WP медіатеці.")
            else:
                logging.error(f"❌ Помилка {response.status_code} при пошуку медіа '{file_slug}'.")
        except Exception as e:
            logging.error(f"❌ Виняток при пошуку медіа '{filename}': {e}", exc_info=True)
        return None

    # --- Основна логіка ---
    pattern = os.path.join(uploads_path, '**', f'{sku}*.*')
    files = glob.glob(pattern, recursive=True)

    # Унікальні base-імена без суфіксів
    unique_files = {re.sub(r'-\d+x\d+(?=\.)', '', os.path.basename(p)) for p in files}

    # GIF — останнім
    sorted_files = sorted(unique_files, key=lambda f: (f.lower().endswith('.gif'), f))

    media_ids = []
    for filename in sorted_files:
        media_id = _get_media_id_by_filename_sql(settings["db"], filename)
        if media_id:
            media_ids.append({"id": media_id})

    if not media_ids:
        logging.warning(f"⚠️ SKU {sku}: Не знайдено зображень у '{uploads_path}'")
    else:
        logging.info(f"🖼️ SKU {sku}: Додано {len(media_ids)} унікальних зображень.")

    return media_ids

# --- ДОПОМІЖНА ФУНКЦІЯ ДЛЯ ПАКЕТНОГО ЗАПИСУ (Створення) ---
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

# --- ЕКСПОРТ ТОВАРУ ЗА ID ---
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

# --- ОНОВЛЕННЯ SEO-АТРИБУТІВ ЗОБРАЖЕНЬ ---
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

# --- ЗАПОВНЕННЯ КОЛОНКИ _wpml_import_translation_group ---
def fill_wpml_translation_group():
    """
    Шукає trid (_wpml_import_translation_group) у базі WordPress
    за SKU і оновлює цей самий CSV-файл (без створення копії).
    """
    log_message_to_existing_file()
    logging.info("🚀 Початок оновлення колонки _wpml_import_translation_group")

    settings = load_settings()
    csv_path = settings["paths"].get("csv_path_sl_new_prod_ru")
    db_conf = settings.get("db")

    # 🔹 Перевірка конфігурації
    if not db_conf:
        logging.error("❌ У settings.json відсутній розділ 'db' з параметрами бази даних")
        return

    # 🔹 Підключення до MySQL
    conn = mysql.connector.connect(
        host=db_conf["host"],
        user=db_conf["user"],
        password=db_conf["password"],
        database=db_conf["database"],
        charset="utf8mb4"
    )
    cursor = conn.cursor(dictionary=True)

    # 🔹 Зчитуємо усі рядки з файлу
    with open(csv_path, "r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)
        fieldnames = reader.fieldnames
        if "_wpml_import_translation_group" not in fieldnames:
            fieldnames.append("_wpml_import_translation_group")

    logging.info("🚀 Початок пошуку trid для кожного SKU...")

    # 🔹 Обробка кожного SKU
    for idx, row in enumerate(rows, start=2):
        sku = row.get("Sku")
        if not sku:
            logging.warning(f"Рядок {idx}: пропущено через відсутній SKU")
            continue

        # Знайти product_id за SKU
        cursor.execute("""
            SELECT pm.post_id
            FROM wp_postmeta pm
            JOIN wp_posts p ON p.ID = pm.post_id
            WHERE pm.meta_key = '_sku' AND pm.meta_value = %s AND p.post_type = 'product'
            LIMIT 1;
        """, (sku,))
        res = cursor.fetchone()

        if not res:
            logging.warning(f"⚠️ SKU {sku}: товар не знайдено у базі")
            continue

        product_id = res["post_id"]

        # Знайти trid
        cursor.execute("""
            SELECT trid
            FROM wp_icl_translations
            WHERE element_type = 'post_product' AND element_id = %s
            LIMIT 1;
        """, (product_id,))
        trid_res = cursor.fetchone()

        if trid_res:
            trid = trid_res["trid"]
            row["_wpml_import_translation_group"] = trid
            logging.info(f"✅ SKU {sku}: знайдено trid = {trid}")
        else:
            logging.warning(f"⚠️ SKU {sku}: не знайдено trid у wp_icl_translations")

    # 🔹 Записуємо назад у той самий файл
    with open(csv_path, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    cursor.close()
    conn.close()

    logging.info(f"🏁 Оновлення завершено. Дані збережено у {csv_path}")

# --- ПЕРЕКЛАД CSV УКР → РУС ---
def clean_text(text):
    """
    Видаляє HTML теги та зайві пробіли.
    """
    if not text:
        return ""
    # text = re.sub(r'<[^>]+>', '', text)  # видалити HTML теги
    text = re.sub(r'\s+', ' ', text).strip()  # зайві пробіли та переводи рядків
    return text

def get_deepl_usage(api_key, api_url="https://api-free.deepl.com/v2/usage"):
    """
    Перевіряє використання символів DeepL API (Free або Pro).
    Повертає словник з used_characters, limit, remaining.
    """
    try:
        response = requests.get(api_url, headers={"Authorization": f"DeepL-Auth-Key {api_key}"}, timeout=15)
        response.raise_for_status()
        data = response.json()
        used = data.get("character_count", 0)
        limit = data.get("character_limit", 0)
        remaining = limit - used if limit else None
        logging.info(f"🔹 Використано {used:,} із {limit:,} символів DeepL (залишилось {remaining:,})")
        return {"used": used, "limit": limit, "remaining": remaining}
    except Exception as e:
        logging.warning(f"⚠️ Не вдалося отримати інформацію про ліміт DeepL: {e}")
        return None

def translate_text_deepl(text, target_lang="RU", api_key=None, api_url=None):
    """
    Переклад через DeepL із БЕЗПЕЧНИМ збереженням HTML:
    - HTML-теги (<strong>, <em>, <p> тощо) НЕ відправляються в DeepL і повертаються як є.
    - Перекладаються лише текстові сегменти між тегами.
    - Сегменти без кирилиці (англ/цифри) не перекладаються взагалі.
    - Довгі сегменти ріжуться на частини ≤ 500 символів.
    """
    if not text or not text.strip():
        return text
    if not api_key:
        logging.error("API ключ DeepL не вказано!")
        return text
    if not api_url:
        api_url = "https://api-free.deepl.com/v2/translate"

    # 1) Розділити рядок на HTML-теги і прості текстові сегменти
    #    Приклад: ["<p>", "Текст ", "<strong>", "жирний", "</strong>", "</p>"]
    tokens = re.split(r'(<[^>]+>)', text)
    out = []

    # регексп для виявлення кирилиці (укр/ru)
    has_cyrillic = re.compile(r'[А-Яа-яЁёЇїІіЄєҐґ]')

    def translate_chunk(chunk: str) -> str:
        """Перекласти один короткий текстовий шматок (≤500 символів)."""
        # якщо немає кирилиці — повертаємо як є (англ/цифри не чіпаємо)
        if not has_cyrillic.search(chunk):
            return chunk
        # жодних службових <i>-тегів усередину — працюємо з чистим текстом
        try:
            resp = requests.post(
                api_url,
                data={
                    "auth_key": api_key,
                    "text": chunk,
                    "target_lang": target_lang,
                },
                timeout=30
            )
            resp.raise_for_status()
            translated = resp.json()["translations"][0]["text"]
            # Якщо DeepL раптом повернув &lt;strong&gt; у тексті, розкодуємо сутності тільки в тексті
            return html.unescape(translated)
        except Exception as e:
            logging.error(f"Помилка перекладу: {e}")
            return chunk

    # 2) Обробити кожен токен
    for tok in tokens:
        if not tok:
            continue
        if tok.startswith("<") and tok.endswith(">"):
            # Це HTML-тег — повертаємо як є
            out.append(tok)
            continue

        # Це простий текст — ріжемо на частини ≤ 500 символів по границях речень/рядків
        text_part = tok
        if not text_part.strip():
            out.append(text_part)
            continue

        # м’яке речення/параграфне ділення
        pieces = []
        current = ""
        # розбиваємо за кінцем речення або переносом, зберігаючи роздільники
        for seg in re.split(r'(\. |\n)', text_part):
            if len(current) + len(seg) <= 500:
                current += seg
            else:
                if current:
                    pieces.append(current)
                current = seg
        if current:
            pieces.append(current)

        # переклад кожного шматка
        translated_pieces = []
        for p in pieces:
            translated_pieces.append(translate_chunk(p))
            time.sleep(0.4)  # легкий тротлінг

        out.append("".join(translated_pieces))

    return "".join(out)

def translate_csv_to_ru():
    """
    Перекладає content та excerpt з української на російську
    та зберігає у SL_new_prod_ru.csv
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
        logging.error("❌ Не вказані всі параметри у settings.json")
        return

    try:
        with open(input_path, 'r', encoding='utf-8') as f_in, \
             open(output_path, 'w', encoding='utf-8', newline='') as f_out:

            reader = csv.DictReader(f_in)
            output_headers = ["sku", "name", "content", "short_description", "rank_math_focus_keyword", "lang", "translation_of"]
            writer = csv.DictWriter(f_out, fieldnames=output_headers)
            writer.writeheader()

            for idx, row in enumerate(reader, start=2):
                new_row = {}

                # 1. SKU
                new_row["sku"] = row.get("sku", "")

                # 2. Name без перекладу
                new_row["name"] = row.get("name", "")

                # 3. Переклад content
                content_text = clean_text(row.get("content", ""))
                new_row["content"] = translate_text_deepl(content_text, target_lang="RU", api_key=api_key, api_url=api_url)

                # 4. Переклад excerpt → short_description
                excerpt_text = clean_text(row.get("excerpt", ""))
                new_row["short_description"] = translate_text_deepl(excerpt_text, target_lang="RU", api_key=api_key, api_url=api_url)

                # 5. Rank Math
                new_row["rank_math_focus_keyword"] = row.get("rank_math_focus_keyword", "")

                # 6. WPML
                new_row["lang"] = "ru"
                new_row["translation_of"] = ""  # можна підставити ID оригіналу

                writer.writerow(new_row)
                logging.info(f"Рядок {idx}: переклад content та short_description завершено")

        logging.info(f"✅ Переклад завершено. Файл збережено: {output_path}")

    except FileNotFoundError:
        logging.error(f"❌ Вхідний файл не знайдено: {input_path}")
    except Exception as e:
        logging.error(f"❌ Помилка при перекладі CSV: {e}")

# --- ЛОГУВАННЯ ГЛОБАЛЬНИХ АТРИБУТІВ WOO ---
def log_global_attributes():
    """
    Отримує список глобальних атрибутів WooCommerce (pa_*)
    і виводить їх у лог із ID, slug та назвою.
    """
    log_message_to_existing_file()
    logging.info("🔍 Починаю отримання списку глобальних атрибутів WooCommerce...")

    settings = load_settings()
    if not settings:
        logging.error("❌ Не вдалося завантажити settings.json")
        return

    wcapi = get_wc_api(settings)
    if not wcapi:
        logging.error("❌ Не вдалося створити об'єкт WooCommerce API.")
        return

    try:
        page = 1
        all_attributes = []
        MAX_PAGES = 5  # 🔒 безпечна межа, бо глобальних атрибутів максимум кілька десятків

        while page <= MAX_PAGES:
            response = wcapi.get("products/attributes", params={"per_page": 100, "page": page})
            logging.info(f"➡️ Отримано сторінку {page} (статус {response.status_code})")

            if response.status_code != 200:
                logging.error(f"❌ Помилка при запиті до WooCommerce API: {response.status_code} - {response.text}")
                break

            data = response.json()
            if not data:
                logging.info("📭 Більше сторінок немає — завершую запит.")
                break

            all_attributes.extend(data)
            if len(data) < 100:
                break  # менше 100 — значить, остання сторінка
            page += 1

        if not all_attributes:
            logging.warning("⚠️ Глобальні атрибути не знайдено.")
            return

        logging.info("🧩 --- Глобальні атрибути WooCommerce ---")
        for attr in all_attributes:
            attr_id = attr.get("id")
            name = attr.get("name")
            slug = attr.get("slug")
            type_ = attr.get("type")
            orderby = attr.get("order_by")
            logging.info(f"ID={attr_id:>3} | slug={slug:<20} | name={name:<25} | type={type_} | orderby={orderby}")

        logging.info(f"✅ Всього знайдено {len(all_attributes)} глобальних атрибутів.")

    except Exception as e:
        logging.error(f"❌ Помилка при отриманні списку атрибутів: {e}", exc_info=True)

# --- КОНВЕРТАЦІЯ ЛОКАЛЬНИХ АТРИБУТІВ У ГЛОБАЛЬНІ ---
def convert_local_attributes_to_global():
    """
    Пакетна конвертація локальних атрибутів у глобальні
    для товарів, створених після 1 вересня 2025 року.
    """
    from datetime import datetime
    import re

    log_message_to_existing_file()
    logging.info("🚀 Початок пакетної конвертації локальних атрибутів у глобальні для останніх товарів...")

    settings = load_settings()
    if not settings:
        logging.critical("❌ Не вдалося завантажити налаштування.")
        return

    wcapi = get_wc_api(settings)
    if not wcapi:
        logging.critical("❌ Не вдалося підключитися до WooCommerce API.")
        return

    global_attr_map = settings.get("global_attr_map", {})
    numeric_attrs = ["pa_diameter", "pa_length", "pa_height"]
    cutoff_date = datetime(2025, 9, 1)

    def _smart_split_attr(attr_name, val):
        if not val:
            return []
        if attr_name in numeric_attrs:
            parts = [p.strip() for p in re.split(r'[|;,]', val) if p.strip()]
            return [",".join(parts)]
        else:
            parts = [p.strip() for p in re.split(r'[|;,]', val) if p.strip()]
            return parts

    try:
        # Отримуємо всі товари після cutoff_date (постаємо у сторінках по 100)
        page = 1
        per_page = 10
        total_checked = 0
        total_updated = 0

        while True:
            response = wcapi.get("products", params={
                "per_page": per_page,
                "page": page,
                "after": cutoff_date.isoformat(),
                "orderby": "date",
                "order": "asc"
            })
            if response.status_code != 200:
                logging.error(f"❌ Не вдалося отримати товари: {response.text}")
                break

            products = response.json()
            if not products:
                break  # кінець списку

            for product in products:
                product_id = product["id"]
                product_name = product.get("name", "")
                attributes = product.get("attributes", [])
                local_attrs = []
                global_attrs = []

                for attr in attributes:
                    attr_name = attr.get("name")
                    attr_id = attr.get("id")
                    if attr_id and attr_id in global_attr_map.values():
                        global_attrs.append(attr)
                    else:
                        local_attrs.append(attr)

                logging.info(f"Товар ID={product_id}, Name='{product_name}': {len(global_attrs)} глобальних, {len(local_attrs)} локальних атрибутів")

                if not local_attrs:
                    total_checked += 1
                    continue

                # Конвертація локальних у глобальні
                changes = 0
                for attr in local_attrs:
                    name = attr.get("name")
                    value = "|".join(attr.get("options", []))
                    attr["options"] = _smart_split_attr(name, value)
                    if name in global_attr_map:
                        attr["id"] = global_attr_map[name]
                        changes += 1

                if changes:
                    product_data = {"id": product_id, "attributes": attributes}
                    resp_update = wcapi.put(f"products/{product_id}", product_data)
                    if resp_update.status_code == 200:
                        logging.info(f"✅ Оновлено {changes} атрибутів для SKU={product.get('sku','')} / ID={product_id}")
                        total_updated += changes
                    else:
                        logging.error(f"❌ Не вдалося оновити товар ID={product_id}: {resp_update.text}")

                total_checked += 1

            page += 1

        logging.info(f"--- 🏁 Підсумок ---")
        logging.info(f"Всього перевірено товарів: {total_checked}")
        logging.info(f"Всього оновлено атрибутів: {total_updated}")

    except Exception as e:
        logging.error(f"❌ Критична помилка: {e}", exc_info=True)

# --- ПЕРЕВІРКА ДОСТУПУ ДО GOOGLE SEARCH CONSOLE ---
def test_search_console_access():
    """
    Перевіряє доступ до Google Search Console API через Service Account.
    Виводить у лог і консоль список сайтів, до яких є доступ.
    """
    # --- 1. Ініціалізація логування ---
    log_message_to_existing_file()
    logging.info("🚀 Початок перевірки доступу до Google Search Console...")

    # --- 2. Завантаження налаштувань ---
    settings = load_settings()
    if not settings:
        logging.critical("❌ Не вдалося завантажити settings.json.")
        return

    json_path = settings["paths"].get("google_json")
    if not json_path:
        logging.critical("❌ Не вказано шлях до Google JSON ключа у settings.json (paths.google_json).")
        print("❌ Не вказано шлях до Google JSON у settings.json (paths.google_json).")
        return

    # Якщо шлях відносний — перетворюємо на абсолютний
    if not os.path.isabs(json_path):
        base_dir = os.path.join(os.path.dirname(__file__), "..")
        json_path = os.path.normpath(os.path.join(base_dir, json_path))

    if not os.path.exists(json_path):
        logging.critical(f"❌ Файл ключа Google JSON не знайдено: {json_path}")
        print(f"❌ Не знайдено файл ключа Google JSON:\n{json_path}")
        return

    # --- 3. Імпорт бібліотек Google API ---
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        msg = "❌ Не встановлені бібліотеки google-auth та google-api-python-client."
        logging.critical(msg)
        print(f"{msg}\nВстанови їх командою:\n  pip install google-auth google-auth-oauthlib google-api-python-client")
        return

    # --- 4. Створення підключення до Search Console API ---
    SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
    try:
        credentials = service_account.Credentials.from_service_account_file(json_path, scopes=SCOPES)
        service = build("searchconsole", "v1", credentials=credentials)
        response = service.sites().list().execute()
    except Exception as e:
        logging.critical(f"❌ Помилка при створенні з'єднання або запиті до Search Console: {e}", exc_info=True)
        print(f"❌ Не вдалося підключитися до Search Console API.\nПомилка: {e}")
        return

    # --- 5. Обробка результатів ---
    site_list = response.get("siteEntry", [])
    if not site_list:
        logging.warning("⚠️ Сервісний акаунт не має доступу до жодного сайту у Search Console.")
        print("⚠️ Акаунт не має доступу до сайтів у Search Console.\nПеревір, чи додано цей email у Search Console з правами Full.")
        return

    print("✅ Сайти, доступні цьому акаунту:")
    logging.info("✅ Отримано список сайтів у Search Console:")
    for site in site_list:
        url = site.get("siteUrl", "")
        level = site.get("permissionLevel", "")
        print(f" - {url} ({level})")
        logging.info(f" - {url} ({level})")

    logging.info("🎯 Перевірка Search Console завершена успішно.")

# --- ПЕРЕВІРКА ТА ІНДЕКСАЦІЯ ОДНІЄЇ СТОРІНКИ В GOOGLE ---
def check_and_index_url_in_google():
    """
    Запитує URL сторінки, перевіряє індексацію в Search Console API.
    Якщо не індексована — надсилає запит на індексацію.
    Логує всю інформацію.
    """
    log_message_to_existing_file()
    logging.info("🚀 Початок перевірки та можливої індексації сторінки Google Search Console...")

    # 1️⃣ Завантаження налаштувань
    settings = load_settings()
    if not settings:
        logging.critical("❌ Не вдалося завантажити settings.json.")
        return

    json_path = settings["paths"].get("google_json")
    if not json_path:
        print("❌ Не вказано шлях до JSON ключа у settings.json")
        logging.critical("❌ Не вказано шлях до Google JSON у settings.json.")
        return

    if not os.path.isabs(json_path):
        base_dir = os.path.join(os.path.dirname(__file__), "..")
        json_path = os.path.normpath(os.path.join(base_dir, json_path))

    if not os.path.exists(json_path):
        print(f"❌ Не знайдено файл ключа Google JSON:\n{json_path}")
        logging.critical(f"❌ Файл ключа Google JSON не знайдено: {json_path}")
        return

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        print("⚠️ Встанови бібліотеки: pip install google-auth google-auth-oauthlib google-api-python-client")
        logging.critical("❌ Відсутні бібліотеки Google API.")
        return

    # 2️⃣ Введення URL користувачем
    url = input("🔗 Введіть URL сторінки для перевірки: ").strip()
    if not url.startswith("http"):
        print("❌ Некоректний URL.")
        return

    site_url = "https://eros.in.ua/"  # можна витягати з settings["site_url"], якщо треба

    try:
        # Авторизація
        credentials = service_account.Credentials.from_service_account_file(
            json_path, scopes=["https://www.googleapis.com/auth/webmasters"]
        )
        service = build("searchconsole", "v1", credentials=credentials)

        # 3️⃣ Перевірка індексації
        logging.info(f"🔍 Перевіряю стан індексації для: {url}")
        inspect_body = {"inspectionUrl": url, "siteUrl": site_url}

        try:
            result = service.urlInspection().index().inspect(body=inspect_body).execute()
            index_result = result.get("inspectionResult", {}).get("indexStatusResult", {})
            verdict = index_result.get("verdict", "UNKNOWN")
            coverage = index_result.get("coverageState", "N/A")
            last_crawl = index_result.get("lastCrawlTime", "N/A")
            page_fetch = index_result.get("pageFetchState", "N/A")

            if verdict == "PASS" or "Indexed" in coverage:
                print(f"✅ Сторінка вже в індексі ({coverage}).")
                logging.info(f"✅ Сторінка {url} вже індексована. Останнє сканування: {last_crawl}, статус: {page_fetch}")
                return
            else:
                print(f"⚠️ Сторінка не індексована ({coverage}).")
                logging.info(f"⚠️ Неіндексована сторінка: {url}, статус: {coverage}")
        except Exception as e:
            print("⚠️ Не вдалося отримати інформацію про індексацію:", e)
            logging.warning(f"⚠️ Помилка при запиті індексації: {e}")

        # 4️⃣ Якщо не індексована — пробуємо надіслати на індексацію
        print("📤 Відправляю сторінку на індексацію...")
        try:
            response = service.urlInspection().index().inspect(
                body={"inspectionUrl": url, "siteUrl": site_url}
            ).execute()
            logging.info(f"📅 {datetime.now().isoformat()} — Відправлено URL на індексацію: {url}")
            print("✅ Запит на індексацію відправлено успішно.")
        except Exception as e:
            logging.error(f"❌ Не вдалося відправити URL на індексацію: {e}", exc_info=True)
            print("❌ Помилка при надсиланні на індексацію:", e)

    except Exception as e:
        logging.critical(f"❌ Критична помилка Google API: {e}", exc_info=True)
        print("❌ Критична помилка:", e)

def process_indexing_for_new_products():
    """
    Оновлена версія:
    1. Зчитує SKU з SL_new_prod.csv.
    2. Знаходить обидві сторінки (UA, RU) через _wpml_import_translation_group.
    3. Перевіряє індексацію у Google Search Console.
    4. Оновлює index_google.csv та none_index.csv (без дублів).
    5. Якщо перевищено квоту API (429 або Quota exceeded) — URL додається в index_none_quota.csv.
    """
    import csv, os, time, logging, mysql.connector
    from datetime import datetime
    from googleapiclient.errors import HttpError

    log_message_to_existing_file()
    logging.info("🚀 Початок перевірки та індексації нових товарів у Google Search Console...")

    # --- Завантаження налаштувань ---
    settings = load_settings()
    if not settings:
        logging.critical("❌ Не вдалося завантажити settings.json.")
        return

    paths = settings.get("paths", {})
    csv_path = paths.get("csv_path_sl_new_prod")
    index_log_path = paths.get("index_google")
    none_index_path = paths.get("none_index")
    index_none_quota_path = paths.get("index_none_quota")
    json_path = paths.get("google_json")

    if not all([csv_path, index_log_path, none_index_path, json_path]):
        logging.critical("❌ Відсутні необхідні шляхи у settings.json.")
        return

    db_conf = settings.get("db")
    if not db_conf:
        logging.critical("❌ Відсутні налаштування бази даних у settings.json.")
        return

    # --- Підключення до MySQL ---
    try:
        conn = mysql.connector.connect(
            host=db_conf["host"],
            user=db_conf["user"],
            password=db_conf["password"],
            database=db_conf["database"],
            charset="utf8mb4"
        )
        cursor = conn.cursor(dictionary=True)
    except Exception as e:
        logging.critical(f"❌ Не вдалося підключитися до бази даних: {e}")
        return

    # --- Підключення до Google Search Console API ---
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        credentials = service_account.Credentials.from_service_account_file(
            json_path, scopes=["https://www.googleapis.com/auth/webmasters"]
        )
        service = build("searchconsole", "v1", credentials=credentials)
    except Exception as e:
        logging.critical(f"❌ Не вдалося створити підключення до Search Console API: {e}")
        return

    # --- Завантаження існуючих URL ---
    existing_urls = set()
    if os.path.exists(index_log_path):
        with open(index_log_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing_urls.add(row["URL"].strip())

    none_index_urls = set()
    if os.path.exists(none_index_path):
        with open(none_index_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                none_index_urls.add(row["URL"].strip())

    # --- Зчитуємо SKU ---
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            skus = [r["sku"].strip() for r in reader if r.get("sku")]
    except Exception as e:
        logging.critical(f"❌ Не вдалося прочитати файл SKU: {e}")
        return

    logging.info(f"📦 Знайдено {len(skus)} SKU у файлі SL_new_prod.csv")

    # --- Підготовка CSV для index_google.csv ---
    fieldnames = [
        "URL", "Тип сторінки", "Стан індексації", "Вердикт (verdict)", "CoverageState",
        "Last Crawl Time", "Page Fetch State", "Indexing Allowed", "Дата запиту",
        "Відправлено на індексацію", "Дата надсилання", "Помилка API", "Опис помилки",
        "HTTP статус сторінки", "Коментар", "ResponseTime", "Tries"
    ]
    index_file_exists = os.path.exists(index_log_path)
    with open(index_log_path, "a", encoding="utf-8", newline="") as index_file:
        writer = csv.DictWriter(index_file, fieldnames=fieldnames)
        if not index_file_exists:
            writer.writeheader()

        # --- Обробка SKU ---
        for sku in skus:
            try:
                # --- 1️⃣ Знайти товар по SKU ---
                cursor.execute("""
                    SELECT p.ID, p.post_name
                    FROM wp_posts p
                    JOIN wp_postmeta m ON p.ID = m.post_id
                    WHERE m.meta_key = '_sku' AND m.meta_value = %s
                    AND p.post_type = 'product' AND p.post_status = 'publish'
                    LIMIT 1;
                """, (sku,))
                product = cursor.fetchone()

                if not product:
                    logging.warning(f"⚠️ SKU {sku}: не знайдено товар у базі.")
                    continue

                product_id = product["ID"]

                # --- 2️⃣ Отримуємо trid і мови ---
                cursor.execute("""
                    SELECT trid, language_code
                    FROM wp_icl_translations
                    WHERE element_type = 'post_product' AND element_id = %s
                    LIMIT 1;
                """, (product_id,))
                tinfo = cursor.fetchone()
                trid = tinfo["trid"] if tinfo else None

                urls_to_check = []
                if trid:
                    cursor.execute("""
                        SELECT element_id, language_code
                        FROM wp_icl_translations
                        WHERE trid = %s AND element_type = 'post_product';
                    """, (trid,))
                    translations = cursor.fetchall()
                    for tr in translations:
                        lang = tr["language_code"]
                        pid = tr["element_id"]
                        cursor.execute("""
                            SELECT post_name FROM wp_posts 
                            WHERE ID = %s AND post_status = 'publish' LIMIT 1;
                        """, (pid,))
                        slug_row = cursor.fetchone()
                        if not slug_row:
                            continue
                        slug = slug_row["post_name"]
                        if lang == "uk":
                            urls_to_check.append(("UA", f"https://eros.in.ua/product/{slug}/"))
                        elif lang == "ru":
                            urls_to_check.append(("RU", f"https://eros.in.ua/ru/product/{slug}/"))

                # --- 3️⃣ Перевірка індексації ---
                for lang, url in urls_to_check:
                    if url in existing_urls:
                        continue

                    result = {
                        "URL": url,
                        "Тип сторінки": "product",
                        "Дата запиту": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "Tries": 1
                    }
                    start_time = time.time()

                    try:
                        inspect_body = {"inspectionUrl": url, "siteUrl": "https://eros.in.ua/"}
                        res = service.urlInspection().index().inspect(body=inspect_body).execute()
                        info = res.get("inspectionResult", {}).get("indexStatusResult", {})

                        result["Вердикт (verdict)"] = info.get("verdict", "")
                        result["CoverageState"] = info.get("coverageState", "")
                        result["Last Crawl Time"] = info.get("lastCrawlTime", "")
                        result["Page Fetch State"] = info.get("pageFetchState", "")
                        result["Indexing Allowed"] = info.get("indexingState", "")
                        result["ResponseTime"] = round(time.time() - start_time, 2)

                        if "Indexed" in info.get("coverageState", ""):
                            result["Стан індексації"] = "Indexed"
                            result["Відправлено на індексацію"] = "No"
                            logging.info(f"✅ SKU {sku} ({lang}) Indexed: {url}")
                        else:
                            result["Стан індексації"] = "Not Indexed"
                            result["Відправлено на індексацію"] = "Yes"
                            result["Дата надсилання"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            if url not in none_index_urls:
                                with open(none_index_path, "a", encoding="utf-8", newline="") as f:
                                    writer_none = csv.DictWriter(f, fieldnames=["URL"])
                                    if os.path.getsize(none_index_path) == 0:
                                        writer_none.writeheader()
                                    writer_none.writerow({"URL": url})
                                    none_index_urls.add(url)
                            logging.warning(f"⚠️ SKU {sku} ({lang}) Not Indexed — відправлено: {url}")

                    except HttpError as e:
                        status = getattr(e, "resp", None).status if hasattr(e, "resp") else None
                        msg = str(e)
                        result["Стан індексації"] = "Error"
                        result["Помилка API"] = status
                        result["Опис помилки"] = msg
                        logging.error(f"❌ SKU {sku} ({lang}) Помилка API: {msg}")

                        # --- якщо перевищено квоту ---
                        if status == 429 or "quota" in msg.lower():
                            logging.warning(f"🚫 Ліміт квоти! URL додано у index_none_quota.csv → {url}")
                            if index_none_quota_path:
                                try:
                                    with open(index_none_quota_path, "a", encoding="utf-8", newline="") as f:
                                        writer_quota = csv.DictWriter(f, fieldnames=["URL"])
                                        if os.path.getsize(index_none_quota_path) == 0:
                                            writer_quota.writeheader()
                                        writer_quota.writerow({"URL": url})
                                except Exception as file_err:
                                    logging.error(f"❌ Не вдалося записати URL у index_none_quota.csv: {file_err}")

                    except Exception as e:
                        result["Стан індексації"] = "Error"
                        result["Опис помилки"] = str(e)
                        logging.error(f"❌ SKU {sku} ({lang}) Помилка API (невідома): {e}")

                    # --- Запис результату ---
                    writer.writerow(result)
                    existing_urls.add(url)

            except Exception as e:
                logging.error(f"❌ Помилка при обробці SKU {sku}: {e}", exc_info=True)

    cursor.close()
    conn.close()
    logging.info("🏁 Перевірку та індексацію нових товарів завершено.")

# --- ПОВТОРНА ПЕРЕВІРКА NONE_INDEX.CSV ---
def recheck_none_indexed_pages():
    """
    Перевіряє URL із none_index.csv:
    - Якщо вже індексований → оновлює або додає у index_google.csv і видаляє з none_index.csv.
    - Якщо ні → повторно відправляє на індексацію та переносить у кінець none_index.csv.
    - Зупиняється, якщо вичерпано квоту API або пройдено всі URL.
    """
    import csv, time
    from datetime import datetime
    from googleapiclient.errors import HttpError

    log_message_to_existing_file()
    logging.info("🚀 Початок повторної перевірки none_index.csv (реіндексація)")

    # --- 1️⃣ Завантаження налаштувань ---
    settings = load_settings()
    if not settings:
        logging.critical("❌ Не вдалося завантажити settings.json.")
        return

    paths = settings.get("paths", {})
    none_index_path = paths.get("none_index")
    index_log_path = paths.get("index_google")
    json_path = paths.get("google_json")

    if not all([none_index_path, index_log_path, json_path]):
        logging.critical("❌ Відсутні необхідні шляхи у settings.json (none_index, index_google, google_json).")
        return

    # --- 2️⃣ Підключення до Google Search Console API ---
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        credentials = service_account.Credentials.from_service_account_file(
            json_path, scopes=["https://www.googleapis.com/auth/webmasters"]
        )
        service = build("searchconsole", "v1", credentials=credentials)
    except Exception as e:
        logging.critical(f"❌ Не вдалося створити підключення до Search Console API: {e}")
        return

    # --- 3️⃣ Завантажуємо none_index.csv ---
    if not os.path.exists(none_index_path):
        logging.warning(f"⚠️ Файл {none_index_path} не знайдено.")
        return

    with open(none_index_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        urls = [row["URL"].strip() for row in reader if row.get("URL")]

    if not urls:
        logging.info("✅ Файл none_index.csv порожній — усі сторінки вже проіндексовані.")
        return

    # --- 4️⃣ Завантажуємо index_google.csv ---
    index_data = []
    existing_urls = set()
    index_fieldnames = [
        "URL","Тип сторінки","Стан індексації","Вердикт (verdict)","CoverageState",
        "Last Crawl Time","Page Fetch State","Indexing Allowed","Дата запиту",
        "Відправлено на індексацію","Дата надсилання","Помилка API","Опис помилки",
        "HTTP статус сторінки","Коментар","ResponseTime","Tries"
    ]

    if os.path.exists(index_log_path):
        with open(index_log_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            index_data = list(reader)
            for row in index_data:
                existing_urls.add(row["URL"].strip())

    # --- 5️⃣ Основний цикл перевірки ---
    processed = 0
    indexed_now = 0
    reindexed = 0

    original_urls = list(urls)
    updated_urls = []  # тут збиратимемо те, що залишиться в none_index

    for url in original_urls:
        processed += 1
        logging.info(f"🔍 Перевірка {url}")

        start_time = time.time()
        result_row = {
            "URL": url,
            "Тип сторінки": "product",
            "Дата запиту": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Tries": 1
        }

        try:
            inspect_body = {"inspectionUrl": url, "siteUrl": "https://eros.in.ua/"}
            res = service.urlInspection().index().inspect(body=inspect_body).execute()
            info = res.get("inspectionResult", {}).get("indexStatusResult", {})

            result_row["Вердикт (verdict)"] = info.get("verdict", "")
            result_row["CoverageState"] = info.get("coverageState", "")
            result_row["Last Crawl Time"] = info.get("lastCrawlTime", "")
            result_row["Page Fetch State"] = info.get("pageFetchState", "")
            result_row["Indexing Allowed"] = info.get("indexingState", "")
            result_row["ResponseTime"] = round(time.time() - start_time, 2)

            coverage = info.get("coverageState", "")
            verdict = info.get("verdict", "")
            is_indexed = ("Indexed" in coverage) or (verdict == "PASS")

            # --- Якщо Indexed ---
            if is_indexed:
                indexed_now += 1
                result_row["Стан індексації"] = "Indexed"
                result_row["Відправлено на індексацію"] = "No"
                logging.info(f"✅ {url} вже в індексі — оновлюю index_google і видаляю з none_index.")

                # Оновлюємо або додаємо до index_google
                updated = False
                for row in index_data:
                    if row["URL"] == url:
                        row.update(result_row)
                        updated = True
                        break
                if not updated:
                    index_data.append(result_row)

                # ❌ Не додаємо назад у updated_urls (тобто видаляємо)
                continue

            # --- Якщо не Indexed ---
            reindexed += 1
            result_row["Стан індексації"] = "Not Indexed"
            result_row["Відправлено на індексацію"] = "Yes"
            result_row["Дата надсилання"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logging.warning(f"⚠️ {url} не індексована — повторна відправка...")

            try:
                service.urlInspection().index().inspect(body=inspect_body).execute()
            except Exception as send_err:
                logging.error(f"❌ Помилка при повторному надсиланні {url}: {send_err}")

            # Оновлюємо або додаємо у index_google
            updated = False
            for row in index_data:
                if row["URL"] == url:
                    row.update(result_row)
                    updated = True
                    break
            if not updated:
                index_data.append(result_row)

            # 🔁 Додаємо у кінець черги (оновлений список)
            updated_urls.append(url)

        except HttpError as e:
            if "quota" in str(e).lower() or "resource_exhausted" in str(e).lower():
                logging.error("⚠️ Вичерпано денний ліміт API. Зупиняю перевірку.")
                break
            else:
                result_row["Стан індексації"] = "Error"
                result_row["Помилка API"] = getattr(e, "status_code", "")
                result_row["Опис помилки"] = str(e)
                logging.error(f"❌ API Error {url}: {e}")
                updated_urls.append(url)  # зберігаємо в черзі, бо не перевірено
        except Exception as e:
            result_row["Стан індексації"] = "Error"
            result_row["Опис помилки"] = str(e)
            logging.error(f"❌ Невідома помилка {url}: {e}")
            updated_urls.append(url)

        # --- Оновлюємо index_google.csv після кожного URL ---
        with open(index_log_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=index_fieldnames)
            writer.writeheader()
            writer.writerows(index_data)

        time.sleep(1)

    # --- 6️⃣ Запис оновленого none_index.csv ---
    with open(none_index_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["URL"])
        writer.writeheader()
        for u in updated_urls:
            writer.writerow({"URL": u})

    logging.info("🏁 Перевірку завершено.")
    logging.info(f"🧮 Всього перевірено: {processed}")
    logging.info(f"✅ В індексі: {indexed_now}")
    logging.info(f"📤 Повторно відправлено: {reindexed}")


# --- ПРЕЛОАД Fastcgi КЕШУ ---
USER_AGENTS = {
    "DESKTOP": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "MOBILE":  "Mozilla/5.0 (Linux; Android 10; Mobile) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
}

def _read_urls(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

def preload_cache_from_urls(source: int = 1, timeout: int = 20, pause_sec: float = 0.5):
    """
    Прелоад кешу Nginx БЕЗ очищення.
    source=1 -> settings['paths']['base_url']
    source=2 -> settings['paths']['product_url']
    Для кожного URL дві перевірки (DESKTOP/MOBILE). Лог — через logging.info().
    """
    # 1) Підключаємо лог-хендлер до існуючого файлу (ВАЖЛИВО: без аргументів)
    log_message_to_existing_file()

    settings = load_settings()
    if not settings:
        logging.info("❌ Не вдалося завантажити settings.json для прелоаду кешу.")
        return

    paths = settings.get("paths", {})
    if source == 1:
        urls_file = paths.get("base_url")
        source_name = "base_url"
    elif source == 2:
        urls_file = paths.get("product_url")
        source_name = "product_url"
    else:
        logging.info("❌ Параметр source має бути 1 або 2.")
        return

    if not urls_file or not os.path.exists(urls_file):
        logging.info(f"❌ Файл із URL не знайдено ({source_name}): {urls_file}")
        return

    urls = _read_urls(urls_file)
    total = len(urls)
    logging.info(f"🚀 Старт прелоаду кешу (source={source_name}, URLs={total})")

    with requests.Session() as session:
        session.headers.update({"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"})

        for i, url in enumerate(urls, start=1):
            for agent_name, agent_val in USER_AGENTS.items():
                headers = {"User-Agent": agent_val}
                t0 = time.perf_counter()
                status = None
                xfcc = "-"
                try:
                    resp = session.get(url, headers=headers, timeout=timeout)
                    status = resp.status_code
                    xfcc = resp.headers.get("X-FastCGI-Cache") or resp.headers.get("x-fastcgi-cache") or "-"
                except requests.RequestException as e:
                    status = "ERR"
                    xfcc = f"ERR:{type(e).__name__}"
                elapsed_ms = int((time.perf_counter() - t0) * 1000)

                logging.info(f"[{i}/{total}][{agent_name}] {url} -> {status}, X-FastCGI-Cache={xfcc}, {elapsed_ms}ms")

            if pause_sec and pause_sec > 0:
                time.sleep(pause_sec)

    logging.info("✅ Прелоад кешу завершено.")









