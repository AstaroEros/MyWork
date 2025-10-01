from woocommerce import API
import json
import os
import json
import csv
import logging
from datetime import datetime


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
        timeout=30,
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
        logging.error(f"❌ Помилка: файл '{full_csv_path}' не знайдено.")
        return False
        
    logging.info(f"🔎 Початок перевірки файлу: {os.path.basename(full_csv_path)}")
    
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
                                    
                    except (ValueError, IndexError):
                        logging.error(f"❌ Непередбачена помилка в рядку {row_number}. Перевірка зупинена.")
                        return False

    except Exception as e:
        logging.error(f"❌ Виникла невідома помилка під час читання CSV: {e}", exc_info=True)
        return False
        
    logging.info(f"✅ Перевірка файлу {os.path.basename(full_csv_path)} пройшла успішно.")
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

def find_max_sku(zalishki_path: str) -> int:
    """
    Знаходить найбільше числове значення SKU в колонці B(1) файлу zalishki.csv.
    """
    SKU_ZALISHKI_INDEX = 1 # Колонка B
    max_sku = 0
    logging.info(f"Починаю пошук максимального SKU у файлі: {zalishki_path}")

    try:
        with open(zalishki_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader) # Пропускаємо заголовок
            
            for row in reader:
                if len(row) > SKU_ZALISHKI_INDEX:
                    sku_str = row[SKU_ZALISHKI_INDEX].strip()
                    try:
                        # Припускаємо, що SKU є цілими числами (int)
                        sku_int = int(sku_str)
                        if sku_int > max_sku:
                            max_sku = sku_int
                    except ValueError:
                        # Ігноруємо нечислові або порожні значення SKU
                        pass
        
        logging.info(f"Знайдено максимальний SKU у базі: {max_sku}")
        return max_sku
    
    except FileNotFoundError:
        logging.error(f"Файл бази zalishki.csv не знайдено за шляхом: {zalishki_path}")
        return 0
    except Exception as e:
        logging.error(f"Помилка при читанні zalishki.csv для пошуку SKU: {e}")
        return 0