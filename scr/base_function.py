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

"""
def setup_logging(settings):
   Налаштовує систему логування, використовуючи шляхи з налаштувань.
    
    # Використовуємо os.path.join для коректного об'єднання шляхів
    base_dir = os.path.join(os.path.dirname(__file__), "..")
    log_dir = os.path.join(base_dir, settings["paths"]["logs_dir"])
    current_log_path = os.path.join(log_dir, settings["paths"]["main_log_file"])

    os.makedirs(log_dir, exist_ok=True)
    
    if os.path.exists(current_log_path):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        new_log_file = f"logs_{timestamp}.log"
        new_log_path = os.path.join(log_dir, new_log_file)
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

"""
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