from woocommerce import API
import json
import os
import json
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


def setup_logging(settings):
    """Налаштовує систему логування, використовуючи шляхи з налаштувань."""
    
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




def setup_log_file():
    """
    Перевіряє наявність logs.log, перейменовує його, якщо існує,
    та повертає шлях до нового файлу.
    """
    log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    current_log_path = os.path.join(log_dir, "logs.log")
    
    if os.path.exists(current_log_path):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        new_log_path = os.path.join(log_dir, f"logs_{timestamp}.log")
        try:
            os.rename(current_log_path, new_log_path)
            print(f"✅ Старий лог-файл перейменовано на {os.path.basename(new_log_path)}")
        except OSError as e:
            print(f"❌ Помилка при перейменуванні лог-файлу: {e}")
            return current_log_path

    return current_log_path


def log_message(message, log_file_path=os.path.join(os.path.dirname(__file__), "..", "logs", "logs.log")):
    """
    Записує повідомлення в лог-файл.
    """
    with open(log_file_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")


def update_log():
    """
    Налаштовує логування для дописування в logs.log.
    Ця функція обходить логіку перейменування в setup_log_file.
    """
    log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    log_file_path = os.path.join(log_dir, "logs.log")

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filemode='a'
    )
    
    return log_file_path




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
