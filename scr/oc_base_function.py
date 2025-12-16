# /var/www/scripts/update/scr/oc_base_function.py

import os
import json
import logging
import pymysql
from datetime import datetime


# --- 1. ЗАВАНТАЖЕННЯ НАЛАШТУВАНЬ OPENCART ---
def load_oc_settings():
    """
    Завантаження конфігурації з oc_settings.json
    """
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "oc_settings.json")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    except FileNotFoundError:
        print(f"❌ Помилка: oc_settings.json не знайдено за шляхом: {config_path}")
        return None

    except json.JSONDecodeError:
        print(f"❌ Помилка: файл oc_settings.json пошкоджений: {config_path}")
        return None



# --- 2. ПІДКЛЮЧЕННЯ ДО БД OPENCART ---
def oc_connect_db():
    """
    Повертає активне підключення до бази OpenCart
    """
    settings = load_oc_settings()
    if not settings or "db" not in settings:
        raise Exception("❌ Неможливо завантажити DB-налаштування з oc_settings.json")

    db = settings["db"]

    try:
        connection = pymysql.connect(
            host=db["host"],
            user=db["user"],
            password=db["password"],
            database=db["database"],
            port=db.get("port", 3306),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection

    except Exception as e:
        print(f"❌ Помилка підключення до БД OpenCart: {e}")
        return None



# --- 3. СТВОРЕННЯ НОВОГО ЛОГ-ФАЙЛУ (ОЧИСТКА/АРХІВАЦІЯ) ---
def oc_setup_new_log_file():
    """
    Створює новий лог-файл oc_logs.log.
    Якщо файл існує — перейменовує його у архів із датою.
    """
    log_path = "/var/www/scripts/update/logs/oc_logs.log"
    log_dir = os.path.dirname(log_path)

    os.makedirs(log_dir, exist_ok=True)

    # архівація старого файлу
    if os.path.exists(log_path):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        archive_name = f"oc_logs_{timestamp}.log"
        archive_path = os.path.join(log_dir, archive_name)

        try:
            os.rename(log_path, archive_path)
            print(f"📦 Старий лог oc_logs.log архівовано як {archive_name}")
        except OSError as e:
            print(f"❌ Помилка при архівації лог-файлу: {e}")

    # створення нового
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filemode="w"
    )

    logging.info("--- Новий сеанс логування OpenCart розпочато ---")
    print("✅ Створено новий файл oc_logs.log")



# --- 4. ДОПИСУВАННЯ В ІСНУЮЧИЙ oc_logs.log ---
def oc_log_message(message: str):
    """
    Дописує повідомлення у існуючий лог-файл oc_logs.log
    """
    log_path = "/var/www/scripts/update/logs/oc_logs.log"
    log_dir = os.path.dirname(log_path)

    os.makedirs(log_dir, exist_ok=True)

    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format="%(asctime)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            filemode="a"
        )

    logging.info(message)
    print(f"📝 {message}")
