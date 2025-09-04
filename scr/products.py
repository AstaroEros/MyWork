import csv
import json
import os
import time
from scr.updater import get_wc_api
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

def setup_log_file():
    """
    Перевіряє наявність logs.log, перейменовує його, якщо існує,
    та повертає шлях до нового файлу.
    """
    log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    current_log_path = os.path.join(log_dir, "logs.log")
    
    # Перевіряємо, чи існує файл logs.log
    if os.path.exists(current_log_path):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        new_log_path = os.path.join(log_dir, f"logs_{timestamp}.log")
        try:
            os.rename(current_log_path, new_log_path)
            print(f"✅ Старий лог-файл перейменовано на {os.path.basename(new_log_path)}")
        except OSError as e:
            print(f"❌ Помилка при перейменуванні лог-файлу: {e}")
            # Якщо не вдалося перейменувати, повертаємо старий шлях
            return current_log_path

    # Повертаємо шлях, який буде використовуватись для запису логів
    return current_log_path

def log_message(message, log_file_path):
    """
    Записує повідомлення в лог-файл.
    """
    with open(log_file_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

def export_products():
    """
    Експорт усіх товарів у CSV пачками по 100.
    """
    settings = load_settings()
    if not settings:
        return
        
    log_file_path = setup_log_file()
    
    csv_path = os.path.join(os.path.dirname(__file__), "..", "csv", "input", "zalishki.csv")
    
    headers = [
        "ID", "Артикул", "Назва", "Опубліковано", "Запаси", "Звичайна ціна", "Категорії",
        "Мета: shtrih_cod", "Мета: postachalnyk", "Мета: artykul_lutsk", "Мета: url_lutsk",
        "Мета: artykul_blizklub", "Мета: url_blizklub",
        "Мета: artykul_sexopt", "Мета: url_sexopt",
        "Мета: artykul_biorytm", "Мета: url_biorytm",
        "Мета: artykul_berdiansk"
    ]

    wcapi = get_wc_api()
    start_time = time.time()
    total_products = 0
    exported_count = 0
    errors = []

    log_message("🚀 Початок експорту товарів.", log_file_path)

    try:
        response = wcapi.get("products", params={"per_page": 1})
        if response.status_code != 200:
            error_msg = f"Помилка {response.status_code} при підрахунку товарів: {response.text}"
            print(f"❌ {error_msg}")
            log_message(f"❌ {error_msg}", log_file_path)
            errors.append(error_msg)
            return

        total_products = int(response.headers.get("X-WP-Total", 0))
        print(f"🔎 Загальна кількість товарів: {total_products}")
        log_message(f"🔎 Загальна кількість товарів: {total_products}", log_file_path)

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

            page = 1
            while exported_count < total_products:
                response = wcapi.get(
                    "products",
                    params={
                        "per_page": 100,
                        "page": page,
                        "_fields": "id,sku,name,status,stock_quantity,regular_price,categories,meta_data"
                    }
                )

                if response.status_code != 200:
                    error_msg = f"Помилка {response.status_code} на сторінці {page}: {response.text}"
                    print(f"❌ {error_msg}")
                    log_message(f"❌ {error_msg}", log_file_path)
                    errors.append(error_msg)
                    break

                products = response.json()
                if not products:
                    break

                for product in products:
                    product["meta_data_dict"] = {m["key"]: m["value"] for m in product.get("meta_data", [])}
                    row = [
                        product.get("id"),
                        product.get("sku"),
                        product.get("name"),
                        "yes" if product.get("status") == "publish" else "no",
                        product.get("stock_quantity"),
                        product.get("regular_price"),
                        ", ".join([cat["name"] for cat in product.get("categories", [])]),
                        product["meta_data_dict"].get("shtrih_cod", ""),
                        product["meta_data_dict"].get("postachalnyk", ""),
                        product["meta_data_dict"].get("artykul_lutsk", ""),
                        product["meta_data_dict"].get("url_lutsk", ""),
                        product["meta_data_dict"].get("artykul_blizklub", ""),
                        product["meta_data_dict"].get("url_blizklub", ""),
                        product["meta_data_dict"].get("artykul_sexopt", ""),
                        product["meta_data_dict"].get("url_sexopt", ""),
                        product["meta_data_dict"].get("artykul_biorytm", ""),
                        product["meta_data_dict"].get("url_biorytm", ""),
                        product["meta_data_dict"].get("artykul_berdiansk", "")
                    ]
                    writer.writerow(row)
                    exported_count += 1
                
                if exported_count % 100 == 0 or exported_count == total_products:
                    elapsed = int(time.time() - start_time)
                    status_message = f"✅ Вивантажено {exported_count} з {total_products} ({elapsed} сек)"
                    print(status_message)
                    log_message(status_message, log_file_path)

                page += 1
                time.sleep(1)

    except Exception as e:
        error_msg = f"Виникла невідома помилка під час експорту: {e}"
        print(f"❌ {error_msg}")
        log_message(f"❌ {error_msg}", log_file_path)
        errors.append(error_msg)
    finally:
        end_time = time.time()
        elapsed_time = int(end_time - start_time)
        
        print(f"🎉 Експорт завершено. Вивантажено {exported_count} з {total_products} товарів за {elapsed_time} сек.")
        if errors:
            print(f"⚠️ Експорт завершився з {len(errors)} помилками. Деталі в лог-файлі.")
        
        log_message(f"--- Підсумок експорту ---", log_file_path)
        log_message(f"Статус: {'Успішно' if not errors else 'Завершено з помилками'}", log_file_path)
        log_message(f"Кількість товарів: {exported_count} з {total_products}", log_file_path)
        log_message(f"Тривалість: {elapsed_time} сек.", log_file_path)
        if errors:
            log_message(f"Кількість помилок: {len(errors)}", log_file_path)
            log_message("Перелік помилок:", log_file_path)
            for err in errors:
                log_message(f"- {err}", log_file_path)