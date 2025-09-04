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


def check_exported_csv():
    """
    Перевіряє і очищає дані в експортованому CSV файлі.
    """
    settings = load_settings()
    if not settings:
        return

    csv_path = os.path.join(os.path.dirname(__file__), "..", "csv", "input", "zalishki.csv")
    if not os.path.exists(csv_path):
        print("❌ Файл експорту не знайдено.")
        return

    log_file_path = os.path.join(os.path.dirname(__file__), "..", "logs", "logs.log")
    log_message("🔍 Початок перевірки експортованого CSV.", log_file_path)

    temp_file_path = f"{csv_path}.temp"
    validation_errors = []
    processed_rows = []

    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile)
            headers = next(reader)
            
            row_number = 1
            for row in reader:
                row_number += 1
                row_id = row[0] if len(row) > 0 else "Невідомий ID"

                # Перевірка та очищення колонки 1 (ID)
                try:
                    int(row[0])
                except (ValueError, IndexError):
                    validation_errors.append(f"❌ Рядок {row_number}, ID {row_id}: Колонка 1 (ID) не є цілим числом. Значення: '{row[0]}'")
                
                # Перевірка та очищення колонки 2 (Артикул)
                try:
                    int(row[1])
                except (ValueError, IndexError):
                    validation_errors.append(f"❌ Рядок {row_number}, ID {row_id}: Колонка 2 (Артикул) не є цілим числом. Значення: '{row[1]}'")
                
                 # Перевірка колонки 3 (Назва)
                if len(row) > 2 and not row[2].strip():
                    validation_errors.append(f"❌ Рядок {row_number}, ID {row_id}: Колонка 3 (Назва) не може бути пустою.")

                # Перевірка та очищення колонки 4 (Опубліковано)
                if len(row) > 3 and row[3].lower() not in ["yes", "no"]:
                    validation_errors.append(f"❌ Рядок {row_number}, ID {row_id}: Колонка 4 (Опубліковано) очікує 'yes' або 'no'. Значення: '{row[3]}'")
                
                # Перевірка та очищення колонки 5 (Запаси)
                if len(row) > 4:
                    if row[4] == "":
                        row[4] = "0"
                        log_message(f"ℹ️ Рядок {row_number}, ID {row_id}: Колонка 5 (Запаси) була пустою, встановлено значення '0'.", log_file_path)
                    try:
                        int(row[4])
                    except ValueError:
                        validation_errors.append(f"❌ Рядок {row_number}, ID {row_id}: Колонка 5 (Запаси) не є цілим числом. Значення: '{row[4]}'")
                
                # Перевірка та очищення колонки 6 (Звичайна ціна)
                try:
                    int(row[5])
                except (ValueError, IndexError):
                    validation_errors.append(f"❌ Рядок {row_number}, ID {row_id}: Колонка 6 (Звичайна ціна) не є цілим числом. Значення: '{row[5]}'")
                
                # Перевірка колонки 7 (Категорії)
                if len(row) > 6 and not row[6].strip():
                    validation_errors.append(f"❌ Рядок {row_number}, ID {row_id}: Колонка 7 (Категорії) не може бути пустою.")

                # Очищення колонки 9 (Постачальник)
                if len(row) > 8:
                    row[8] = row[8].replace("[", "").replace("'", "").replace("]", "")
                
                # Очищення колонок 11, 13, 15, 17
                for i in [10, 12, 14, 16]:
                    if len(row) > i:
                        row[i] = row[i].replace("{'title': '', 'url': '", "").replace("', 'target': ''}", "")
                
                processed_rows.append(row)
    
    except Exception as e:
        print(f"❌ Виникла помилка під час перевірки файлу: {e}")
        log_message(f"❌ Виникла помилка під час перевірки файлу: {e}", log_file_path)
        return

    # Запис відсортованого та очищеного файлу
    print("⏳ Сортую дані та записую оновлений файл...")
    try:
        processed_rows.sort(key=lambda x: int(x[0]))
    except (ValueError, IndexError) as e:
        print(f"❌ Помилка сортування: неможливо перетворити ID на число. {e}")
        log_message(f"❌ Помилка сортування: неможливо перетворити ID на число. {e}", log_file_path)

    with open(temp_file_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(headers)
        writer.writerows(processed_rows)

    os.replace(temp_file_path, csv_path)

    # Логування результатів
    log_message("🎉 Перевірку та очищення файлу завершено.", log_file_path)
    if validation_errors:
        log_message(f"⚠️ Знайдено {len(validation_errors)} помилок:", log_file_path)
        for error in validation_errors:
            log_message(error, log_file_path)
        print(f"⚠️ Знайдено {len(validation_errors)} помилок. Деталі в лог-файлі.")
    else:
        log_message("✅ Всі дані коректні. Помилок не знайдено.", log_file_path)
        print("✅ Перевірка завершена, помилок не знайдено.")