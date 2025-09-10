import csv
import json
import os
import time
import requests
import shutil
import re
import pandas as pd
import random # Новий імпорт
from scr.updater import get_wc_api
from datetime import datetime, timedelta

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



def download_supplier_price_list(supplier_id):
    """
    Скачує прайс-лист від постачальника за його ID.
    """
    settings = load_settings()
    if not settings:
        return
    
    supplier_info = settings.get("suppliers", {}).get(str(supplier_id))
    if not supplier_info:
        print(f"❌ Помилка: Інформацію про постачальника з ID '{supplier_id}' не знайдено.")
        return

    base_dir = os.path.join(os.path.dirname(__file__), "..")
    log_file_path = os.path.join(base_dir, settings.get("log_file_path"))
    
    url = supplier_info.get("download_url")
    csv_path = os.path.join(base_dir, supplier_info.get("csv_path"))
   
    log_message(f"⏳ Запускаю завантаження прайс-листа від {supplier_id} (ID: {supplier_id}).", log_file_path)

    # Видалення старого файлу, якщо він існує
    if os.path.exists(csv_path):
        try:
            os.remove(csv_path)
            log_message(f"✅ Старий файл {os.path.basename(csv_path)} успішно видалено.", log_file_path)
        except OSError as e:
            log_message(f"❌ Помилка при видаленні старого файлу: {e}", log_file_path)
            print(f"❌ Помилка: {e}")
            return
    
    try:
        # Завантаження файлу
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(csv_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        
        log_message(f"🎉 Прайс-лист від {supplier_id} успішно завантажено.", log_file_path)
        print(f"✅ Прайс-лист від {supplier_id} успішно завантажено.")
    except requests.exceptions.RequestException as e:
        log_message(f"❌ Помилка завантаження файлу від {supplier_id}: {e}", log_file_path)
        print(f"❌ Помилка завантаження файлу: {e}")


def process_supplier_1_price_list():
    """
    Обробляє та очищає прайс-лист від постачальника 1.
    """
    settings = load_settings()
    if not settings:
        return

    supplier_id = "1"
    supplier_info = settings.get("suppliers", {}).get(supplier_id)
    if not supplier_info:
        print(f"❌ Помилка: Інформацію про постачальника з ID '{supplier_id}' не знайдено.")
        return

    base_dir = os.path.join(os.path.dirname(__file__), "..")
    log_file_path = os.path.join(base_dir, settings.get("log_file_path"))
    csv_path = os.path.join(base_dir, supplier_info.get("csv_path"))
    delimiter = supplier_info.get("delimiter", ",")

    if not os.path.exists(csv_path):
        print(f"❌ Файл прайс-листа для постачальника {supplier_id} не знайдено за шляхом: {csv_path}")
        return

    log_message(f"⚙️ Запускаю обробку прайс-листа для постачальника {supplier_id}.", log_file_path)

    # Список слів для видалення рядків
    words_to_filter_from_name = ["jos", "a-toys"]
    words_to_filter_from_brand = ["toyfa"]

    temp_file_path = f"{csv_path}.temp"
    processed_rows = []
    skipped_rows = 0
    total_rows = 0

    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile, delimiter=delimiter)
            headers = next(reader)
            processed_rows.append(headers)
            
            row_number = 1
            for row in reader:
                row_number += 1
                total_rows += 1

                # Перевірка колонки 4 (ціна) на наявність літер
                if len(row) > 3:
                    price_value = row[3]
                    if re.search(r'[a-zA-Z]', price_value):
                        log_message(f"🚫 Видалено рядок {row_number} через наявність літер у колонці 4 (ціна): '{price_value}'.", log_file_path)
                        skipped_rows += 1
                        continue

                # Фільтрація за колонкою 3 (назва)
                if len(row) > 2:
                    product_name = row[2].lower()
                    if any(word in product_name for word in words_to_filter_from_name):
                        log_message(f"🚫 Видалено рядок {row_number} через заборонене слово в назві ('{row[2]}').", log_file_path)
                        skipped_rows += 1
                        continue
                
                # Фільтрація за колонкою 8 (бренд)
                if len(row) > 7:
                    brand_name = row[7].lower()
                    if any(word in brand_name for word in words_to_filter_from_brand):
                        log_message(f"🚫 Видалено рядок {row_number} через заборонене слово в бренді ('{row[7]}').", log_file_path)
                        skipped_rows += 1
                        continue

                # Перетворення колонки 4 (ціна) з float на int
                if len(row) > 3 and row[3]:
                    try:
                        row[3] = str(int(float(row[3])))
                    except (ValueError, IndexError):
                        log_message(f"⚠️ Помилка перетворення ціни в рядку {row_number}. Значення: '{row[3]}'", log_file_path)
                
                # Заміна значення в колонці 7 (категорія)
                if len(row) > 6 and row[6] == ">3":
                    row[6] = "4"
                    
                processed_rows.append(row)
    
    except Exception as e:
        log_message(f"❌ Виникла помилка під час обробки файлу: {e}", log_file_path)
        print(f"❌ Виникла помилка під час обробки файлу: {e}")
        return

    with open(temp_file_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile, delimiter=delimiter)
        writer.writerows(processed_rows)

    os.replace(temp_file_path, csv_path)

    log_message(f"🎉 Обробку прайс-листа для постачальника {supplier_id} завершено.", log_file_path)
    log_message(f"--- Підсумок обробки: ---", log_file_path)
    log_message(f"📦 Всього рядків у файлі: {total_rows}", log_file_path)
    log_message(f"🗑️ Видалено рядків: {skipped_rows}", log_file_path)
    log_message(f"✅ Оброблені рядки: {len(processed_rows) - 1}", log_file_path)
    print("✅ Обробка прайс-листа завершена. Деталі в лог-файлі.")



def process_supplier_2_price_list():
    """
    Обробляє та очищає прайс-лист від постачальника 2.
    """
    settings = load_settings()
    if not settings:
        return

    supplier_id = "2"
    supplier_info = settings.get("suppliers", {}).get(supplier_id)
    if not supplier_info:
        print(f"❌ Помилка: Інформацію про постачальника з ID '{supplier_id}' не знайдено.")
        return

    base_dir = os.path.join(os.path.dirname(__file__), "..")
    log_file_path = os.path.join(base_dir, settings.get("log_file_path"))
    csv_path = os.path.join(base_dir, supplier_info.get("csv_path"))
    delimiter = supplier_info.get("delimiter", ",")

    if not os.path.exists(csv_path):
        print(f"❌ Файл прайс-листа для постачальника {supplier_id} не знайдено за шляхом: {csv_path}")
        return

    log_message(f"⚙️ Запускаю обробку прайс-листа для постачальника {supplier_id}.", log_file_path)

    temp_file_path = f"{csv_path}.temp"
    processed_rows = []
    skipped_rows = 0
    total_rows = 0
    modifications_count = 0

    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile, delimiter=delimiter)
            headers = next(reader)
            processed_rows.append(headers)
            
            row_number = 1
            for row in reader:
                row_number += 1
                total_rows += 1

                # Перевірка колонки 5 (валюта)
                if len(row) > 4:
                    currency_value = row[4].strip().upper()
                    if currency_value != "UAH":
                        log_message(f"🚫 Видалено рядок {row_number} через некоректну валюту у колонці 5: '{row[4]}'.", log_file_path)
                        skipped_rows += 1
                        continue
                else:
                    log_message(f"🚫 Видалено рядок {row_number} через відсутність значення у колонці 5 (валюта).", log_file_path)
                    skipped_rows += 1
                    continue

                # Перетворення колонки 4 (ціна) з float на int
                if len(row) > 3 and row[3]:
                    try:
                        row[3] = str(int(float(row[3])))
                    except (ValueError, IndexError):
                        log_message(f"⚠️ Помилка перетворення ціни в рядку {row_number}. Значення: '{row[3]}'", log_file_path)
                
                # Заміна значення в колонці 7 (категорія)
                if len(row) > 6 and row[6] == ">3":
                    row[6] = "4"
                    modifications_count += 1

                processed_rows.append(row)
    
    except Exception as e:
        log_message(f"❌ Виникла помилка під час обробки файлу: {e}", log_file_path)
        print(f"❌ Виникла помилка під час обробки файлу: {e}")
        return

    with open(temp_file_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile, delimiter=delimiter)
        writer.writerows(processed_rows)

    os.replace(temp_file_path, csv_path)

    log_message(f"🎉 Обробку прайс-листа для постачальника {supplier_id} завершено.", log_file_path)
    log_message(f"--- Підсумок обробки: ---", log_file_path)
    log_message(f"📦 Всього рядків у файлі: {total_rows}", log_file_path)
    log_message(f"🗑️ Видалено рядків: {skipped_rows}", log_file_path)
    log_message(f"📝 Змінено категорій: {modifications_count}", log_file_path)
    log_message(f"✅ Оброблені рядки: {len(processed_rows) - 1}", log_file_path)
    print("✅ Обробка прайс-листа завершена. Деталі в лог-файлі.")


def process_supplier_3_price_list():
    """
    Обробляє та конвертує прайс-лист від постачальника 3 (формат .xls),
    а потім фільтрує дані.
    """
    settings = load_settings()
    if not settings:
        return

    supplier_id = "3"
    supplier_info = settings.get("suppliers", {}).get(supplier_id)
    if not supplier_info:
        print(f"❌ Помилка: Інформацію про постачальника з ID '{supplier_id}' не знайдено.")
        return

    base_dir = os.path.join(os.path.dirname(__file__), "..")
    log_file_path = os.path.join(base_dir, settings.get("log_file_path"))
    
    # Шлях до вхідного XLS-файлу
    xls_path = os.path.join(base_dir, supplier_info.get("csv_path"))
    
    # Шлях до вихідного CSV-файлу
    csv_name = os.path.join(base_dir, supplier_info.get("csv_name"))

    log_message(f"⚙️ Запускаю обробку прайс-листа для постачальника {supplier_id}...", log_file_path)

    # 1. Видалення старого CSV-файлу
    if os.path.exists(csv_name):
        try:
            os.remove(csv_name)
            log_message(f"✅ Старий файл {os.path.basename(csv_name)} успішно видалено.", log_file_path)
        except OSError as e:
            log_message(f"❌ Помилка при видаленні старого CSV-файлу: {e}", log_file_path)
            print(f"❌ Помилка: {e}")
            return

    # 2. Конвертація XLS в CSV
    if not os.path.exists(xls_path):
        log_message(f"❌ Файл .xls для постачальника {supplier_id} не знайдено за шляхом: {xls_path}", log_file_path)
        print("❌ Файл .xls не знайдено.")
        return

    try:
        df = pd.read_excel(xls_path)
        df.to_csv(csv_name, index=False, encoding="utf-8")
        
        log_message(f"🎉 Файл .xls для постачальника {supplier_id} успішно конвертовано в {os.path.basename(csv_name)}.", log_file_path)
        print("✅ Конвертація завершена.")

    except Exception as e:
        log_message(f"❌ Помилка під час конвертації файлу: {e}", log_file_path)
        print(f"❌ Помилка під час конвертації: {e}")
        return

    # 3. Фільтрація та очищення CSV-файлу
    log_message(f"🔍 Запускаю фільтрацію даних у {os.path.basename(csv_name)}.", log_file_path)

    temp_file_path = f"{csv_name}.temp"
    processed_rows = []
    skipped_rows = 0
    total_rows = 0

    try:
        with open(csv_name, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile)
            headers = next(reader)
            processed_rows.append(headers)
            
            row_number = 1
            for row in reader:
                row_number += 1
                total_rows += 1

                # Перевірка, що рядок містить достатньо колонок
                if len(row) < 4:
                    log_message(f"🚫 Видалено рядок {row_number} через недостатню кількість колонок.", log_file_path)
                    skipped_rows += 1
                    continue

                # Перевірка колонок 3 та 4 на ціле число >= 0
                is_valid = True
                for col_index in [2, 3]:
                    value = row[col_index]
                    try:
                        int_value = int(float(value)) # Використовуємо float, щоб обробляти числа з .00
                        if int_value < 0:
                            log_message(f"🚫 Видалено рядок {row_number} через від'ємне значення в колонці {col_index + 1}: '{value}'.", log_file_path)
                            is_valid = False
                            break
                    except (ValueError, IndexError):
                        log_message(f"🚫 Видалено рядок {row_number} через некоректне числове значення в колонці {col_index + 1}: '{value}'.", log_file_path)
                        is_valid = False
                        break

                if is_valid:
                    processed_rows.append(row)
                else:
                    skipped_rows += 1
    
    except Exception as e:
        log_message(f"❌ Виникла помилка під час фільтрації файлу: {e}", log_file_path)
        print(f"❌ Виникла помилка під час фільтрації файлу: {e}")
        return

    # Запис відфільтрованих даних у файл
    with open(temp_file_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerows(processed_rows)

    os.replace(temp_file_path, csv_name)

    log_message(f"🎉 Обробку та фільтрацію прайс-листа для постачальника {supplier_id} завершено.", log_file_path)
    log_message(f"--- Підсумок обробки: ---", log_file_path)
    log_message(f"📦 Всього рядків у файлі: {total_rows}", log_file_path)
    log_message(f"🗑️ Видалено рядків: {skipped_rows}", log_file_path)
    log_message(f"✅ Оброблені рядки: {len(processed_rows) - 1}", log_file_path)
    print("✅ Обробка прайс-листа завершена. Деталі в лог-файлі.")


def process_and_combine_all_data():
    """
    Обробляє прайс-листи та об'єднує дані у зведену таблицю.
    """
    settings = load_settings()
    if not settings:
        return

    base_dir = os.path.join(os.path.dirname(__file__), "..")
    log_file_path = os.path.join(base_dir, settings.get("log_file_path"))
    
    zalishki_path = os.path.join(base_dir, settings.get("csv_path_zalishki"))
    zvedena_path = os.path.join(base_dir, "csv", "process", "zvedena.csv")
    
    # Інформація про постачальників
    supplier_info_1 = settings.get("suppliers", {}).get("1")
    supplier_csv_path_1 = os.path.join(base_dir, supplier_info_1.get("csv_path"))
    supplier_delimiter_1 = supplier_info_1.get("delimiter", ",")
    
    supplier_info_2 = settings.get("suppliers", {}).get("2")
    supplier_csv_path_2 = os.path.join(base_dir, supplier_info_2.get("csv_path"))
    supplier_delimiter_2 = supplier_info_2.get("delimiter", ",")

    supplier_info_3 = settings.get("suppliers", {}).get("3")
    supplier_csv_path_3 = os.path.join(base_dir, supplier_info_3.get("csv_name"))
    supplier_delimiter_3 = supplier_info_3.get("delimiter", ",")
    
    supplier_info_4 = settings.get("suppliers", {}).get("4")
    supplier_csv_path_4 = os.path.join(base_dir, supplier_info_4.get("csv_path"))
    supplier_delimiter_4 = supplier_info_4.get("delimiter", ",")

    zvedena_names_map = settings.get("column_zvedena_name")
    new_header = [zvedena_names_map.get(str(i)) for i in range(len(zvedena_names_map))]
    
    zalishki_columns = ["0", "1", "7", "9", "11", "13", "4", "5", "3", "2", "6"]
    
    supplier_1_columns = ["0", "3", "6"]
    supplier_1_match_column = "0"
    zvedena_match_column_1 = "3"
    
    supplier_2_columns = ["0", "3", "6"]
    supplier_2_match_column = "0"
    zvedena_match_column_2 = "4"

    supplier_3_columns = ["0", "2", "3"]
    supplier_3_match_column = "0"
    zvedena_match_column_3 = "5"
    
    supplier_4_columns = ["5", "4", "6"]
    supplier_4_match_column = "5"
    zvedena_match_column_4 = "1"

    if not os.path.exists(zalishki_path):
        log_message(f"❌ Файл залишків не знайдено: {zalishki_path}", log_file_path)
        print("❌ Файл залишків не знайдено.")
        return
    if not os.path.exists(supplier_csv_path_1):
        log_message(f"❌ Прайс-лист постачальника 1 не знайдено: {supplier_csv_path_1}", log_file_path)
        print("❌ Прайс-лист постачальника 1 не знайдено.")
        return
    if not os.path.exists(supplier_csv_path_2):
        log_message(f"❌ Прайс-лист постачальника 2 не знайдено: {supplier_csv_path_2}", log_file_path)
        print("❌ Прайс-лист постачальника 2 не знайдено.")
        return
    if not os.path.exists(supplier_csv_path_3):
        log_message(f"❌ Прайс-лист постачальника 3 не знайдено: {supplier_csv_path_3}", log_file_path)
        print("❌ Прайс-лист постачальника 3 не знайдено.")
        return
    if not os.path.exists(supplier_csv_path_4):
        log_message(f"❌ Прайс-лист постачальника 4 не знайдено: {supplier_csv_path_4}", log_file_path)
        print("❌ Прайс-лист постачальника 4 не знайдено.")
        return

    log_message("⚙️ Запускаю повний процес обробки та об'єднання даних...", log_file_path)
    
    if os.path.exists(zvedena_path):
        try:
            os.remove(zvedena_path)
            log_message(f"✅ Старий файл {os.path.basename(zvedena_path)} успішно видалено.", log_file_path)
        except OSError as e:
            log_message(f"❌ Помилка при видаленні старого файлу: {e}", log_file_path)
            print(f"❌ Помилка: {e}")
            return
    
    supplier_data_dict_1 = {}
    try:
        with open(supplier_csv_path_1, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile, delimiter=supplier_delimiter_1)
            next(reader) 
            for row in reader:
                if len(row) > max(int(col) for col in supplier_1_columns):
                    key = row[int(supplier_1_match_column)].strip()
                    values = [row[int(col)].strip() for col in supplier_1_columns]
                    supplier_data_dict_1[key] = values
    except Exception as e:
        log_message(f"❌ Помилка при читанні прайс-листа постачальника 1: {e}", log_file_path)
        print(f"❌ Помилка при читанні прайс-листа постачальника 1: {e}")
        return

    supplier_data_dict_2 = {}
    try:
        with open(supplier_csv_path_2, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile, delimiter=supplier_delimiter_2)
            next(reader) 
            for row in reader:
                if len(row) > max(int(col) for col in supplier_2_columns):
                    key = row[int(supplier_2_match_column)].strip()
                    values = [row[int(col)].strip() for col in supplier_2_columns]
                    supplier_data_dict_2[key] = values
    except Exception as e:
        log_message(f"❌ Помилка при читанні прайс-листа постачальника 2: {e}", log_file_path)
        print(f"❌ Помилка при читанні прайс-листа постачальника 2: {e}")
        return

    supplier_data_dict_3 = {}
    try:
        with open(supplier_csv_path_3, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile, delimiter=supplier_delimiter_3)
            next(reader) 
            for row in reader:
                if len(row) > max(int(col) for col in supplier_3_columns):
                    key = row[int(supplier_3_match_column)].strip()
                    values = [row[int(col)].strip() for col in supplier_3_columns]
                    supplier_data_dict_3[key] = values
    except Exception as e:
        log_message(f"❌ Помилка при читанні прайс-листа постачальника 3: {e}", log_file_path)
        print(f"❌ Помилка при читанні прайс-листа постачальника 3: {e}")
        return

    supplier_data_dict_4 = {}
    try:
        with open(supplier_csv_path_4, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile, delimiter=supplier_delimiter_4)
            next(reader) 
            for row in reader:
                if len(row) > max(int(col) for col in supplier_4_columns):
                    key = row[int(supplier_4_match_column)].strip()
                    values = [row[int(col)].strip() for col in supplier_4_columns]
                    supplier_data_dict_4[key] = values
    except Exception as e:
        log_message(f"❌ Помилка при читанні прайс-листа постачальника 4: {e}", log_file_path)
        print(f"❌ Помилка при читанні прайс-листа постачальника 4: {e}")
        return

    processed_rows = []
    processed_count = 0
    updated_by_s1_count = 0
    updated_by_s2_count = 0
    updated_by_s3_count = 0
    updated_by_s4_count = 0

    # Визначаємо індекси колонок для формул згідно з вашими даними
    formula_cols = {
        'N': 13, 'Q': 16, 'S': 18, 'V': 21,
        'M': 12, 'P': 15, 'T': 19, 'W': 22,
        'H': 7,
        'I': 8,
        'X': 23,
        'G': 6,
        'Y': 24
    }

    try:
        with open(zalishki_path, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile)
            next(reader)
            
            # Додаємо нові колонки з назвами з settings.json
            processed_rows.append(new_header)
            
            for row in reader:
                processed_count += 1
                if len(row) > 13:
                    new_row = [row[int(col_index)] for col_index in zalishki_columns]
                    
                    supplier_data_1 = supplier_data_dict_1.get(new_row[int(zvedena_match_column_1)].strip(), ["", "", ""])
                    supplier_data_2 = supplier_data_dict_2.get(new_row[int(zvedena_match_column_2)].strip(), ["", "", ""])
                    supplier_data_3 = supplier_data_dict_3.get(new_row[int(zvedena_match_column_3)].strip(), ["", "", ""])
                    supplier_data_4 = supplier_data_dict_4.get(new_row[int(zvedena_match_column_4)].strip(), ["", "", ""])
                    
                    new_row.extend(supplier_data_1)
                    new_row.extend(supplier_data_2)
                    new_row.extend(supplier_data_3)
                    new_row.extend(supplier_data_4)

                    # Обчислення для колонки 23: max(N, Q, S, V)
                    quantities_to_compare = []
                    for col_name in ['N', 'Q', 'S', 'V']:
                        try:
                            index = formula_cols[col_name]
                            val = new_row[index].strip()
                            quantities_to_compare.append(int(val) if val else 0)
                        except (KeyError, IndexError):
                            quantities_to_compare.append(0)
                    
                    max_quantity = max(quantities_to_compare) if quantities_to_compare else 0
                    new_row.append(str(max_quantity))

                    # Обчислення для колонки 24: if((M + P + T + W) = 0; H; min(M; P; T; W))
                    quantities_for_sum = []
                    valid_quantities_for_min = []
                    
                    for col_name in ['M', 'P', 'T', 'W']:
                        try:
                            index = formula_cols[col_name]
                            val = new_row[index].strip()
                            num_val = int(val) if val else 0
                            quantities_for_sum.append(num_val)
                            if num_val > 0:
                                valid_quantities_for_min.append(num_val)
                        except (KeyError, IndexError):
                            quantities_for_sum.append(0)
                    
                    if sum(quantities_for_sum) == 0:
                        result_24 = new_row[formula_cols['H']]
                    else:
                        if valid_quantities_for_min:
                            result_24 = min(valid_quantities_for_min)
                        else:
                            result_24 = 0

                    new_row.append(str(result_24))

                    # Обчислення для колонки 25: if(I = "yes"; 1; 0)
                    try:
                        i_val = new_row[formula_cols['I']].strip().lower()
                    except IndexError:
                        i_val = ""
                    
                    result_25 = 1 if i_val == "yes" else 0
                    new_row.append(str(result_25))
                    
                    # Обчислення для колонки 26: IF((X - G) = 0; 0; 1)
                    x_val = 0
                    g_val = 0
                    try:
                        x_val = int(new_row[formula_cols['X']])
                    except (ValueError, IndexError):
                        x_val = 0
                    
                    try:
                        g_val = int(new_row[formula_cols['G']])
                    except (ValueError, IndexError):
                        g_val = 0
                    
                    if (x_val - g_val) == 0:
                        result_26 = 0
                    else:
                        result_26 = 1
                    
                    new_row.append(str(result_26))
                    #log_message(f"рядок {processed_count}: X = \"{x_val}\", G = \"{g_val}\". (X - G) = \"{x_val - g_val}\". Результат для колонки 26 = \"{result_26}\"", log_file_path)

                    # Обчислення для колонки 27: IF((Y - H) = 0; 0; 1)
                    y_val = 0
                    h_val = 0
                    try:
                        y_val = int(new_row[formula_cols['Y']])
                    except (ValueError, IndexError):
                        y_val = 0
                    
                    try:
                        h_val = int(new_row[formula_cols['H']])
                    except (ValueError, IndexError):
                        h_val = 0
                    
                    if (y_val - h_val) == 0:
                        result_27 = 0
                    else:
                        result_27 = 1
                    
                    new_row.append(str(result_27))
                    #log_message(f"рядок {processed_count}: Y = \"{y_val}\", H = \"{h_val}\". (Y - H) = \"{y_val - h_val}\". Результат для колонки 27 = \"{result_27}\"", log_file_path)

                    if supplier_data_1[0] != "":
                        updated_by_s1_count += 1
                    if supplier_data_2[0] != "":
                        updated_by_s2_count += 1
                    if supplier_data_3[0] != "":
                        updated_by_s3_count += 1
                    if supplier_data_4[0] != "":
                        updated_by_s4_count += 1
                    
                    processed_rows.append(new_row)
    
    except Exception as e:
        log_message(f"❌ Виникла помилка під час обробки даних: {e}", log_file_path)
        print(f"❌ Виникла помилка під час обробки даних: {e}")
        return

    try:
        with open(zvedena_path, "w", newline="", encoding="utf-8") as outfile:
            writer = csv.writer(outfile)
            writer.writerows(processed_rows)

        log_message("🎉 Повний процес обробки та об'єднання завершено!", log_file_path)
        log_message(f"--- Підсумок: ---", log_file_path)
        log_message(f"📦 Всього рядків у файлі залишків: {processed_count}", log_file_path)
        log_message(f"✅ Оновлено даними постачальника 1: {updated_by_s1_count} рядків.", log_file_path)
        log_message(f"✅ Оновлено даними постачальника 2: {updated_by_s2_count} рядків.", log_file_path)
        log_message(f"✅ Оновлено даними постачальника 3: {updated_by_s3_count} рядків.", log_file_path)
        log_message(f"✅ Оновлено даними постачальника 4: {updated_by_s4_count} рядків.", log_file_path)
        log_message(f"📄 Створено зведених рядків: {len(processed_rows) - 1}", log_file_path)
        print("✅ Повний процес обробки завершено. Деталі в лог-файлі.")

    except Exception as e:
        log_message(f"❌ Помилка при збереженні зведеної таблиці: {e}", log_file_path)
        print(f"❌ Помилка: {e}")


def prepare_for_website_upload():
    """
    Готує дані зі зведеної таблиці для завантаження на сайт,
    виконуючи кожен крок окремо з записом у файл.
    """
    base_dir = os.path.join(os.path.dirname(__file__), "..")
    log_file_path = os.path.join(base_dir, "logs", "logs.log")
    source_file_path = os.path.join(base_dir, "csv", "process", "zvedena.csv")
    target_file_path = os.path.join(base_dir, "csv", "process", "na_sait.csv")
    
    log_message("⚙️ Запускаю підготовку даних для сайту...", log_file_path)

    # Крок 1: Очищаємо табличку na_sait.csv
    try:
        log_message("⚙️ Крок 1: Очищаю файл 'na_sait.csv'...", log_file_path)
        with open(target_file_path, 'w', newline='', encoding='utf-8') as f:
            pass
        log_message("✅ Файл 'na_sait.csv' успішно очищено.", log_file_path)
    except Exception as e:
        log_message(f"❌ Помилка при очищенні файлу {os.path.basename(target_file_path)}: {e}", log_file_path)
        return

    # Крок 2: Копіюємо колонки 1, 23-30 із zvedena.csv
    try:
        log_message("⚙️ Крок 2: Копіюю дані зі 'zvedena.csv'...", log_file_path)
        with open(source_file_path, 'r', newline='', encoding='utf-8') as infile, \
             open(target_file_path, 'w', newline='', encoding='utf-8') as outfile:
            
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            try:
                header = next(reader)
                columns_to_copy = [1] + list(range(23, min(31, len(header))))
                new_header = [header[i] for i in columns_to_copy]
                writer.writerow(new_header)
            except StopIteration:
                log_message("❌ Помилка: Вхідний файл порожній.", log_file_path)
                return
            
            copied_count = 0
            for i, row in enumerate(reader):
                selected_columns = [row[1]] if len(row) > 1 else [""]
                
                for j in range(23, 31):
                    if j < len(row):
                        selected_columns.append(row[j])
                    else:
                        selected_columns.append("")
                
                if len(selected_columns) > 1:
                    writer.writerow(selected_columns)
                    copied_count += 1
        
        log_message(f"✅ Крок 2 завершено. Скопійовано {copied_count} рядків.", log_file_path)
    except FileNotFoundError:
        log_message(f"❌ Помилка: Вхідний файл {os.path.basename(source_file_path)} не знайдено.", log_file_path)
        return
    except Exception as e:
        log_message(f"❌ Виникла помилка під час копіювання: {e}", log_file_path)
        return

    # Крок 3: Додаємо 4 нові колонки з назвами
    try:
        log_message("⚙️ Крок 3: Додаю 4 нові колонки...", log_file_path)
        with open(target_file_path, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            rows = list(reader)
        
        new_header = header + ["sale_price", "sale_price_dates_from", "sale_price_dates_to", "Знижка%"]
        
        with open(target_file_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(new_header)
            for row in rows:
                row += [""] * 4
                writer.writerow(row)
        
        log_message(f"✅ Крок 3 завершено. Додано 4 нові колонки. Рядків у файлі: {len(rows)}", log_file_path)
    except Exception as e:
        log_message(f"❌ Виникла помилка під час додавання колонок: {e}", log_file_path)
        return

    # Крок 4: Видаляємо всі рядки, де в колонці з індексом 3 стоїть "0"
    try:
        log_message("⚙️ Крок 4: Видаляю рядки з нульовими значеннями...", log_file_path)
        with open(target_file_path, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            rows = list(reader)

        original_count = len(rows)
        filtered_rows = [row for row in rows if row[3] != "0"]
        deleted_count = original_count - len(filtered_rows)

        with open(target_file_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)
            writer.writerows(filtered_rows)
        
        log_message(f"✅ Крок 4 завершено. Видалено {deleted_count} рядків. Залишилось {len(filtered_rows)}.", log_file_path)
    except Exception as e:
        log_message(f"❌ Виникла помилка під час видалення рядків: {e}", log_file_path)
        return

    # Крок 5: Заповнюємо колонку з індексом 12 рандомними значеннями
    try:
        log_message("⚙️ Крок 5: Заповнюю колонку 12 рандомними значеннями...", log_file_path)
        with open(target_file_path, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            rows = list(reader)
        
        random_choices = [0, 2, 3, 5]
        weights = [94, 3, 2, 1]
        
        updated_count = 0
        for row in rows:
            try:
                if len(row) > 2 and float(row[1]) > 0 and float(row[2].replace(',', '.')) > 800:
                    random_value = random.choices(random_choices, weights=weights, k=1)[0]
                    row[12] = str(random_value)
                    if random_value > 0:
                        updated_count += 1
            except (ValueError, IndexError):
                continue
        
        with open(target_file_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)
            writer.writerows(rows)
            
        log_message(f"✅ Крок 5 завершено. Успішно заповнено {updated_count} рядків.", log_file_path)
    except Exception as e:
        log_message(f"❌ Виникла помилка під час заповнення: {e}", log_file_path)
        return

    # Крок 6: Заповнюємо колонку з індексом 9 за формулою
    try:
        log_message("⚙️ Крок 6: Заповнюю колонку 9 за формулою...", log_file_path)
        with open(target_file_path, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            rows = list(reader)

        updated_count = 0
        for row in rows:
            try:
                c_val = float(row[2].replace(',', '.') if row[2] else 0)
                m_val = float(row[12]) if row[12] else 0
                
                if m_val > 0:
                    result = round(c_val * (100 - m_val) / 100, 0)
                    row[9] = str(int(result))
                    updated_count += 1
                else:
                    row[9] = ""
            except (ValueError, IndexError):
                row[9] = ""
                continue
        
        with open(target_file_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)
            writer.writerows(rows)
            
        log_message(f"✅ Крок 6 завершено. Заповнено {updated_count} рядків.", log_file_path)
    except Exception as e:
        log_message(f"❌ Виникла помилка під час заповнення колонки 9: {e}", log_file_path)
        return

    # Крок 7: Видаляємо рядки, де колонка 9 пуста, а 4 та 5 дорівнюють "0"
    try:
        log_message("⚙️ Крок 7: Видаляю рядки, де колонка 9 пуста, а 4 і 5 дорівнюють '0'...", log_file_path)
        with open(target_file_path, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            rows = list(reader)
        
        original_count = len(rows)
        
        filtered_rows = []
        for row in rows:
            if not (row[9] == "" and row[4] == "0" and row[5] == "0"):
                filtered_rows.append(row)
        
        deleted_count = original_count - len(filtered_rows)

        with open(target_file_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)
            writer.writerows(filtered_rows)

        log_message(f"✅ Крок 7 завершено. Видалено {deleted_count} рядків. Залишилось {len(filtered_rows)}.", log_file_path)
    except Exception as e:
        log_message(f"❌ Виникла помилка під час видалення рядків: {e}", log_file_path)
        return

    # Крок 8: Додаємо дати в колонки 10 та 11 (колишній Крок 7)
    try:
        log_message("⚙️ Крок 8: Додаю дати в колонки 10 та 11...", log_file_path)
        with open(target_file_path, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            rows = list(reader)

        today = datetime.now()
        seven_days_later = today + timedelta(days=7)
        
        today_formatted = today.strftime("%Y-%m-%d 00:00:00")
        seven_days_later_formatted = seven_days_later.strftime("%Y-%m-%d 00:00:00")
        
        updated_count = 0
        for row in rows:
            try:
                if len(row) > 12 and row[12] and float(row[12]) > 0:
                    if len(row) > 10:
                        row[10] = today_formatted
                    if len(row) > 11:
                        row[11] = seven_days_later_formatted
                    updated_count += 1
            except (ValueError, IndexError):
                continue

        with open(target_file_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)
            writer.writerows(rows)
        
        log_message(f"✅ Крок 8 завершено. Дати додано до {updated_count} рядків.", log_file_path)
    except Exception as e:
        log_message(f"❌ Виникла помилка під час додавання дат: {e}", log_file_path)
        return
        
    # Крок 9: Копіюємо дані в файл zalishky_akcii.csv
    try:
        log_message("⚙️ Крок 9: Готую файл 'zalishky_akcii.csv'...", log_file_path)
        
        source_copy_file_path = os.path.join(base_dir, "csv", "process", "na_sait.csv")
        target_copy_file_path = "/var/www/scripts/update/csv/output/zalishky_akcii.csv"
        
        # 9.1 Очищаємо файл
        with open(target_copy_file_path, 'w', newline='', encoding='utf-8') as f:
            pass

        # 9.2 Копіюємо дані
        with open(source_copy_file_path, 'r', newline='', encoding='utf-8') as infile, \
             open(target_copy_file_path, 'w', newline='', encoding='utf-8') as outfile:

            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Читаємо заголовок і визначаємо індекси колонок для копіювання
            try:
                header = next(reader)
                columns_to_copy = [0, 1, 2, 9, 10, 11]
                new_header = [header[i] for i in columns_to_copy if i < len(header)]
                writer.writerow(new_header)
            except StopIteration:
                log_message("❌ Помилка: Вхідний файл 'na_sait.csv' порожній.", log_file_path)
                return

            copied_count = 0
            for row in reader:
                selected_columns = [row[i] for i in columns_to_copy if i < len(row)]
                writer.writerow(selected_columns)
                copied_count += 1
        
        log_message(f"✅ Крок 9 завершено. Скопійовано {copied_count} рядків в 'zalishky_akcii.csv'.", log_file_path)
    except FileNotFoundError:
        log_message(f"❌ Помилка: Вхідний або вихідний файл не знайдено: {e}", log_file_path)
        return
    except Exception as e:
        log_message(f"❌ Виникла помилка під час копіювання в 'zalishky_akcii.csv': {e}", log_file_path)
        return

    log_message("🎉 Підготовка даних для сайту завершена!", log_file_path)
    print("✅ Підготовка даних для сайту завершена.")