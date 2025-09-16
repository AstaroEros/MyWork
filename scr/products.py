import csv
import os
import time
import requests
import shutil
import re
import pandas as pd
import random 
import logging
from scr.base_function import get_wc_api, load_settings, setup_new_log_file, log_message_to_existing_file
from datetime import datetime, timedelta


def export_products():
    """
    Експорт усіх товарів у CSV пачками по 100, використовуючи поля з налаштувань.
    """
    setup_new_log_file()

    settings = load_settings()
    if not settings or "paths" not in settings or "csv_path_zalishki" not in settings["paths"] or "export_fields" not in settings:
        logging.error("❌ Не знайдено необхідні налаштування (шлях до CSV або поля експорту) в settings.json. Експорт перервано.")
        return

    csv_path = os.path.join(os.path.dirname(__file__), "..", settings["paths"]["csv_path_zalishki"])

    # Створення списку полів для запиту до API та заголовків для CSV
    api_fields = []
    csv_headers = []
    meta_fields_for_api = []
    
    # Розділяємо поля на стандартні і метадані
    for field in settings["export_fields"]:
        if isinstance(field, str):
            api_fields.append(field)
            csv_headers.append(field)
        elif isinstance(field, dict) and "meta_data" in field:
            meta_fields_for_api = field["meta_data"]
            api_fields.append("meta_data")
            # Додаємо метадані до заголовків CSV з префіксом "Мета:"
            for meta_field in meta_fields_for_api:
                csv_headers.append(f"Мета: {meta_field}")

    wcapi = get_wc_api(settings)
    if not wcapi:
        logging.error("❌ Не вдалося створити об'єкт WooCommerce API. Експорт перервано.")
        return

    start_time = time.time()
    total_products = 0
    exported_count = 0
    errors = []

    logging.info("🚀 Початок експорту товарів.")

    try:
        response = wcapi.get("products", params={"per_page": 1})
        if response.status_code != 200:
            error_msg = f"Помилка {response.status_code} при підрахунку товарів: {response.text}"
            print(f"❌ {error_msg}")
            logging.error(f"❌ {error_msg}")
            errors.append(error_msg)
            return

        total_products = int(response.headers.get("X-WP-Total", 0))
        logging.info(f"🔎 Загальна кількість товарів: {total_products}")

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(csv_headers)

            page = 1
            while exported_count < total_products:
                response = wcapi.get(
                    "products",
                    params={
                        "per_page": 100,
                        "page": page,
                        "_fields": ",".join(api_fields)
                    }
                )

                if response.status_code != 200:
                    error_msg = f"Помилка {response.status_code} на сторінці {page}: {response.text}"
                    print(f"❌ {error_msg}")
                    logging.error(f"❌ {error_msg}")
                    errors.append(error_msg)
                    break

                products = response.json()
                if not products:
                    break
                
                for product in products:
                    row = []
                    # Заповнення рядка стандартними полями
                    for field in settings["export_fields"]:
                        if isinstance(field, str):
                            if field == "status":
                                row.append("yes" if product.get(field) == "publish" else "no")
                            elif field == "categories":
                                row.append(", ".join([cat["name"] for cat in product.get("categories", [])]))
                            else:
                                row.append(product.get(field, ""))
                        # Заповнення метаданими
                        elif isinstance(field, dict) and "meta_data" in field:
                            meta_data_dict = {m["key"]: m["value"] for m in product.get("meta_data", [])}
                            for meta_field in meta_fields_for_api:
                                row.append(meta_data_dict.get(meta_field, ""))
                    
                    writer.writerow(row)
                    exported_count += 1
                
                if exported_count % 100 == 0 or exported_count == total_products:
                    elapsed = int(time.time() - start_time)
                    status_message = f"✅ Вивантажено {exported_count} з {total_products} ({elapsed} сек)"
                    print(status_message)
                    logging.info(status_message)

                page += 1
                time.sleep(1)

    except Exception as e:
        error_msg = f"Виникла невідома помилка під час експорту: {e}"
        print(f"❌ {error_msg}")
        logging.error(f"❌ {error_msg}", exc_info=True)
        errors.append(error_msg)
    finally:
        end_time = time.time()
        elapsed_time = int(end_time - start_time)
        
        print(f"🎉 Експорт завершено. Вивантажено {exported_count} з {total_products} товарів за {elapsed_time} сек.")
        if errors:
            print(f"⚠️ Експорт завершився з {len(errors)} помилками. Деталі в лог-файлі.")
        
        logging.info("--- Підсумок експорту ---")
        logging.info(f"Статус: {'Успішно' if not errors else 'Завершено з помилками'}")
        logging.info(f"Кількість товарів: {exported_count} з {total_products}")
        logging.info(f"Тривалість: {elapsed_time} сек.")
        if errors:
            logging.info(f"Кількість помилок: {len(errors)}")
            logging.info("Перелік помилок:")
            for err in errors:
                logging.info(f"- {err}")

def download_supplier_price_list(supplier_id):
    """
    Скачує прайс-лист від постачальника за його ID.
    """
    # 0. Налаштування логування для дописування
    log_message_to_existing_file()

    # 1. Завантаження налаштувань
    settings = load_settings()
    if not settings:
        logging.error("❌ Не вдалося завантажити налаштування. Скачування прайс-листа перервано.")
        return
    
    # 2. Отримання інформації про постачальника
    supplier_info = settings.get("suppliers", {}).get(str(supplier_id))
    if not supplier_info:
        logging.error(f"❌ Помилка: Інформацію про постачальника з ID '{supplier_id}' не знайдено.")
        return

    # 3. Визначення шляхів
    base_dir = os.path.join(os.path.dirname(__file__), "..")
    url = supplier_info.get("download_url")
    csv_path = os.path.join(base_dir, supplier_info.get("csv_path"))

    if not url or not csv_path:
        logging.error(f"❌ Неповні дані про постачальника '{supplier_id}'. Відсутній URL або шлях.")
        return
    
    logging.info(f"⏳ Запускаю завантаження прайс-листа від постачальника (ID: {supplier_id}).")

    # 4. Видалення старого файлу
    if os.path.exists(csv_path):
        try:
            os.remove(csv_path)
            # Оновлена логіка: використовуємо ID постачальника замість назви файлу
            logging.info(f"✅ Старий прайс-лист від постачальника (ID: {supplier_id}) успішно видалено.")
        except OSError as e:
            logging.error(f"❌ Помилка при видаленні старого файлу: {e}")
            return
    
    # 5. Завантаження нового файлу
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(csv_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        
        logging.info(f"🎉 Прайс-лист від постачальника (ID: {supplier_id}) успішно завантажено.")
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Помилка завантаження файлу від постачальника (ID: {supplier_id}): {e}", exc_info=True)
    except Exception as e:
        logging.error(f"❌ Виникла невідома помилка під час завантаження: {e}", exc_info=True)

def process_supplier_1_price_list():
    """
    Обробляє та очищає прайс-лист від постачальника 1.
    """
    # 0. Налаштування логування для дописування
    log_message_to_existing_file()

    # 1. Завантаження налаштувань
    settings = load_settings()
    if not settings:
        logging.error("❌ Не вдалося завантажити налаштування. Обробка прайс-листа перервана.")
        return

    supplier_id = "1"
    supplier_info = settings.get("suppliers", {}).get(supplier_id)
    if not supplier_info:
        logging.error(f"❌ Помилка: Інформацію про постачальника з ID '{supplier_id}' не знайдено.")
        return

    # 2. Визначення шляхів та параметрів
    base_dir = os.path.join(os.path.dirname(__file__), "..")
    csv_path = os.path.join(base_dir, supplier_info.get("csv_path"))
    delimiter = supplier_info.get("delimiter", ",")

    if not os.path.exists(csv_path):
        logging.error(f"❌ Файл прайс-листа для постачальника {supplier_id} не знайдено")
        return

    logging.info(f"⚙️ Запускаю обробку прайс-листа для постачальника {supplier_id}.")

    # 3. Фільтрація та обробка даних
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
                        logging.warning(f"🚫 Видалено рядок {row_number} через наявність літер у колонці 4 (ціна): '{price_value}'.")
                        skipped_rows += 1
                        continue

                # Фільтрація за колонкою 3 (назва)
                if len(row) > 2:
                    product_name = row[2].lower()
                    if any(word in product_name for word in words_to_filter_from_name):
                        logging.warning(f"🚫 Видалено рядок {row_number} через заборонене слово в назві ('{row[2]}').")
                        skipped_rows += 1
                        continue
                
                # Фільтрація за колонкою 8 (бренд)
                if len(row) > 7:
                    brand_name = row[7].lower()
                    if any(word in brand_name for word in words_to_filter_from_brand):
                        logging.warning(f"🚫 Видалено рядок {row_number} через заборонене слово в бренді ('{row[7]}').")
                        skipped_rows += 1
                        continue

                # Перетворення колонки 4 (ціна) з float на int
                if len(row) > 3 and row[3]:
                    try:
                        row[3] = str(int(float(row[3])))
                    except (ValueError, IndexError):
                        logging.warning(f"⚠️ Помилка перетворення ціни в рядку {row_number}. Значення: '{row[3]}'")
                
                # Заміна значення в колонці 7 (категорія)
                if len(row) > 6 and row[6] == ">3":
                    row[6] = "4"
                
                processed_rows.append(row)
    
    except Exception as e:
        logging.error(f"❌ Виникла помилка під час обробки файлу: {e}", exc_info=True)
        return

    # 4. Запис обробленого файлу
    with open(temp_file_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile, delimiter=delimiter)
        writer.writerows(processed_rows)

    os.replace(temp_file_path, csv_path)

    # 5. Логування підсумків
    logging.info(f"🎉 Обробку прайс-листа для постачальника {supplier_id} завершено.")
    logging.info(f"--- Підсумок обробки: ---")
    logging.info(f"📦 Всього рядків у файлі: {total_rows}")
    logging.info(f"🗑️ Видалено рядків: {skipped_rows}")
    logging.info(f"✅ Оброблені рядки: {len(processed_rows) - 1}")

def process_supplier_2_price_list():
    """
    Обробляє та очищає прайс-лист від постачальника 2.
    """
    # 0. Налаштування логування для дописування
    log_message_to_existing_file()

    # 1. Завантаження налаштувань
    settings = load_settings()
    if not settings:
        logging.error("❌ Не вдалося завантажити налаштування. Обробка прайс-листа перервана.")
        return

    supplier_id = "2"
    supplier_info = settings.get("suppliers", {}).get(supplier_id)
    if not supplier_info:
        logging.error(f"❌ Помилка: Інформацію про постачальника з ID '{supplier_id}' не знайдено.")
        return

    # 2. Визначення шляхів та параметрів
    base_dir = os.path.join(os.path.dirname(__file__), "..")
    csv_path = os.path.join(base_dir, supplier_info.get("csv_path"))
    delimiter = supplier_info.get("delimiter", ",")

    if not os.path.exists(csv_path):
        logging.error(f"❌ Файл прайс-листа для постачальника {supplier_id} не знайдено за шляхом: {csv_path}")
        return

    logging.info(f"⚙️ Запускаю обробку прайс-листа для постачальника {supplier_id}.")

    # 3. Обробка даних
    temp_file_path = f"{csv_path}.temp"
    processed_rows = []
    skipped_rows = 0
    total_rows = 0
    modifications_count = 0

    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile, delimiter=delimiter)
            try:
                headers = next(reader)
                processed_rows.append(headers)
            except StopIteration:
                logging.warning("⚠️ Файл порожній. Відсутні рядки.")
                return

            row_number = 1
            for row in reader:
                row_number += 1
                total_rows += 1

                # Перевірка колонки 5 (валюта)
                if len(row) > 4:
                    currency_value = row[4].strip().upper()
                    if currency_value != "UAH":
                        logging.warning(f"🚫 Видалено рядок {row_number} через некоректну валюту у колонці 5: '{row[4]}'.")
                        skipped_rows += 1
                        continue
                else:
                    logging.warning(f"🚫 Видалено рядок {row_number} через відсутність значення у колонці 5 (валюта).")
                    skipped_rows += 1
                    continue

                # Перетворення колонки 4 (ціна) з float на int
                if len(row) > 3 and row[3]:
                    try:
                        row[3] = str(int(float(row[3])))
                    except (ValueError, IndexError):
                        logging.warning(f"⚠️ Помилка перетворення ціни в рядку {row_number}. Значення: '{row[3]}'")
                
                # Заміна значення в колонці 7 (категорія)
                if len(row) > 6 and row[6] == ">3":
                    row[6] = "4"
                    modifications_count += 1
                
                processed_rows.append(row)
    
    except Exception as e:
        logging.error(f"❌ Виникла помилка під час обробки файлу: {e}", exc_info=True)
        return

    # 4. Запис обробленого файлу
    with open(temp_file_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile, delimiter=delimiter)
        writer.writerows(processed_rows)

    os.replace(temp_file_path, csv_path)

    # 5. Логування підсумків
    logging.info(f"🎉 Обробку прайс-листа для постачальника {supplier_id} завершено.")
    logging.info("--- Підсумок обробки: ---")
    logging.info(f"📦 Всього рядків у файлі: {total_rows}")
    logging.info(f"🗑️ Видалено рядків: {skipped_rows}")
    logging.info(f"📝 Змінено категорій '>3' -> '4': {modifications_count} разів")
    logging.info(f"✅ Оброблені рядки: {len(processed_rows) - 1}")
    print("✅ Обробка прайс-листа завершена. Деталі в лог-файлі.")

def process_supplier_3_price_list():
    """
    Обробляє та конвертує прайс-лист від постачальника 3 (формат .xls),
    а потім фільтрує дані.
    """
    # 0. Налаштування логування для дописування
    log_message_to_existing_file()

    # 1. Завантаження налаштувань
    settings = load_settings()
    if not settings:
        logging.error("❌ Не вдалося завантажити налаштування. Обробка прайс-листа перервана.")
        return

    supplier_id = "3"
    supplier_info = settings.get("suppliers", {}).get(supplier_id)
    if not supplier_info:
        logging.error(f"❌ Помилка: Інформацію про постачальника з ID '{supplier_id}' не знайдено.")
        return

    # 2. Визначення шляхів
    base_dir = os.path.join(os.path.dirname(__file__), "..")
    xls_path = os.path.join(base_dir, supplier_info.get("csv_path"))
    csv_name = os.path.join(base_dir, supplier_info.get("csv_name"))

    logging.info(f"⚙️ Запускаю обробку прайс-листа для постачальника {supplier_id}.")

    # 3. Видалення старого CSV-файлу
    if os.path.exists(csv_name):
        try:
            os.remove(csv_name)
            logging.info(f"✅ Старий прайс-лист для постачальника {supplier_id} успішно видалено.")
        except OSError as e:
            logging.error(f"❌ Помилка при видаленні старого CSV-файлу: {e}")
            return

    # 4. Конвертація XLS в CSV
    if not os.path.exists(xls_path):
        logging.error(f"❌ Файл .xls для постачальника {supplier_id} не знайдено за шляхом: {xls_path}")
        return

    try:
        df = pd.read_excel(xls_path)
        df.to_csv(csv_name, index=False, encoding="utf-8")
        
        logging.info(f"🎉 Файл .xls для постачальника {supplier_id} успішно конвертовано в CSV.")

    except Exception as e:
        logging.error(f"❌ Помилка під час конвертації файлу: {e}", exc_info=True)
        return

    # 5. Фільтрація та очищення CSV-файлу
    logging.info(f"🔍 Запускаю фільтрацію даних у CSV-файлі постачальника {supplier_id}.")

    temp_file_path = f"{csv_name}.temp"
    processed_rows = []
    skipped_rows = 0
    total_rows = 0

    try:
        with open(csv_name, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile)
            try:
                headers = next(reader)
                processed_rows.append(headers)
            except StopIteration:
                logging.warning("⚠️ Файл порожній. Відсутні рядки.")
                return
            
            row_number = 1
            for row in reader:
                row_number += 1
                total_rows += 1

                # Перевірка, що рядок містить достатньо колонок
                if len(row) < 4:
                    logging.warning(f"🚫 Видалено рядок {row_number} через недостатню кількість колонок.")
                    skipped_rows += 1
                    continue

                # Перевірка колонок 3 та 4 на ціле число >= 0
                is_valid = True
                for col_index in [2, 3]:
                    value = row[col_index]
                    try:
                        int_value = int(float(value))
                        if int_value < 0:
                            logging.warning(f"🚫 Видалено рядок {row_number} через від'ємне значення в колонці {col_index + 1}: '{value}'.")
                            is_valid = False
                            break
                    except (ValueError, IndexError):
                        logging.warning(f"🚫 Видалено рядок {row_number} через некоректне числове значення в колонці {col_index + 1}: '{value}'.")
                        is_valid = False
                        break

                if is_valid:
                    processed_rows.append(row)
                else:
                    skipped_rows += 1
    
    except Exception as e:
        logging.error(f"❌ Виникла помилка під час фільтрації файлу: {e}", exc_info=True)
        return

    # 6. Запис відфільтрованих даних у файл
    with open(temp_file_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerows(processed_rows)

    os.replace(temp_file_path, csv_name)

    # 7. Логування підсумків
    logging.info(f"🎉 Обробку та фільтрацію прайс-листа для постачальника {supplier_id} завершено.")
    logging.info("--- Підсумок обробки: ---")
    logging.info(f"📦 Всього рядків у файлі: {total_rows}")
    logging.info(f"🗑️ Видалено рядків: {skipped_rows}")
    logging.info(f"✅ Оброблені рядки: {len(processed_rows) - 1}")
    print("✅ Обробка прайс-листа завершена. Деталі в лог-файлі.")


def process_and_combine_all_data():
    """
    Обробляє прайс-листи та об'єднує дані у зведену таблицю csv/process/zvedena.csv.
    """
    
    # 0. Початкове налаштування та логування
    log_message_to_existing_file()
    logging.info("--- ⚙️ Запускаю повний процес обробки та об'єднання даних... ---")

    # 1. Завантаження налаштувань та визначення шляхів
    # Цей блок завантажує файл налаштувань settings.json і визначає всі необхідні шляхи до файлів.
    settings = load_settings()
    if not settings:
        logging.error("❌ Не вдалося завантажити налаштування. Процес перервано.")
        return

    base_dir = os.path.join(os.path.dirname(__file__), "..")
    
    # Використовуємо метод .get() з вкладеними ключами для безпечного доступу
    paths = settings.get("paths", {})
    suppliers = settings.get("suppliers", {})
    
    log_file_path = os.path.join(base_dir, paths.get("main_log_file"))
    zalishki_path = os.path.join(base_dir, paths.get("csv_path_zalishki"))
    zvedena_path = os.path.join(base_dir, paths.get("csv_path_zvedena"))
    
    # Інформація про постачальників
    supplier_info_1 = suppliers.get("1", {})
    supplier_csv_path_1 = os.path.join(base_dir, supplier_info_1.get("csv_path"))
    supplier_delimiter_1 = supplier_info_1.get("delimiter", ",")
    
    supplier_info_2 = suppliers.get("2", {})
    supplier_csv_path_2 = os.path.join(base_dir, supplier_info_2.get("csv_path"))
    supplier_delimiter_2 = supplier_info_2.get("delimiter", ",")

    supplier_info_3 = suppliers.get("3", {})
    supplier_csv_path_3 = os.path.join(base_dir, supplier_info_3.get("csv_name"))
    supplier_delimiter_3 = supplier_info_3.get("delimiter", ",")
    
    supplier_info_4 = suppliers.get("4", {})
    supplier_csv_path_4 = os.path.join(base_dir, supplier_info_4.get("csv_path"))
    supplier_delimiter_4 = supplier_info_4.get("delimiter", ",")
    
    # Додаткові налаштування
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
    
    # 2. Перевірка наявності всіх необхідних файлів
    # Цей блок перевіряє, чи існують усі файли, перш ніж почати обробку.
    required_files = {
        "файл залишків": zalishki_path,
        "прайс-лист постачальника 1": supplier_csv_path_1,
        "прайс-лист постачальника 2": supplier_csv_path_2,
        "прайс-лист постачальника 3": supplier_csv_path_3,
        "прайс-лист постачальника 4": supplier_csv_path_4,
    }

    for file_name, path in required_files.items():
        if not path or not os.path.exists(path):
            logging.error(f"❌ Не знайдено {file_name} за шляхом: {path}. Процес зупинено.")
            print(f"❌ Не знайдено {file_name}.")
            return
    
    # 3. Видалення старого зведеного файлу (якщо існує)
    if os.path.exists(zvedena_path):
        try:
            os.remove(zvedena_path)
            logging.info(f"✅ Старий файл {os.path.basename(zvedena_path)} успішно видалено.")
        except OSError as e:
            logging.error(f"❌ Помилка при видаленні старого файлу: {e}", exc_info=True)
            print(f"❌ Помилка: {e}")
            return
    
    # 4. Завантаження даних постачальників у словники для швидкого доступу
    # Кожен файл постачальника зчитується в пам'ять, щоб прискорити процес обробки.
    supplier_data_dict = {}
    supplier_list = [
        ("1", supplier_csv_path_1, supplier_delimiter_1, supplier_1_columns, supplier_1_match_column),
        ("2", supplier_csv_path_2, supplier_delimiter_2, supplier_2_columns, supplier_2_match_column),
        ("3", supplier_csv_path_3, supplier_delimiter_3, supplier_3_columns, supplier_3_match_column),
        ("4", supplier_csv_path_4, supplier_delimiter_4, supplier_4_columns, supplier_4_match_column)
    ]
    
    for s_id, path, delimiter, columns, match_col in supplier_list:
        try:
            supplier_data_dict[s_id] = {}
            with open(path, "r", newline="", encoding="utf-8") as infile:
                reader = csv.reader(infile, delimiter=delimiter)
                try:
                    next(reader) 
                except StopIteration:
                    logging.warning(f"⚠️ Прайс-лист постачальника {s_id} порожній. Пропускаю завантаження.")
                    continue
                for row in reader:
                    if len(row) > max(int(c) for c in columns):
                        key = row[int(match_col)].strip()
                        values = [row[int(c)].strip() for c in columns]
                        supplier_data_dict[s_id][key] = values
            logging.info(f"✅ Дані постачальника {s_id} успішно завантажено в пам'ять. Знайдено {len(supplier_data_dict[s_id])} унікальних записів.")
        except Exception as e:
            logging.error(f"❌ Помилка при читанні прайс-листа постачальника {s_id}: {e}", exc_info=True)
            print(f"❌ Помилка при читанні прайс-листа постачальника {s_id}: {e}")
            return

    # 5. Обробка основного файлу залишків та об'єднання даних
    # Цей блок читає основний файл, додає до кожного рядка дані від постачальників
    # та обчислює нові колонки, ігноруючи порожні або пошкоджені рядки.
    processed_rows = []
    processed_count = 0
    updated_by_s1_count = 0
    updated_by_s2_count = 0
    updated_by_s3_count = 0
    updated_by_s4_count = 0
    
    formula_cols = {
        'N': 13, 'Q': 16, 'S': 18, 'V': 21,
        'M': 12, 'P': 15, 'T': 19, 'W': 22,
        'H': 7,
        'I': 8,
        'X': 23,
        'G': 6,
        'Y': 24
    }
    
    suppliers_to_process = [
        ("1", zvedena_match_column_1, updated_by_s1_count),
        ("2", zvedena_match_column_2, updated_by_s2_count),
        ("3", zvedena_match_column_3, updated_by_s3_count),
        ("4", zvedena_match_column_4, updated_by_s4_count)
    ]

    try:
        with open(zalishki_path, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile)
            try:
                next(reader)
            except StopIteration:
                logging.warning("⚠️ Файл залишків порожній. Зведена таблиця буде створена лише із заголовком.")
                
            processed_rows.append(new_header)
            
            for row in reader:
                if not row or not any(row):
                    continue
                
                processed_count += 1
                if len(row) > 13:
                    new_row = [row[int(col_index)] for col_index in zalishki_columns]
                    
                    for s_id, match_col, counter in suppliers_to_process:
                        supplier_data = supplier_data_dict.get(s_id, {}).get(new_row[int(match_col)].strip(), ["", "", ""])
                        new_row.extend(supplier_data)
                        if supplier_data and supplier_data[0] != "":
                            if s_id == "1": updated_by_s1_count += 1
                            elif s_id == "2": updated_by_s2_count += 1
                            elif s_id == "3": updated_by_s3_count += 1
                            elif s_id == "4": updated_by_s4_count += 1

                    # Обчислення для колонки 23
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

                    # Обчислення для колонки 24
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
                        try:
                            result_24 = new_row[formula_cols['H']]
                        except IndexError:
                            result_24 = "0"
                    else:
                        result_24 = min(valid_quantities_for_min) if valid_quantities_for_min else 0
                    
                    new_row.append(str(result_24))

                    # Обчислення для колонки 25
                    try:
                        i_val = new_row[formula_cols['I']].strip().lower()
                    except IndexError:
                        i_val = ""
                    
                    result_25 = 1 if i_val == "yes" else 0
                    new_row.append(str(result_25))
                    
                    # Обчислення для колонки 26
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
                    logging.debug(f"рядок {processed_count}: X = \"{x_val}\", G = \"{g_val}\". (X - G) = \"{x_val - g_val}\". Результат для колонки 26 = \"{result_26}\"")

                    # Обчислення для колонки 27
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
                    logging.debug(f"рядок {processed_count}: Y = \"{y_val}\", H = \"{h_val}\". (Y - H) = \"{y_val - h_val}\". Результат для колонки 27 = \"{result_27}\"")
                    
                    processed_rows.append(new_row)
                else:
                    logging.warning(f"⚠️ Пропущено рядок {processed_count} через недостатню кількість колонок.")
    
    except Exception as e:
        logging.error(f"❌ Виникла помилка під час обробки даних: {e}", exc_info=True)
        print(f"❌ Виникла помилка під час обробки даних: {e}")
        return

    # 6. Сортування та збереження даних у зведений файл CSV
    # Цей блок сортує оброблені дані та записує їх у новий файл,
    # перевіряючи, чи є дані для запису.
    try:
        if len(processed_rows) <= 1:
            logging.warning("⚠️ Немає даних для запису, крім заголовка.")
            print("⚠️ Немає даних для запису, крім заголовка. Файл не буде створено.")
            return

        # Сортування за колонкою B (індекс 1)
        # Спочатку видаляємо заголовок, сортуємо, а потім додаємо його назад.
        header = processed_rows[0]
        data_rows = processed_rows[1:]
        data_rows.sort(key=lambda row: int(row[1]))
        sorted_rows = [header] + data_rows

        with open(zvedena_path, "w", newline="", encoding="utf-8") as outfile:
            writer = csv.writer(outfile, delimiter=',')
            writer.writerows(sorted_rows)

        logging.info("--- 🎉 Повний процес обробки та об'єднання завершено! ---")
        logging.info("--- Підсумок: ---")
        logging.info(f"📦 Всього рядків у файлі залишків: {processed_count}")
        logging.info(f"✅ Оновлено даними постачальника 1: {updated_by_s1_count} рядків.")
        logging.info(f"✅ Оновлено даними постачальника 2: {updated_by_s2_count} рядків.")
        logging.info(f"✅ Оновлено даними постачальника 3: {updated_by_s3_count} рядків.")
        logging.info(f"✅ Оновлено даними постачальника 4: {updated_by_s4_count} рядків.")
        logging.info(f"📄 Створено зведених рядків: {len(processed_rows) - 1}")
        print("✅ Повний процес обробки завершено. Деталі в лог-файлі.")

    except Exception as e:
        logging.error(f"❌ Помилка при збереженні зведеної таблиці: {e}", exc_info=True)
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


def update_products():
    """
    Оновлює дані про товар на сайті, використовуючи API.
    Дані беруться з файлу zalishky_akcii.csv.
    """
    log_file_path = update_log()
    
    settings = load_settings()
    if not settings:
        log_message("❌ Неможливо завантажити налаштування. Перевірте файл.", log_file_path)
        print("❌ Неможливо завантажити налаштування. Перевірте файл.")
        return
        
    source_file_path = "/var/www/scripts/update/csv/output/zalishky_akcii.csv"
    
    url = settings.get("url")
    consumer_key = settings.get("consumer_key")
    consumer_secret = settings.get("consumer_secret")
    
    if not url or not consumer_key or not consumer_secret:
        error_msg = "URL або ключі (consumer_key, consumer_secret) відсутні в налаштуваннях."
        log_message(f"❌ {error_msg}", log_file_path)
        print(f"❌ {error_msg}")
        return

    api_url = f"{url}/wp-json/wc/v3/products/batch"

    start_time = time.time()
    total_items = 0
    updated_count = 0
    error_count = 0

    log_message("🚀 Початок оновлення товарів через API.", log_file_path)

    try:
        with open(source_file_path, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            data_to_update = list(reader)
            total_items = len(data_to_update)

            log_message(f"🔎 Знайдено {total_items} товарів для оновлення.", log_file_path)

            payloads = []
            for row in data_to_update:
                product_id = row.get('id')
                
                if not product_id:
                    log_message(f"⚠️ Пропущено товар: не знайдено ID.", log_file_path)
                    continue

                regular_price = row.get('regular_price')
                sale_price = row.get('sale_price')
                stock_quantity = row.get('stock')
                date_on_sale_from = row.get('date_on_sale_from')
                date_on_sale_to = row.get('date_on_sale_to')
                
                # Перетворюємо порожні рядки цін на None
                if not regular_price:
                    regular_price = None
                
                if not sale_price:
                    sale_price = None
                    # Якщо акційна ціна відсутня, також видаляємо дати акції
                    date_on_sale_from = None
                    date_on_sale_to = None
                
                log_message(f"🔍 Готуємо товар ID {product_id}. Ціна: {regular_price} -> {sale_price}. Залишок: {stock_quantity}. Дати: {date_on_sale_from} - {date_on_sale_to}.", log_file_path)

                payload = {
                    "id": product_id,
                    "regular_price": regular_price,
                    "sale_price": sale_price,
                    "stock_quantity": stock_quantity,
                    "date_on_sale_from": date_on_sale_from,
                    "date_on_sale_to": date_on_sale_to
                }
                payloads.append(payload)
            
            response = requests.post(api_url, json={"update": payloads}, auth=(consumer_key, consumer_secret))
            response.raise_for_status()

            result = response.json()
            if 'update' in result:
                updated_count = len(result['update'])
                error_count = len(result.get('errors', []))
                for error in result.get('errors', []):
                    error_msg = f"❌ Помилка оновлення: {error.get('message', 'Невідома помилка')}"
                    log_message(error_msg, log_file_path)
                    print(error_msg)

            status_message = f"✅ Оброблено {total_items} товарів."
            log_message(status_message, log_file_path)
            print(status_message)
            
    except FileNotFoundError:
        error_msg = f"❌ Помилка: Файл '{source_file_path}' не знайдено."
        print(error_msg)
        log_message(error_msg, log_file_path)
        error_count += total_items
    except requests.exceptions.RequestException as e:
        error_msg = f"❌ Помилка з'єднання або запиту: {e}"
        print(error_msg)
        log_message(error_msg, log_file_path)
        error_count += total_items
    except Exception as e:
        error_msg = f"❌ Виникла невідома помилка під час завантаження: {e}"
        print(error_msg)
        log_message(error_msg, log_file_path)
        error_count += total_items
    finally:
        end_time = time.time()
        elapsed_time = int(end_time - start_time)
        
        print(f"🎉 Оновлення завершено. Оновлено {updated_count} товарів за {elapsed_time} сек.")
        if error_count > 0:
            print(f"⚠️ Завершено з {error_count} помилками. Детальніше в лог-файлі.")
        
        log_message(f"--- Підсумок оновлення ---", log_file_path)
        log_message(f"Статус: {'Успішно' if error_count == 0 else 'Завершено з помилками'}", log_file_path)
        log_message(f"Кількість товарів: {updated_count} з {total_items}", log_file_path)
        log_message(f"Тривалість: {elapsed_time} сек.", log_file_path)
        if error_count > 0:
            log_message(f"Кількість помилок: {error_count}. Детальні помилки дивіться вище.", log_file_path)