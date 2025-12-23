import csv, pymysql, html, os, re, logging, requests, shutil
import pandas as pd
from scr.oc_base_function import oc_setup_new_log_file, oc_log_message, oc_connect_db, load_oc_settings

# ГОЛОВНА ФУНКЦІЯ 1: Експорт товарів з бд
def oc_export_products():
    """
    Основна функція експорту товарів:
    1. Налаштовує новий лог.
    2. Читає налаштування та пресети.
    3. Виконує SQL запит.
    4. Зберігає результат у CSV.
    """

    # 1. Створюємо новий лог (ініціалізація logging)
    oc_setup_new_log_file()
    
    start_msg = "▶ Старт експорту товарів OpenCart"
    logging.info(start_msg)
    print(start_msg)

    # 2. Завантажуємо налаштування
    settings = load_oc_settings()
    if not settings or "presets" not in settings:
        err_msg = "❌ Не знайдено пресети в oc_settings.yaml"
        logging.error(err_msg)
        print(err_msg)
        return

    presets = settings["presets"]
    # Отримуємо базовий шлях для CSV
    csv_base_path = settings["paths"]["output_file"]

    # 3. Вибір пресету
    print("\nВиберіть пресет для експорту:\n")
    for key, preset in presets.items():
        print(f" [{key}] - {preset['name']}")

    user_input = input("\nВаш вибір: ").strip()

    # --- ВИПРАВЛЕННЯ: РОЗУМНИЙ ПОШУК КЛЮЧА ---
    # Спочатку припускаємо, що ключа немає
    preset_id = None

    # 1. Перевіряємо, чи є такий ключ як рядок (на випадок ключів типу "all")
    if user_input in presets:
        preset_id = user_input
    else:
        # 2. Якщо ні, пробуємо перетворити введення на число (для ключів 1, 2...)
        try:
            user_input_int = int(user_input)
            if user_input_int in presets:
                preset_id = user_input_int
        except ValueError:
            pass # Це було не число, і такого рядка теж немає

    # Якщо після перевірок preset_id все ще None — це помилка
    if preset_id is None:
        err_msg = f"❌ Невідомий номер пресету: {user_input}"
        logging.warning(err_msg)
        print(err_msg)
        return

    sql = presets[preset_id]["sql"]
    preset_name = presets[preset_id]["name"]

    info_msg = f"▶ Обраний пресет [{preset_id}]: {preset_name}"
    logging.info(info_msg)
    print(info_msg)

    # 4. Підключення до бази та виконання запиту
    conn = oc_connect_db()
    if not conn:
        logging.error("❌ Неможливо підключитися до БД (conn is None)")
        return

    try:
        with conn.cursor() as cursor:
            logging.info("⏳ Виконується SQL запит...")
            cursor.execute(sql)
            rows = cursor.fetchall()
    except pymysql.MySQLError as e:
        err_sql = f"❌ Помилка виконання SQL: {e}"
        logging.error(err_sql)
        print(err_sql)
        return
    finally:
        # Завжди закриваємо з'єднання
        conn.close()
        logging.info("🔌 З'єднання з БД закрито.")

    # 5. Запис CSV
    if rows:
        try:
            with open(csv_base_path, "w", encoding="utf-8", newline="") as f:
                # Беремо заголовки з ключів першого рядка
                fieldnames = list(rows[0].keys())
                
                writer = csv.DictWriter(
                    f,
                    fieldnames=fieldnames,
                    quoting=csv.QUOTE_MINIMAL,
                    delimiter=",",
                    escapechar="\\"
                )

                writer.writeheader()

                for row in rows:
                    # Декодування HTML сутностей (наприклад &quot; -> ")
                    decoded_row = {
                        k: html.unescape(v) if isinstance(v, str) else v
                        for k, v in row.items()
                    }
                    writer.writerow(decoded_row)

            success_msg = f"✔ Експорт успішно виконано: {len(rows)} записів."
            logging.info(success_msg)
            print(f"{success_msg}\n📁 Файл: {csv_base_path}")

        except IOError as e:
            err_io = f"❌ Помилка запису файлу: {e}"
            logging.error(err_io)
            print(err_io)
    else:
        empty_msg = "⚠ Результат SQL запиту пустий. Файл не створено."
        logging.warning(empty_msg)
        print(empty_msg)

def download_supplier_price_list(supplier_id):
    """
    Скачує прайс-лист від постачальника за його ID.
    """
    # 0. Налаштування логування для дописування
    oc_log_message()

    # 1. Завантаження налаштувань
    settings = load_oc_settings()
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
    oc_log_message()

    # 1. Завантаження налаштувань
    settings = load_oc_settings()
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
    skipped_by_date_in_name = 0
    skipped_by_empty_barcode = 0

    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile, delimiter=delimiter)
            headers = next(reader)
            date_pattern = re.compile(r'\b(0[1-9]|1[0-2])\.\d{4}\b')
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

                # Перевірка на дату у назві товару (колонка B)
                if len(row) > 1:
                    product_name_raw = row[1]
                    if date_pattern.search(product_name_raw):
                        logging.warning(
                            f"🚫 Видалено рядок {row_number} через дату в назві товару ('{product_name_raw}')."
                        )
                        skipped_rows += 1
                        skipped_by_date_in_name += 1
                        continue

                # Перевірка штрихкоду (колонка S)
                if len(row) <= 18 or not row[18].strip():
                    logging.warning(
                        f"🚫 Видалено рядок {row_number} через порожній штрихкод (колонка S)."
                    )
                    skipped_rows += 1
                    skipped_by_empty_barcode += 1
                    continue
                
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
    logging.info(f"📅 Видалено через дату в назві: {skipped_by_date_in_name}")
    logging.info(f"🏷️ Видалено через відсутній штрихкод: {skipped_by_empty_barcode}")

def process_supplier_2_price_list():
    """
    Обробляє та очищає прайс-лист від постачальника 2.
    """
    # 0. Налаштування логування для дописування
    oc_log_message()

    # 1. Завантаження налаштувань
    settings = load_oc_settings()
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
    oc_log_message()

    # 1. Завантаження налаштувань
    settings = load_oc_settings()
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

                # Перетворення колонок 3 та 4 на цілі числа
                is_valid = True
                for col_index in [2, 3]:
                    value = row[col_index]
                    try:
                        # Перетворюємо значення на float, а потім на int, щоб позбутися .0
                        int_value = int(float(value))
                        if int_value < 0:
                            logging.warning(f"🚫 Видалено рядок {row_number} через від'ємне значення в колонці {col_index + 1}: '{value}'.")
                            is_valid = False
                            break
                        # Записуємо оброблене ціле значення назад у рядок
                        row[col_index] = str(int_value) 
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