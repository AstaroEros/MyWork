import os
import yaml
import logging
import pymysql
import csv
from datetime import datetime


# --- 1. ЗАВАНТАЖЕННЯ НАЛАШТУВАНЬ OPENCART ---
def load_oc_settings():
    """
    Завантаження конфігурації з oc_settings.yaml
    """
    # Будуємо шлях до файлу. 
    # os.path.dirname(__file__) — це папка, де лежить цей скрипт.
    # ".." — вихід на рівень вище.
    # Вказуємо нове ім'я файлу з розширенням .yaml
    config_path = os.path.join(os.path.dirname(__file__), "..", "oc_config", "oc_settings.yaml")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            # Використовуємо safe_load, це стандарт безпеки для YAML
            return yaml.safe_load(f)

    except FileNotFoundError:
        print(f"❌ Помилка: oc_settings.yaml не знайдено за шляхом: {config_path}")
        return None

    except yaml.YAMLError as exc:
        # yaml.YAMLError — це аналог json.JSONDecodeError для YAML
        if hasattr(exc, 'problem_mark'):
            mark = exc.problem_mark
            print(f"❌ Помилка синтаксису YAML у рядку {mark.line + 1}, стовпчик {mark.column + 1}")
        else:
            print(f"❌ Помилка: файл oc_settings.yaml пошкоджений.")
        return None

# --- 2. ПІДКЛЮЧЕННЯ ДО БД OPENCART ---
def oc_connect_db():
    """
    Повертає активне підключення до бази OpenCart
    """
    settings = load_oc_settings()
    
    # Базова перевірка: чи завантажився файл і чи є головний блок
    if not settings or "db" not in settings:
        raise Exception("❌ Неможливо завантажити DB-налаштування з oc_settings.yaml")

    db = settings["db"]

    try:
        connection = pymysql.connect(
            host=db["host"],       # Якщо ключа немає в YAML — вилетить KeyError
            user=db["user"],
            password=db["password"], 
            database=db["database"],
            port=db.get("port", 3306), # .get() дозволяє порту бути необов'язковим
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection

    # Окремо ловимо відсутність налаштувань ("лінива" валідація)
    except KeyError as e:
        print(f"❌ Помилка конфігурації: у файлі YAML відсутній обов'язковий параметр {e}")
        return None

    # Окремо ловимо помилки самого підключення (невірний пароль, сервер лежить)
    except pymysql.MySQLError as e:
        print(f"❌ Помилка підключення до БД OpenCart: {e}")
        return None
        
    except Exception as e:
        print(f"❌ Непередбачена помилка: {e}")
        return None

# --- 3. СТВОРЕННЯ НОВОГО ЛОГ-ФАЙЛУ (ОЧИСТКА/АРХІВАЦІЯ) ---
def oc_setup_new_log_file():
    """
    Створює новий лог-файл.
    """
    settings = load_oc_settings()
    
    # 1. Беремо шлях прямо з YAML 
    log_path = settings["paths"]["log_path"]
    
    # Визначаємо папку (logs), щоб створити її, якщо немає
    log_dir = os.path.dirname(log_path)
    os.makedirs(log_dir, exist_ok=True)

    # 2. Архівація старого файлу
    if os.path.exists(log_path):
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            archive_name = f"oc_logs_{timestamp}.log"
            
            # Архів кладемо в ту ж папку
            archive_path = os.path.join(log_dir, archive_name)
            
            os.rename(log_path, archive_path)
            print(f"📦 Старий лог архівовано: {archive_name}")
        except OSError as e:
            print(f"❌ Помилка архівації: {e}")

    # 3. Налаштування логування
    try:
        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format="%(asctime)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            filemode="w",
            force=True
        )
        logging.info("--- Новий сеанс логування OpenCart розпочато ---")
        print(f"✅ Лог файл створено: {log_path}")
        
    except Exception as e:
        print(f"❌ Не вдалося ініціалізувати лог-файл: {e}")

# --- 4. ДОПИСУВАННЯ В ІСНУЮЧИЙ oc_logs.log ---
def oc_log_message(message=None):
    """
    Ініціалізує логування або дописує повідомлення.
    При запуску нового скрипта з ланцюжка — читає шлях з YAML і підхоплює існуючий файл.
    """
    
    # Перевірка: чи логування ВЖЕ налаштоване у поточному запущеному скрипті?
    # Якщо ні (це перший виклик у цьому скрипті) — налаштовуємо.
    if not logging.getLogger().hasHandlers():
        
        # 1. Читаємо налаштування, щоб знати КУДИ писати
        settings = load_oc_settings()
        log_path = settings["paths"]["log_path"]

        # 2. Переконуємось, що папка існує (про всяк випадок)
        log_dir = os.path.dirname(log_path)
        os.makedirs(log_dir, exist_ok=True)

        # 3. Підключаємось до файлу в режимі 'a' (append - дописування)
        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format="%(asctime)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            filemode="a" 
        )

    # --- Логіка запису ---
    
    # Якщо передали повідомлення — пишемо його і в файл, і в консоль
    if message:
        logging.info(message)
        print(f"📝 {message}")
    
    # Якщо message=None, ми нічого не робимо (просто ініціалізували логер вище).

# --- 5. ЗАВАНТАЖЕННЯ ФАЙЛУ attribute.csv ---
def load_attributes_csv():
    """
    Завантажує атрибути. 
    Повертає словник замін, де значення — це кортеж (UA_Standard, RU_Translation).
    Остання колонка файлу вважається російським перекладом.
    """
    settings = load_oc_settings()
    if not settings:
        return {}, []

    attribute_path = settings["paths"]["attribute"]
    replacements_map = {}
    raw_data = []
    
    # Оновлений заголовок (додано Russian в кінці)
    default_header = ["column_name", "attr_site_name", "atr_a", "atr_b", "atr_c", "atr_d", "atr_e", "atr_f", "atr_g", "atr_h", "atr_i", "Russian"]
    current_block_name = None

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
                # Нормалізація довжини рядка
                current_len = len(row)
                if current_len < max_row_len:
                    row = row + [''] * (max_row_len - current_len)
                else:
                    row = row[:max_row_len]
                
                raw_data.append(row)
                
                first_cell = row[0].strip()

                # 1. Початок блоку (назва атрибута, наприклад "Основа|ua")
                if first_cell:
                    current_block_name = first_cell
                    if current_block_name not in replacements_map:
                        replacements_map[current_block_name] = {}
                
                # 2. Рядок зі значеннями
                elif current_block_name is not None and len(row) >= 3:
                    
                    # UA Standard - колонка 1
                    ua_std = row[1].strip()
                    
                    # RU Translation - ОСТАННЯ колонка (-1)
                    ru_std = row[-1].strip()

                    # Якщо немає UA значення, рядок порожній або службовий -> пропускаємо логіку замін
                    if not ua_std:
                        continue

                    # Синоніми — це все між UA (index 1) та RU (index -1)
                    # Тобто slice [2:-1]
                    synonyms = row[2:-1]

                    for original in synonyms:
                        original = original.strip().lower()
                        if original:
                            # Зберігаємо ПАРУ: (UA, RU)
                            replacements_map[current_block_name][original] = (ua_std, ru_std)

        return replacements_map, raw_data
    
    except Exception as e:
        logging.error(f"Помилка attribute.csv: {e}")
        return {}, [default_header]

# --- 6. ЗБЕРЕЖЕННЯ ФАЙЛУ attribute.csv ---    
def save_attributes_csv(raw_data):
    """
    Зберігає оновлені сирі дані у attribute.csv.
    """
    settings = load_oc_settings()
    if not settings or "paths" not in settings or "attribute" not in settings["paths"]:
        logging.error("❌ У settings.json не знайдено paths.attribute")
        return {}, []
    attribute_path = settings["paths"]["attribute"]
    try:
        # 'newline=''' важливий для коректного збереження CSV на різних ОС
        with open(attribute_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(raw_data)
        logging.info("Файл атрибутів attribute.csv оновлено.")
    except Exception as e:
        logging.error(f"Помилка при збереженні файлу атрибутів attribute.csv: {e}")

# --- 7. ІМПОРТ КАТЕГОРІЙ З CSV В БД OPENCART ---
def oc_import_categories_from_csv():
    """
    Імпорт категорій з CSV у OpenCart з використанням path_id
    """
    oc_log_message("▶ Старт імпорту категорій (CSV → OpenCart, path_id)")

    csv_path = "/var/www/scripts/update/csv/output/oc_categorii.csv"

    if not os.path.exists(csv_path):
        oc_log_message(f"❌ CSV файл не знайдено: {csv_path}")
        return

    conn = oc_connect_db()
    if not conn:
        oc_log_message("❌ Не вдалося підключитись до БД")
        return

    cursor = conn.cursor()

    # Мови (з твоєї БД)
    languages = {
        "uk-ua": 2,
        "ru-ru": 3
    }

    # --- локальні safe-хелпери ---
    def safe_int(v, default=0):
        if v is None:
            return default
        s = str(v).strip()
        if s == "":
            return default
        try:
            return int(s)
        except ValueError:
            try:
                return int(float(s))
            except Exception:
                return default

    def safe_bool(v, default=0):
        if v is None:
            return default
        s = str(v).strip().lower()
        if s in ("1", "true", "yes", "y", "on"):
            return 1
        if s in ("0", "false", "no", "n", "off"):
            return 0
        return default

    imported = 0

    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")

            for row in reader:
                # прибираємо BOM
                row = {k.lstrip("\ufeff").strip(): (v if v is not None else "") for k, v in row.items()}

                category_id = safe_int(row.get("category_id"), None)
                if category_id is None:
                    oc_log_message("⚠️ Пропущено рядок без category_id")
                    continue

                parent_id = safe_int(row.get("parent_id"), 0)
                sort_order = safe_int(row.get("sort_order"), 0)

                image = row.get("image_name") or None
                top = safe_bool(row.get("top"), 0)
                column_value = safe_int(row.get("column"), 1)
                status = safe_bool(row.get("status"), 1)

                page_group_links = row.get("page_group_links") or ""
                date_added = row.get("date_added") or "2000-01-01 00:00:00"
                date_modified = row.get("date_modified") or date_added

                # --- oc_category ---
                cursor.execute("""
                    DELETE FROM oc_seo_url
                    WHERE query = %s
                    """, (f"category_id={category_id}",))
                cursor.execute(
                    """
                    INSERT INTO oc_category
                    (category_id, image, parent_id, top, `column`,
                     sort_order, status, page_group_links,
                     date_added, date_modified)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        image = VALUES(image),
                        parent_id = VALUES(parent_id),
                        top = VALUES(top),
                        `column` = VALUES(`column`),
                        sort_order = VALUES(sort_order),
                        status = VALUES(status),
                        page_group_links = VALUES(page_group_links),
                        date_modified = VALUES(date_modified)
                    """,
                    (
                        category_id,
                        image,
                        parent_id,
                        top,
                        column_value,
                        sort_order,
                        status,
                        page_group_links,
                        date_added,
                        date_modified
                    )
                )

                # --- oc_category_description ---
                for code, language_id in languages.items():
                    cursor.execute(
                        """
                        INSERT INTO oc_category_description
                        (category_id, language_id, name, description, description2,
                         meta_title, meta_description, meta_keyword)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            name = VALUES(name),
                            description = VALUES(description),
                            description2 = VALUES(description2),
                            meta_title = VALUES(meta_title),
                            meta_description = VALUES(meta_description),
                            meta_keyword = VALUES(meta_keyword)
                        """,
                        (
                            category_id,
                            language_id,
                            row.get(f"name({code})", "") or "",
                            row.get(f"description({code})", "") or "",
                            row.get(f"description2({code})", "") or "",
                            row.get(f"meta_title({code})", "") or "",
                            row.get(f"meta_description({code})", "") or "",
                            row.get(f"meta_keywords({code})", "") or ""
                        )
                    )

                # --- oc_seo_url ---
                seo_map = [
                    (2, "uk-ua"),
                    (3, "ru-ru"),
                ]
                for lang_id, suffix in seo_map:
                    seo_keyword = row.get(f"seo_keyword({suffix})")

                    if seo_keyword:
                        cursor.execute("""
                            INSERT INTO oc_seo_url
                            (store_id, language_id, query, keyword)
                            VALUES (0, %s, %s, %s)
                        """, (
                            lang_id,
                            f"category_id={category_id}",
                            seo_keyword.strip()
                        ))
                # --- oc_category_to_store ---
                cursor.execute(
                    "INSERT IGNORE INTO oc_category_to_store (category_id, store_id) VALUES (%s, %s)",
                    (category_id, 0)
                )

                # --- oc_category_path (CSV path_id) ---
                cursor.execute(
                    "DELETE FROM oc_category_path WHERE category_id = %s",
                    (category_id,)
                )

                path_raw = row.get("path_id", "").strip()
                if path_raw:
                    path_parts = [safe_int(x) for x in path_raw.split(">") if safe_int(x) > 0]
                else:
                    path_parts = [category_id]

                for level, path_id in enumerate(path_parts):
                    cursor.execute(
                        """
                        INSERT INTO oc_category_path (category_id, path_id, level)
                        VALUES (%s, %s, %s)
                        """,
                        (category_id, path_id, level)
                    )

                imported += 1

        conn.commit()
        oc_log_message(f"✅ Імпорт категорій завершено. Оброблено: {imported}")

    except Exception as e:
        conn.rollback()
        oc_log_message(f"❌ Помилка імпорту категорій: {e}")

    finally:
        cursor.close()
        conn.close()

# --- 8. ЗАВАНТАЖЕННЯ КАТЕГОРІЙ (Нова логіка)
def load_category_csv():
    """
    Завантажує правила мапінгу з category.csv.
    Повертає:
    1. category_map: Словник {(name1, name2, name3): {'id':..., 'ua':..., 'ru':...}}
    2. fieldnames: Список заголовків файлу (щоб знати, як дописувати нові).
    """
    settings = load_oc_settings()
    category_csv_path = settings['paths']['category_csv']
    category_map = {}
    
    # Стандартні заголовки, якщо файл порожній
    fieldnames = ['name_1', 'name_2', 'name_3', 'category_name', 'category', 'Категорія|ua', 'Категория|ru']

    try:
        with open(category_csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            if reader.fieldnames:
                fieldnames = reader.fieldnames

            for row in reader:
                # Очищаємо та приводимо до нижнього регістру вхідні назви
                n1 = row.get('name_1', '').strip().lower()
                n2 = row.get('name_2', '').strip().lower()
                n3 = row.get('name_3', '').strip().lower()
                
                # Якщо рядок порожній (немає назв), пропускаємо
                if not any([n1, n2, n3]):
                    continue

                # Сортуємо для створення унікального ключа незалежно від порядку слів у товарі
                key = tuple(sorted([n1, n2, n3]))
                
                category_map[key] = {
                    'category': row.get('category', '').strip(),
                    'cat_ua':   row.get('Категорія|ua', '').strip(),
                    'cat_ru':   row.get('Категория|ru', '').strip()
                }

        return category_map, fieldnames

    except FileNotFoundError:
        logging.warning(f"Файл 'category.csv' не знайдено. Буде створено новий при збереженні.")
        return {}, fieldnames
    except Exception as e:
        logging.error(f"Помилка при читанні category.csv: {e}")
        return {}, fieldnames

# --- 9. ДОДАВАННЯ НОВИХ КАТЕГОРІЙ (Append)
def append_new_categories(new_rows, fieldnames):
    """
    Дописує нові знайдені комбінації в кінець category.csv.
    Використовує DictWriter для точності.
    """
    if not new_rows:
        return

    settings = load_oc_settings()
    category_csv_path = settings['paths']['category_csv']
    
    try:
        # Перевіряємо, чи файл існує (щоб записати хедер, якщо ні)
        file_exists = os.path.isfile(category_csv_path)
        
        with open(category_csv_path, 'a', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            # Записуємо тільки нові рядки
            for row in new_rows:
                # Фільтруємо row, залишаючи тільки ті ключі, що є в fieldnames
                clean_row = {k: row.get(k, '') for k in fieldnames}
                writer.writerow(clean_row)
                
        logging.info(f"У файл category.csv додано {len(new_rows)} нових правил.")
        
    except Exception as e:
        logging.error(f"Помилка при запису в category.csv: {e}")

# --- 10.  ЗАВАНТАЖЕННЯ ПОЗНАЧОК (Виправлено шлях)
def load_poznachky_csv():
    """
    Завантажує список тегів з poznachky.csv.
    """
    settings = load_oc_settings()
    poznachky_csv_path = settings['paths']['poznachky_csv'] 
    
    poznachky_list = []
    
    try:
        with open(poznachky_csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            # Пробуємо пропустити заголовок, якщо він є
            try:
                first_row = next(reader)
                # Якщо перший рядок схожий на заголовок, ігноруємо, інакше додаємо
                if first_row and "poznachky" not in first_row[0].lower():
                     poznachky_list.append(first_row[0].strip().lower())
            except StopIteration:
                pass 

            for row in reader:
                if row and row[0].strip():
                    poznachky_list.append(row[0].strip().lower())

        # Сортуємо: спочатку довгі фрази, щоб "вібратор кролик" знайшовся раніше ніж просто "вібратор"
        poznachky_list.sort(key=len, reverse=True)
        return poznachky_list
    
    except FileNotFoundError:
        logging.warning(f"Файл позначок '{poznachky_csv_path}' не знайдено. Пропускаємо.")
        return []
    except Exception as e:
        logging.error(f"Помилка poznachky.csv: {e}")
        return []

# --- ПЕРЕВІРКА CSV ---
def check_csv_data(profile_id):
    """
    Перевіряє CSV-файл на відповідність правилам, визначеним у oc_settings.json.

    Args:
        profile_id (str): ID профілю перевірки з 'validation_profiles' в oc_settings.json.

    Returns:
        bool: True, якщо перевірка пройшла успішно, інакше False.
    """

    # 0. Ініціалізація логування
    oc_log_message(f"🔎 Старт перевірки CSV (профіль: {profile_id})")

    # 1. Завантаження налаштувань
    settings = load_oc_settings()
    if not settings:
        oc_log_message("❌ Не вдалося завантажити oc_settings.json")
        return False

    profiles = settings.get("validation_profiles", {})
    if profile_id not in profiles:
        oc_log_message(f"❌ Не знайдено профіль валідації з ID '{profile_id}'")
        return False

    # 2. Дані профілю
    profile = profiles[profile_id]
    csv_path_relative = profile.get("path")
    validation_rules = profile.get("rules")

    if not csv_path_relative or validation_rules is None:
        oc_log_message("❌ Неповні дані в профілі валідації")
        return False

    # 3. Перевірка файлу
    base_dir = os.path.join(os.path.dirname(__file__), "..")
    full_csv_path = os.path.join(base_dir, csv_path_relative)

    if not os.path.exists(full_csv_path):
        oc_log_message(f"❌ CSV файл не знайдено: {full_csv_path}")
        return False

    oc_log_message(f"📄 Перевіряється файл: {full_csv_path}")

    # 4. Читання та валідація CSV
    try:
        with open(full_csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)

            try:
                headers = next(reader)
            except StopIteration:
                oc_log_message("❌ CSV файл порожній (відсутні заголовки)")
                return False

            # 5. Перевірка заголовків
            expected_columns = list(validation_rules.keys())
            headers_set = set(headers)

            for col_name in expected_columns:
                if col_name not in headers_set:
                    oc_log_message(f"❌ Відсутня обовʼязкова колонка: '{col_name}'")
                    return False

            header_map = {name: idx for idx, name in enumerate(headers)}

            # 6. Перевірка рядків
            for i, row in enumerate(reader):
                row_number = i + 2

                if not row or all(not col.strip() for col in row):
                    oc_log_message(f"ℹ️ Рядок {row_number} порожній — пропущено")
                    continue

                for col_name, rule_type in validation_rules.items():
                    col_index = header_map.get(col_name)

                    if col_index is None or col_index >= len(row):
                        oc_log_message(f"❌ Рядок {row_number}: некоректна структура CSV")
                        return False

                    value = row[col_index].strip()

                    # 6.1 not_empty
                    if rule_type == "not_empty":
                        if not value:
                            oc_log_message(f"❌ Рядок {row_number}, '{col_name}': поле не повинно бути порожнім")
                            return False
                        continue

                    # 6.2 integer
                    if rule_type == "integer":
                        if not value or not value.lstrip("-").isdigit():
                            oc_log_message(
                                f"❌ Рядок {row_number}, '{col_name}': очікується ціле число, отримано '{value}'"
                            )
                            return False

                    # 6.3 integer_or_empty
                    elif rule_type == "integer_or_empty":
                        if value and not value.lstrip("-").isdigit():
                            oc_log_message(
                                f"❌ Рядок {row_number}, '{col_name}': очікується ціле число або порожнє поле"
                            )
                            return False

                    # 6.4 float_or_empty
                    elif rule_type == "float_or_empty":
                        if value:
                            try:
                                float(value.replace(",", "."))
                            except ValueError:
                                oc_log_message(
                                    f"❌ Рядок {row_number}, '{col_name}': очікується float або порожнє поле"
                                )
                                return False

                    # 6.5 datetime
                    elif rule_type == "datetime":
                        if value:
                            try:
                                datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
                            except ValueError:
                                oc_log_message(
                                    f"❌ Рядок {row_number}, '{col_name}': невірний формат datetime"
                                )
                                return False

                    # 6.6 список допустимих значень
                    elif isinstance(rule_type, list):
                        if value not in rule_type:
                            oc_log_message(
                                f"❌ Рядок {row_number}, '{col_name}': значення '{value}' не входить у {rule_type}"
                            )
                            return False

    except Exception as e:
        oc_log_message(f"❌ Невідома помилка під час перевірки CSV: {e}")
        return False

    oc_log_message("✅ Перевірка CSV успішна")
    return True
