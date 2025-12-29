import csv
import logging
import os
import random
import time
import requests
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from scr.oc_base_function import oc_log_message, load_oc_settings, load_attributes_csv, save_attributes_csv, \
                                load_category_csv, append_new_categories, load_poznachky_csv, clear_directory, \
                                download_product_images, move_gifs, convert_to_webp_square, sync_webp_column_named, \
                                copy_to_site, fill_opencart_paths_single_file, get_deepl_usage, translate_text_deepl, \
                                get_first_sentence

# ОСНОВНА ФУНКЦІЯ 1: Перевірка зміни артикулу і штрихкоду
def find_change_art_shtrihcod():
    """
    Перевіряє розбіжності:
    1) по штрихкоду (порівняння по артикулу)
    2) по артикулу (порівняння по штрихкоду)
    Усі розбіжності записує у change_art_shtrihcod
    """

    oc_log_message()
    logging.info("▶ Старт перевірки артикулів і штрихкодів (2 напрямки)")

    settings = load_oc_settings()
    if not settings:
        logging.info("❌ oc_settings.yaml не завантажено")
        return

    site_csv = settings["paths"]["output_file"]
    supplier_csv = settings["suppliers"][1]["csv_path"]
    result_csv = settings["paths"]["change_art_shtrihcod"]

    # --------------------------------------------------
    # 1. Читаємо прайс постачальника
    # --------------------------------------------------
    supplier_by_artykul = {}   # Код_товара -> Штрих_код
    supplier_by_shtrih = {}    # Штрих_код  -> Код_товара

    with open(supplier_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")

        for row in reader:
            artykul = row.get("Код_товара", "").strip()
            shtrih = row.get("Штрих_код", "").strip()

            if artykul and shtrih:
                supplier_by_artykul[artykul] = shtrih
                supplier_by_shtrih[shtrih] = artykul

    logging.info(
        f"ℹ Товарів постачальника: "
        f"{len(supplier_by_artykul)} (по артикулу), "
        f"{len(supplier_by_shtrih)} (по штрихкоду)"
    )

    # --------------------------------------------------
    # 2. Очищення файлу результату
    # --------------------------------------------------
    headers = [
        "sku",
        "shtrih_cod",
        "artykul_lutsk",
        "Код_товара",
        "Штрих_код"
    ]

    with open(result_csv, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(headers)

    logging.info("🧹 change_art_shtrihcod очищено")

    # --------------------------------------------------
    # 3. Порівняння (2 напрямки)
    # --------------------------------------------------
    checked = 0
    diff_count = 0
    written_keys = set()  # захист від дублів

    with open(site_csv, newline="", encoding="utf-8") as site_f, \
         open(result_csv, "a", newline="", encoding="utf-8") as out_f:

        site_reader = csv.DictReader(site_f)
        writer = csv.writer(out_f)

        for row in site_reader:
            sku = row.get("sku", "").strip()
            site_shtrih = row.get("shtrih_cod", "").strip()
            site_artykul = row.get("artykul_lutsk", "").strip()

            # ---------- ПРАВИЛО 1 ----------
            # Є артикул → порівнюємо штрихкод
            if site_artykul and site_artykul in supplier_by_artykul:
                checked += 1
                supplier_shtrih = supplier_by_artykul[site_artykul]

                if site_shtrih != supplier_shtrih:
                    key = (sku, site_artykul, supplier_shtrih)
                    if key not in written_keys:
                        writer.writerow([
                            sku,
                            site_shtrih,
                            site_artykul,
                            site_artykul,
                            supplier_shtrih
                        ])
                        written_keys.add(key)
                        diff_count += 1

            # ---------- ПРАВИЛО 2 ----------
            # Є штрихкод → порівнюємо артикул
            if site_shtrih and site_shtrih in supplier_by_shtrih:
                checked += 1
                supplier_artykul = supplier_by_shtrih[site_shtrih]

                if site_artykul != supplier_artykul:
                    key = (sku, site_shtrih, supplier_artykul)
                    if key not in written_keys:
                        writer.writerow([
                            sku,
                            site_shtrih,
                            site_artykul,
                            supplier_artykul,
                            site_shtrih
                        ])
                        written_keys.add(key)
                        diff_count += 1

    # --------------------------------------------------
    # 4. Підсумок
    # --------------------------------------------------
    logging.info(f"✅ Перевірено позицій: {checked}")
    logging.info(f"⚠ Знайдено розбіжностей: {diff_count}")

# ОСНОВНА ФУНКЦІЯ 2: Знаходження нових товарів
def find_new_products():
    """
    Порівнює артикули товарів з прайс-листа постачальника з артикулами,
    що є на сайті, і записує нові товари в окремий файл.
    """
    # --- 1. Ініціалізація логування ---
    oc_log_message()
    logging.info("ФУНКЦІЯ 1. Починаю пошук нових товарів...")
    
    # --- 2. Завантаження налаштувань з settings.json ---
    settings = load_oc_settings()
    if not settings:
        logging.info("❌ oc_settings.yaml не завантажено")
        return
    
    # --- 3. Отримання шляхів до потрібних файлів ---
    zalishki_path = settings['paths']['output_file']                         # База існуючих товарів
    supliers_new_path = settings['paths']['csv_path_new_product']         # Файл, куди буде записано нові товари
    supliers_csv_path = settings['suppliers'][1]['csv_path']                 # Прайс-лист постачальника 1
    delimiter = settings['suppliers'][1]['delimiter']                        # Роздільник у CSV
    
    # --- 4. Отримання допоміжних параметрів постачальника ---
    sku_prefix = settings['suppliers'][1]['search']                          # Префікс для пошуку
       
    logging.info("Зчитую існуючі артикули з файлу, вказаного за ключем 'csv_path_zalishki'.")

    try:

        # --- 3. Зчитування заголовків з файлу нових товарів ---
        fieldnames = []
        with open(supliers_new_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            try:
                fieldnames = next(reader) # Отримуємо список назв колонок: ['search', 'url_lutsk', ...]
            except StopIteration:
                logging.info("❌ Файл для запису порожній.")
                return

        logging.info(f"Структуру файлу зчитано. Колонок: {len(fieldnames)}")

        # --- 4. Зчитування існуючих артикулів із бази (ОНОВЛЕНО) ---
        logging.info("Зчитую базу існуючих товарів...")
        with open(zalishki_path, mode='r', encoding='utf-8') as zalishki_file:
            # Використовуємо DictReader, щоб звертатися по назві
            zalishki_reader = csv.DictReader(zalishki_file)

            existing_skus = {
                row.get("artykul_lutsk", "").strip().lower() 
                for row in zalishki_reader 
                if row.get("artykul_lutsk")
            }
            
            logging.info(f"Зчитано {len(existing_skus)} унікальних артикулів із бази.")

        # --- 5. Відкриваємо файл для запису (DictWriter) ---
        logging.info("Відкриваю файл для запису нових товарів...")
        with open(supliers_new_path, mode='w', encoding='utf-8', newline='') as new_file:
            # Ініціалізуємо Writer, передаючи йому список заголовків
            writer = csv.DictWriter(new_file, fieldnames=fieldnames)
            writer.writeheader() # Записуємо заголовки назад у файл
            
            # --- 6. Читаємо прайс постачальника (DictReader) ---
            with open(supliers_csv_path, mode='r', encoding='utf-8') as supliers_file:
                # DictReader автоматично візьме перший рядок прайсу як ключі словника
                supliers_reader = csv.DictReader(supliers_file, delimiter=delimiter)
                
                new_products_count = 0

                for row in supliers_reader:
                    # Отримуємо артикул по назві колонки
                    sku = row.get("Код_товара", "").strip().lower()
                    
                    if sku and sku not in existing_skus:
                        
                        # Створюємо пустий словник для нового рядка, заповнюємо порожніми значеннями
                        new_row = {key: '' for key in fieldnames}
                        
                        # --- 7. ПРЯМЕ МАПУВАННЯ (Назва -> Назва) ---
                        
                        # Спеціальне поле search (префікс + код)
                        new_row["search"] = sku_prefix + row.get("Код_товара", "")
                        
                        # Основні поля (беруться прямо з row по ключу)
                        new_row["shtrih_cod"] = row.get("Штрих_код", "")
                        new_row["Код_товара"] = row.get("Код_товара", "")
                        new_row["Название_позиции"] = row.get("Название_позиции", "")
                        new_row["Описание"] = row.get("Описание", "")
                        new_row["Цена"] = row.get("Цена", "")
                        new_row["Наличие"] = row.get("Наличие", "")
                        new_row["Производитель"] = row.get("Производитель", "")
                        new_row["Страна_производитель"] = row.get("Страна_производитель", "")
                        new_row["Категория"] = row.get("Категория", "")
                        new_row["Доп. Категория 1"] = row.get("Доп. Категория 1", "")
                        new_row["Доп. Категория 2"] = row.get("Доп. Категория 2", "")

                        # --- 8. Запис рядка ---
                        writer.writerow(new_row)
                        new_products_count += 1

        # --- 17. Підсумкове логування ---
        logging.info(f"✅ Знайдено {new_products_count} нових товарів.")
        logging.info(f"Дані записано у файл csv 'supliers_new_path'.")

    # --- 18. Обробка помилок ---
    except FileNotFoundError as e:
        logging.info(f"❌ Помилка: Файл не знайдено - {e}")
    except Exception as e:
        logging.info(f"❌ Виникла непередбачена помилка: {e}")

# ОСНОВНА ФУНКЦІЯ 3: Знаходження урл товару
def find_product_url():
    """
    Зчитує файл з новими товарами, переходить за URL-адресою,
    знаходить URL-адресу простого або варіативного товару,
    і записує знайдену URL-адресу в колонку B(1) в тимчасовий файл.
    """

    # --- 1. Ініціалізація логування (підключаємо існуючий лог-файл) ---
    oc_log_message()
    logging.info("ФУНКЦІЯ 2. Починаю пошук URL-адрес товарів...")
    
    # --- 2. Завантаження налаштувань та формування шляхів/тимчасового файлу ---
    settings = load_oc_settings()
    if not settings:
        logging.error("❌ Не вдалося завантажити налаштування. Обробка прайс-листа перервана.")
        return
    supliers_new_path = settings['paths']['csv_path_new_product']     # вхідний CSV (1.csv)
    site_url = settings['suppliers'][1]['site']                       # базовий URL сайту (щоб додавати відносні посилання)
    temp_file_path = supliers_new_path + '.temp'                      # тимчасовий файл під час запису

    # --- Лічильники і статистика ---
    total_rows = 0
    found_variant_count = 0
    found_simple_count = 0
    not_found_count = 0
    found_variant_rows = []
    not_found_rows = []

    try:
        # --- 3. Відкриваємо вхідний файл для читання ---
        with open(supliers_new_path, mode='r', encoding='utf-8') as input_file:
            reader = csv.DictReader(input_file)
            # ВАЖЛИВО: Отримуємо заголовки одразу
            fieldnames = reader.fieldnames
            
            # --- ПЕРЕВІРКА НА ПОМИЛКУ: Якщо заголовки пусті ---
            if not fieldnames:
                logging.error("❌ Помилка: Не вдалося прочитати заголовки стовпців. Перевірте, чи файл не пустий і чи коректне кодування.")
                return
            # Перевірка наявності критичних колонок
            required_columns = ['search', 'sku', 'url_lutsk']
            for col in required_columns:
                if col not in fieldnames:
                    logging.error(f"❌ У файлі відсутня обов'язкова колонка: {col}")
                    return

            # --- 4. Відкриваємо тимчасовий файл для запису (DictWriter) ---
            with open(temp_file_path, mode='w', encoding='utf-8', newline='') as output_file:
                writer = csv.DictWriter(output_file, fieldnames=fieldnames)
                writer.writeheader() # Автоматично записує рядок заголовків
                
                # --- 5. Ітерація по рядках ---
                for idx, row in enumerate(reader):
                    total_rows += 1
                    
                    # 5.1. Витягуємо дані по назвах колонок
                    search_url = row.get('search', '').strip()
                    
                    file_sku = row.get('Код_товара', '').strip()

                    # --- 6. Перевірка валідності URL ---
                    if not search_url or search_url.startswith('Помилка запиту'):
                        writer.writerow(row)
                        continue

                    try:
                        # --- 7. Виконання HTTP-запиту до search_url і парсинг HTML ---
                        response = requests.get(search_url)
                        response.raise_for_status()
                        soup = BeautifulSoup(response.text, 'html.parser')
                        found_type = None  # 'variant' або 'simple'
                        found_url = None  # сюди запишемо знайдену реальну URL-адресу товару
                        
                        # --- 8. Пошук варіативних товарів (input.variant_control[data-code]) ---
                        # Шукаємо input теги з класом variant_control та атрибутом data-code,
                        # порівнюємо data-code з file_sku — якщо співпадіння, беремо посилання у батьківському блоці.
                        variant_inputs = soup.find_all('input', class_='variant_control', attrs={'data-code': True})
                        for input_tag in variant_inputs:
                            site_sku = input_tag.get('data-code', '').strip()
                            if file_sku == site_sku:
                                parent_div = input_tag.find_parent('div', class_='card-block')
                                if parent_div:
                                    link_tag = parent_div.find('h4', class_='card-title').find('a')
                                    if link_tag and link_tag.has_attr('href'):
                                        # Формуємо повний URL (додаємо site_url до відносного шляху)
                                        found_url = site_url + link_tag['href']
                                        found_type = 'variant'
                                        break

                        # --- 9. Якщо не знайшли серед варіантів — шукаємо прості товари ---
                        if not found_url:
                            # Для простих товарів шукаємо div з класом 'radio', беремо текст як SKU,
                            # і за таким же підходом знаходимо посилання у блоці card-block.
                            simple_divs = soup.find_all('div', class_='radio')
                            for div_tag in simple_divs:
                                site_sku = div_tag.get_text(strip=True).strip()
                                if file_sku == site_sku:
                                    parent_div = div_tag.find_parent('div', class_='card-block')
                                    if parent_div:
                                        link_tag = parent_div.find('h4', class_='card-title').find('a')
                                        if link_tag and link_tag.has_attr('href'):
                                            found_url = site_url + link_tag['href']
                                            found_type = 'simple'
                                            break

                        # --- 10. Запис результату в колонку 'url_lutsk' або логування якщо не знайдено ---
                        if found_url:
                            row['url_lutsk'] = found_url
                            if found_type == 'variant':
                                found_variant_count += 1
                                found_variant_rows.append(idx + 2)  # +2, бо рядки CSV рахуються з 1 + заголовок
                            elif found_type == 'simple':
                                found_simple_count += 1
                        else:
                            not_found_count += 1
                            not_found_rows.append(idx + 2)

                        # Записуємо (знайдений або незмінений) рядок у тимчасовий файл
                        writer.writerow(row)

                    except requests.RequestException as e:
                        # --- 11. Обробка помилок запиту ---
                        logging.error(f"Рядок {idx + 2}: Помилка при запиті: {e}")
                        # Записуємо помилку в колонку 'search', як було раніше
                        row['search'] = f'Помилка запиту: {e}'
                        writer.writerow(row)
                    
                    # --- 12. Додаткова пауза між запитами (рандомізована) для уникнення бана/DDOS ---
                    time.sleep(random.uniform(1, 3))
  
        # --- 13. Після успішної обробки: заміна оригінального файлу тимчасовим ---
        os.replace(temp_file_path, supliers_new_path)

        # --- 14. Зведена статистика ---
        logging.info("=== ПІДСУМКОВА ІНФОРМАЦІЯ ===")
        logging.info(f"Всього рядків з товарами: {total_rows}")
        logging.info(
            f"Знайдено URL варіативних товарів: {found_variant_count}"
            + (f" (Рядки {', '.join(map(str, found_variant_rows))})" if found_variant_rows else "")
        )
        logging.info(f"Знайдено URL простих товарів: {found_simple_count}")
        logging.info(
            f"Не знайдено URL: {not_found_count}"
            + (f" (Рядки {', '.join(map(str, not_found_rows))})" if not_found_rows else "")
        )

    except FileNotFoundError as e:
        # --- 15. Обробка помилки: вхідний файл не знайдено ---
        logging.error(f"Помилка: Файл не знайдено - {e}")
    except Exception as e:
        # --- 16. Гарантійне прибирання: видаляємо тимчасовий файл при помилці, щоб не залишити сміття ---
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        logging.error(f"Виникла непередбачена помилка: {e}")

# ОСНОВНА ФУНКЦІЯ 4: Парсинг атрибутів
def parse_product_attributes():
    oc_log_message()
    logging.info("▶ ФУНКЦІЯ: Парсинг (Mapping + Auto-suffix |ua + Next Col RU)")

    settings = load_oc_settings()
    if not settings: return
    
    supliers_new_path = settings["paths"]["csv_path_new_product"]

    # =========================================================================
    # 🔧 СЛОВНИК ПЕРЕЙМЕНУВАННЯ АТРИБУТІВ
    # Тут ми вказуємо: "Як названо на сайті" : "Як називається колонка у файлі"
    # =========================================================================
    ATTRIBUTE_NAME_MAPPING = {
        "Країна": "Зроблено в|ua",
        "Марка/Лінія": "Виробник|ua",
        # Додавайте сюди нові, якщо будуть розбіжності
        # "Матеріал": "Склад тканини|ua", 
    }

    # Завантаження мапи значень (attribute.csv)
    replacements_map, raw_data = load_attributes_csv()
    changes_made = False
    max_raw_row_len = len(raw_data[0]) if raw_data and raw_data[0] else 12

    # Точки вставки
    insertion_points = {} 
    current_block_name = None
    for i, row_raw in enumerate(raw_data[1:], start=1):
        first_col = row_raw[0].strip()
        if first_col: 
            current_block_name = first_col
            insertion_points[current_block_name] = i + 1
        elif current_block_name:
            insertion_points[current_block_name] = i + 1

    new_attributes_counter = {} 
    temp_file_path = supliers_new_path + ".temp"

    try:
        with open(supliers_new_path, mode="r", encoding="utf-8") as input_file, \
             open(temp_file_path, mode="w", encoding="utf-8", newline="") as output_file:

            reader = csv.DictReader(input_file)
            fieldnames = reader.fieldnames 
            
            if not fieldnames:
                logging.error("❌ Файл порожній!")
                return
            
            logging.info(f"Знайдено колонок у файлі: {len(fieldnames)}")

            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            writer.writeheader()

            processed_count = 0

            for row in reader:
                processed_count += 1
                row_values = list(row.values())
                product_url = row_values[1].strip() if len(row_values) > 1 else ""

                if not product_url or product_url.startswith("Помилка"):
                    writer.writerow(row)
                    continue

                try:
                    response = requests.get(product_url, timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "html.parser")

                    parsed_attributes = {}
                    characteristics_div = soup.find("div", id="w0-tab0")
                    if characteristics_div and characteristics_div.find("table"):
                        for tr in characteristics_div.find("table").find_all("tr"):
                            cells = tr.find_all("td")
                            if len(cells) == 2:
                                key = cells[0].get_text(strip=True).replace(":", "")
                                value = cells[1].get_text(strip=True)
                                parsed_attributes[key] = value

                    other_attributes_list = []

                    # --- Обробка атрибутів ---
                    for attr_name_site, attr_value in parsed_attributes.items():
                        if attr_name_site == "Штрих-код": continue

                        # === ЛОГІКА ВИЗНАЧЕННЯ КОЛОНКИ ===
                        final_col_name = None
                        
                        # 1. Пріоритет: Перевіряємо ручний мапинг (Словник зверху)
                        if attr_name_site in ATTRIBUTE_NAME_MAPPING:
                            mapped_name = ATTRIBUTE_NAME_MAPPING[attr_name_site]
                            if mapped_name in row:
                                final_col_name = mapped_name
                        
                        # 2. Пріоритет: Автоматичний суфікс |ua
                        if not final_col_name:
                            target_col_ua = f"{attr_name_site}|ua"
                            if target_col_ua in row:
                                final_col_name = target_col_ua
                            elif attr_name_site in row: # Прямий збіг (без суфіксів)
                                final_col_name = attr_name_site
                        
                        # === ДІЇ З ЗНАЙДЕНОЮ КОЛОНКОЮ ===
                        if final_col_name:
                            original_value_lower = attr_value.strip().lower()
                            rules_for_this_attr = replacements_map.get(final_col_name, {})
                            
                            found_data = rules_for_this_attr.get(original_value_lower)

                            if found_data:
                                # Розпаковка значень (Tuple check)
                                if isinstance(found_data, tuple) and len(found_data) >= 2:
                                    ua_new, ru_new = found_data
                                else:
                                    ua_new = str(found_data)
                                    ru_new = "" 

                                # Пишемо UA
                                row[final_col_name] = ua_new
                                
                                # Пишемо RU (наступна колонка)
                                if ru_new:
                                    try:
                                        current_idx = fieldnames.index(final_col_name)
                                        if current_idx + 1 < len(fieldnames):
                                            next_col_name = fieldnames[current_idx + 1]
                                            row[next_col_name] = ru_new
                                    except ValueError: pass 
                            else:
                                # Нове значення -> пишемо оригінал
                                row[final_col_name] = attr_value
                                
                                # Додаємо в attribute.csv
                                if original_value_lower not in rules_for_this_attr:
                                    insert_index = insertion_points.get(final_col_name)
                                    if insert_index is not None:
                                        new_raw_row = [""] * max_raw_row_len
                                        new_raw_row[2] = original_value_lower
                                        raw_data.insert(insert_index, new_raw_row)
                                        
                                        # Кешуємо як кортеж
                                        replacements_map.setdefault(final_col_name, {})[original_value_lower] = (attr_value, "") 
                                        
                                        changes_made = True
                                        new_attributes_counter[final_col_name] = new_attributes_counter.get(final_col_name, 0) + 1
                                        
                                        for k, v in insertion_points.items():
                                            if v >= insert_index: insertion_points[k] += 1
                        else:
                            # Колонки немає ні в мапингу, ні напряму -> "Нові атрибути"
                            other_attributes_list.append(f"{attr_name_site}:{attr_value}")

                    # --- Запис "Нові атрибути" ---
                    if other_attributes_list:
                        new_content = ", ".join(other_attributes_list)
                        if "Нові атрибути" in row:
                            current = row.get("Нові атрибути", "")
                            row["Нові атрибути"] = (current + ", " + new_content) if current else new_content

                    writer.writerow(row)

                except Exception as e:
                    logging.error(f"Помилка URL {product_url}: {e}")
                    writer.writerow(row)

                time.sleep(random.uniform(1, 3))

        os.replace(temp_file_path, supliers_new_path)
        logging.info(f"Парсинг завершено. Товарів: {processed_count}")

        if changes_made:
            save_attributes_csv(raw_data)
            logging.info("===== ЗВІТ ПРО НОВІ ЗНАЧЕННЯ =====")
            total_new = 0
            for attr_block, count in sorted(new_attributes_counter.items()):
                logging.info(f"• {attr_block}: +{count}")
                total_new += count
            logging.info(f"РАЗОМ додано: {total_new}")
            logging.info("==================================")
        else:
            logging.info("Нових значень атрибутів не виявлено.")
        
    except Exception as e:
        logging.error(f"Критична помилка: {e}")
        if os.path.exists(temp_file_path): os.remove(temp_file_path)

# ОСНОВНА ФУНКЦІЯ 5: Стандартизація атрибутів
def apply_final_standardization():
    """
    Проходить по вже сформованому файлу і стандартизує значення згідно з attribute.csv.
    1. Виправляє регістр (наприклад "водна" -> "Водна").
    2. Оновлює російський переклад в сусідній колонці.
    3. Працює на основі назв колонок.
    """
    oc_log_message()
    logging.info("▶ ФУНКЦІЯ 5. Фінальна стандартизація (UA + RU update)...")

    # --- 1. Завантаження налаштувань ---
    settings = load_oc_settings()
    if not settings: return

    csv_path = settings['paths']['csv_path_new_product']
    
    # --- 2. Завантаження правил заміни ---
    # Отримуємо словник: {'Основа|ua': {'водна': ('Водна', 'Водная')}}
    replacements_map, _ = load_attributes_csv()
    
    if not replacements_map:
        logging.warning("⚠️ attribute.csv порожній або не завантажився. Стандартизація пропущена.")
        return

    # --- 3. Підготовка статистики ---
    replacement_counter = {}  # {col_name: count}

    # --- 4. Обробка CSV ---
    temp_file_path = csv_path + '.final_temp'
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as infile, \
             open(temp_file_path, 'w', encoding='utf-8', newline='') as outfile:

            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            
            if not fieldnames:
                logging.error("❌ Файл порожній!")
                return

            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            processed_rows = 0

            for row in reader:
                processed_rows += 1
                row_updated = False

                # Проходимо по всіх колонках, для яких у нас є правила в attribute.csv
                # col_name - це, наприклад, "Основа|ua" або "Колір|ua"
                for col_name, rules in replacements_map.items():
                    
                    # Перевіряємо, чи існує така колонка у файлі
                    if col_name in row:
                        current_value = row[col_name].strip()
                        
                        if not current_value:
                            continue

                        current_value_lower = current_value.lower()
                        
                        # Чи є правило для цього значення?
                        found_pair = rules.get(current_value_lower)

                        if found_pair:
                            # found_pair має вигляд ('Водна', 'Водная')
                            # Захист від старого формату даних
                            if isinstance(found_pair, tuple) and len(found_pair) >= 2:
                                ua_std, ru_std = found_pair
                            else:
                                ua_std = str(found_pair)
                                ru_std = ""

                            # 1. Оновлюємо UA значення (якщо відрізняється)
                            if row[col_name] != ua_std:
                                row[col_name] = ua_std
                                row_updated = True
                                # Логуємо зміну
                                replacement_counter[col_name] = replacement_counter.get(col_name, 0) + 1

                            # 2. Оновлюємо RU значення (сусідня колонка)
                            if ru_std:
                                try:
                                    # Знаходимо індекс UA колонки
                                    ua_idx = fieldnames.index(col_name)
                                    # Беремо наступну колонку (+1)
                                    if ua_idx + 1 < len(fieldnames):
                                        ru_col_name = fieldnames[ua_idx + 1]
                                        
                                        # Оновлюємо RU, якщо воно відрізняється або пусте
                                        if row[ru_col_name] != ru_std:
                                            row[ru_col_name] = ru_std
                                            row_updated = True
                                except ValueError:
                                    pass # Якщо щось пішло не так з індексами

                writer.writerow(row)

        # --- 5. Заміна файлу ---
        os.replace(temp_file_path, csv_path)
        logging.info("Фінальна стандартизація завершена. Файл оновлено.")

        # --- 6. Підсумкове логування ---
        if replacement_counter:
            logging.info("===== ЗВІТ ПРО СТАНДАРТИЗАЦІЮ =====")
            for col, count in sorted(replacement_counter.items()):
                logging.info(f"• {col}: стандартизовано {count} рядків")
            logging.info(f"РАЗОМ змін: {sum(replacement_counter.values())}")
            logging.info("===================================")
        else:
            logging.info("Усі значення вже відповідають стандартам.")

    except Exception as e:
        logging.error(f"Критична помилка при стандартизації: {e}")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

# ОСНОВНА ФУНКЦІЯ 6: Заповнення допоміжних колонок
def fill_auxiliary_columns():
    """
    Адаптовано під структуру БД OpenCart (oc_product, oc_product_description).
    - Ціна без .00
    - Дата зростає на 1 хв для кожного товару
    """
    oc_log_message()
    logging.info("ФУНКЦІЯ: Підготовка колонок для OpenCart...")

    settings = load_oc_settings()
    try:
        csv_path = settings['paths']['csv_path_new_product']
        name_ukr = settings['suppliers'][1]['name_ukr'] 
    except (TypeError, KeyError) as e:
        logging.error(f"Помилка налаштувань: {e}")
        return

    # Завантаження мап категорій
    category_map, cat_fieldnames = load_category_csv()
    poznachky_list = load_poznachky_csv()
    
    new_category_entries = []
    seen_new_keys = set()
    
    # --- ЛОГІКА ДАТИ (Старт о 00:00:00 сьогодні) ---
    # Беремо поточну дату, але час скидаємо на 00:00:00
    base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    temp_path = csv_path + '.oc_temp'

    try:
        with open(csv_path, 'r', encoding='utf-8') as infile, \
             open(temp_path, 'w', encoding='utf-8', newline='') as outfile:

            reader = csv.DictReader(infile)
            fieldnames = list(reader.fieldnames)

            # Додаємо нові OC колонки
            oc_columns = [
                'category', 'Категорія|ua', 'Категория|ru', 
                'Позначки', 
                'stock_status_id','price', 'status', 'subtract', 
                'minimum', 'shipping', 'date_added', 
                'store_id', 'layout_id'
            ]
            
            for col in oc_columns:
                if col not in fieldnames:
                    fieldnames.append(col)

            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            # Використовуємо enumerate, щоб мати індекс рядка (i) для збільшення часу
            for i, row in enumerate(reader):
                
                # --- ЛОГІКА КАТЕГОРІЙ ---
                c1 = row.get("Категория", "").strip()
                c2 = row.get("Доп. Категория 1", "").strip()
                c3 = row.get("Доп. Категория 2", "").strip()
                lookup_key = tuple(sorted([c1.lower(), c2.lower(), c3.lower()]))

                if lookup_key in category_map:
                    data = category_map[lookup_key]
                    row['category']     = data['category']
                    row['Категорія|ua'] = data['cat_ua']
                    row['Категория|ru'] = data['cat_ru']
                else:
                    if any(lookup_key) and lookup_key not in seen_new_keys:
                        new_category_entries.append({
                            'name_1': c1, 'name_2': c2, 'name_3': c3,
                            'category_name': f"{c1} {c2} {c3}".strip(),
                            'category': '', 'Категорія|ua': '', 'Категория|ru': ''
                        })
                        seen_new_keys.add(lookup_key)
                    row['category'] = ''
                    row['Категорія|ua'] = ''
                    row['Категория|ru'] = ''

                # --- ЛОГІКА ПОЗНАЧОК ---
                prod_name = row.get("Название_позиции", "")
                if prod_name and poznachky_list:
                    found = [tag.capitalize() for tag in poznachky_list if tag in prod_name.lower()]
                    row["Позначки"] = ', '.join(sorted(list(set(found)))) if found else ""

                # --- 3. ЗАПОВНЕННЯ OPENCART ДАНИХ ---
                
                row["status"] = "0" 
                row["stock_status_id"] = "7" 
                row["subtract"] = "1"
                row["minimum"] = "1"
                row["shipping"] = "1"

                # === ВИПРАВЛЕННЯ ЦІНИ ===
                try:
                    # 1. Заміна коми на крапку, видалення пробілів
                    raw_price_str = row.get("Цена", "0").replace(',', '.').replace(' ', '')
                    # 2. Перетворення у float (щоб зрозуміти "802.00")
                    price_float = float(raw_price_str)
                    # 3. Перетворення у int (відкидає дробову частину: 802.99 -> 802, 802.00 -> 802)
                    # Якщо вам важливо округляти математично, використовуйте round(price_float)
                    price_int = int(price_float)
                    row["price"] = str(price_int)
                except ValueError:
                    row["price"] = "0"

                # === ВИПРАВЛЕННЯ ДАТИ ===
                # Додаємо i хвилин до базового часу
                # i=0 -> 00:00, i=1 -> 00:01, i=2 -> 00:02...
                row_time = base_date + timedelta(minutes=i)
                row["date_added"] = row_time.strftime('%Y-%m-%d %H:%M:%S')

                row["store_id"] = "0"
                row["layout_id"] = "0"
                row["postachalnyk"] = name_ukr

                writer.writerow(row)

        os.replace(temp_path, csv_path)
        logging.info("Файл оновлено під стандарт OpenCart.")

        if new_category_entries:
            append_new_categories(new_category_entries, cat_fieldnames)

    except Exception as e:
        logging.error(f"Помилка: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)

# ОСНОВНА ФУНКЦІЯ 7: Повторне заповнення категорій
def refill_product_category():
    """
    Повторно проходить по файлу товарів і заповнює колонки категорій
    (category, Категорія|ua, Категория|ru) на основі rules з category.csv.
    
    Використовується, коли в category.csv додали нові правила руками,
    і треба оновити товари без повного перезапуску всього процесу.
    """
    oc_log_message()
    logging.info("ФУНКЦІЯ 7. Починаю повторне заповнення категорій (по назвах)...")

    # --- 1. Завантаження налаштувань ---
    settings = load_oc_settings()
    try:
        csv_path = settings['paths']['csv_path_new_product']
    except (TypeError, KeyError) as e:
        logging.error(f"Помилка доступу до налаштувань: {e}")
        return

    # --- 2. Завантаження правил (використовуємо спільну функцію) ---
    # category_map має структуру: {(key): {'category': '...', 'cat_ua': '...', 'cat_ru': '...'}}
    category_map, _ = load_category_csv()
    
    logging.info(f"Зчитано {len(category_map)} правил категорій.")

    # --- 3. Обробка CSV ---
    temp_path = csv_path + '.refill_temp'
    updated_rows_count = 0
    missing_category_rows = []

    try:
        with open(csv_path, 'r', encoding='utf-8') as infile, \
             open(temp_path, 'w', encoding='utf-8', newline='') as outfile:

            reader = csv.DictReader(infile)
            fieldnames = list(reader.fieldnames)
            
            # Переконуємось, що цільові колонки існують (хоча при refill вони вже мають бути)
            target_cols = ['category', 'Категорія|ua', 'Категория|ru']
            for col in target_cols:
                if col not in fieldnames:
                    fieldnames.append(col)

            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for idx, row in enumerate(reader):
                # --- Формуємо ключ пошуку (так само, як в fill_auxiliary_columns) ---
                c1 = row.get("Категория", "").strip()
                c2 = row.get("Доп. Категория 1", "").strip()
                c3 = row.get("Доп. Категория 2", "").strip()
                
                # Сортуємо, щоб не залежати від порядку слів
                key = tuple(sorted([c1.lower(), c2.lower(), c3.lower()]))

                row_changed = False
                
                # --- Пошук у мапі ---
                if key in category_map:
                    data = category_map[key]
                    
                    # Перевіряємо та оновлюємо ID категорії
                    if row.get('category', '').strip() != data['category']:
                        row['category'] = data['category']
                        row_changed = True
                    
                    # Перевіряємо та оновлюємо UA назву
                    if row.get('Категорія|ua', '').strip() != data['cat_ua']:
                        row['Категорія|ua'] = data['cat_ua']
                        row_changed = True

                    # Перевіряємо та оновлюємо RU назву
                    if row.get('Категория|ru', '').strip() != data['cat_ru']:
                        row['Категория|ru'] = data['cat_ru']
                        row_changed = True
                
                # --- Логування змін ---
                if row_changed:
                    updated_rows_count += 1
                    # logging.info(f"Рядок {idx + 2}: Оновлено категорію для '{c1} {c2}' -> ID: {row['category']}")

                # --- Перевірка на пропуски ---
                # Якщо після всіх маніпуляцій ID категорії все ще порожній - записуємо в помилки
                # (Ігноруємо рядки, де взагалі немає вхідних категорій)
                if any(key) and not row.get('category', '').strip():
                    missing_category_rows.append(idx + 2)

                writer.writerow(row)

        # --- 4. Заміна файлу ---
        os.replace(temp_path, csv_path)
        logging.info(f"Повторне заповнення завершено. Оновлено записів: {updated_rows_count}.")

        # --- 5. Вивід попереджень ---
        if missing_category_rows:
            logging.warning(f"УВАГА: {len(missing_category_rows)} товарів все ще без прив'язки до категорії (ID empty).")
            # Виводимо перші 5 для прикладу, щоб не спамити
            logging.warning(f"Номери рядків (перші 5): {missing_category_rows[:5]} ...")

    except Exception as e:
        logging.error(f"Критична помилка при refill_product_category: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)

# ОСНОВНА ФУНКЦІЯ 8: Знаходження товарів, які вже є в базі (НЕОНОВЛЕНО)
def separate_existing_products():
    """
    Звіряє штрихкоди прайсу 1.csv з базою (oc_zalishki.csv),
    переносить знайдені товари у old_prod_new_SHK.csv,
    видаляє їх з 1.csv та формує підсумкову статистику.
    """
    oc_log_message()
    logging.info("ФУНКЦІЯ 8. Починаю звірку 1.csv зі штрихкодами бази (zalishki.csv)...")

    settings = load_oc_settings()
    try:
        sl_new_path = settings['paths']['csv_path_supliers_1_new']
        zalishki_path = settings['paths']['output_file']
        sl_old_prod_shk_path = settings['paths']['csv_path_sl_old_prod_new_shk']
        column_mapping = settings['suppliers'][1]['column_mapping_sl_old_to_sl_new']
    except KeyError as e:
        logging.error(f"Помилка конфігурації. Не знайдено шлях або мапу колонок: {e}")
        return

    # --- 0. Зчитування існуючого заголовка old_prod_new_SHK.csv ---
    sl_old_header = []
    try:
        if os.path.exists(sl_old_prod_shk_path):
            with open(sl_old_prod_shk_path, mode='r', encoding='utf-8') as f:
                reader = csv.reader(f)
                sl_old_header = next(reader, [])
        else:
            logging.warning("Файл old_prod_new_SHK.csv не знайдено — створюю новий із заголовком за замовчуванням.")
            sl_old_header_base = [
                'id', 'sku', 'Мета: url_lutsk', 'Мета: shtrih_cod', 'Мета: artykul_lutsk', 'Позначки',
                'rank_math_focus_keyword', 'Мета: postachalnyk', 'manage_stock', 'tax_status', 'excerpt'
            ]
            # Додаємо атрибути та додаткові колонки (без attribute_none)
            sl_old_header = sl_old_header_base + [f'attribute_{i}' for i in range(1, 24)] + [
                'content', 'post_date', 'product_type'
            ]

        # Очищаємо файл, але залишаємо заголовок
        with open(sl_old_prod_shk_path, mode='w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(sl_old_header)

        logging.info("Файл old_prod_new_SHK.csv очищено, заголовок залишено без змін.")
    except Exception as e:
        logging.error(f"Помилка при ініціалізації old_prod_new_SHK.csv: {e}")
        return

    # --- 1. Зчитування бази штрихкодів ---
    zalishki_map = {}  # {shk: (id, sku)}
    try:
        with open(zalishki_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # пропускаємо заголовок
            for row in reader:
                if len(row) > 7:
                    shk = row[7].strip()
                    if shk:
                        zalishki_map[shk] = (row[0].strip(), row[1].strip())
        logging.info(f"Зчитано {len(zalishki_map)} унікальних штрихкодів з бази.")
    except Exception as e:
        logging.error(f"Помилка при читанні бази: {e}")
        return

    # --- 2. Обробка 1.csv та формування списків ---
    items_to_keep = []
    items_to_move = []

    try:
        with open(sl_new_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            items_to_keep.append(header)

            for row in reader:
                # Розширюємо рядок до максимального індексу у мапі
                max_index = max(column_mapping.values())
                if len(row) <= max_index:
                    row.extend([''] * (max_index + 1 - len(row)))

                shk_value = row[2].strip()  # C (Штрихкод)
                if shk_value in zalishki_map:
                    item_id, item_sku = zalishki_map[shk_value]

                    # Формуємо новий рядок для old_prod_new_SHK.csv
                    new_row = [''] * len(sl_old_header)
                    new_row[0] = item_id
                    new_row[1] = item_sku

                    for sl_old_idx_str, sl_new_idx in column_mapping.items():
                        sl_old_idx = int(sl_old_idx_str)  # перетворюємо ключ у int
                        if sl_new_idx < len(row):
                            new_row[sl_old_idx] = row[sl_new_idx]

                    items_to_move.append(new_row)
                else:
                    items_to_keep.append(row)

        # --- 3. Запис перенесених товарів ---
        if items_to_move:
            with open(sl_old_prod_shk_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(items_to_move)
            logging.info(f"Перенесено {len(items_to_move)} існуючих товарів у old_prod_new_SHK.csv.")
        else:
            logging.info("Не знайдено жодного товару з існуючим штрихкодом у базі.")

        # --- 4. Запис оновленого 1.csv ---
        temp_path = sl_new_path + '.temp'
        with open(temp_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(items_to_keep)
        os.replace(temp_path, sl_new_path)
        logging.info(f"1.csv оновлено. Залишилось {len(items_to_keep)-1} нових товарів для імпорту.")

    except Exception as e:
        logging.error(f"Непередбачена помилка під час обробки 1.csv: {e}")
        if 'temp_path' in locals() and os.path.exists(temp_path):
            os.remove(temp_path)

# ОСНОВНА ФУНКЦІЯ 9: Нові SKU
def assign_new_sku_to_products():
    """
    Знаходить найбільший SKU у zalishki.csv (шукає в колонці 'sku')
    і присвоює послідовні SKU товарам без SKU у колонці 'sku' файлу 1.csv.
    """
    oc_log_message()
    logging.info("ФУНКЦІЯ 9. Починаю присвоєння нових SKU товарам (по назвах колонок)...")

    # --- 1. Завантаження налаштувань ---
    settings = load_oc_settings()
    try:
        new_product = settings['paths']['csv_path_new_product']
        zalishki = settings['paths']['output_file']
    except KeyError as e:
        logging.error(f"Помилка конфігурації. Не знайдено шлях: {e}")
        return

    # --- 2. Знаходимо максимальний SKU у zalishki.csv ---
    try:
        with open(zalishki, mode='r', encoding='utf-8') as f:
            # Використовуємо DictReader для роботи з назвами колонок
            reader = csv.DictReader(f)
            
            sku_list = []
            for row in reader:
                # Отримуємо значення по назві колонки 'sku'
                val = row.get('sku', '').strip()
                
                if val.isdigit():
                    sku_list.append(int(val))

            if not sku_list:
                logging.warning("У базі не знайдено жодного числового SKU. Присвоєння неможливе.")
                return

            sku_list.sort()
            last_sku = sku_list[-1]
            logging.info(f"Максимальний SKU у базі: {last_sku}")

    except FileNotFoundError:
        logging.error(f"Файл бази zalishki.csv не знайдено за шляхом: {zalishki}")
        return
    except Exception as e:
        logging.error(f"Помилка при читанні zalishki.csv: {e}")
        return

    # --- 3. Присвоєння нових SKU у 1.csv ---
    next_sku = last_sku + 1
    assigned_count = 0
    temp_path = new_product + '.temp'

    try:
        with open(new_product, mode='r', encoding='utf-8', newline='') as input_file:
            reader = csv.DictReader(input_file)
            fieldnames = reader.fieldnames # Зберігаємо заголовки для запису
            
            # Якщо колонки 'sku' немає у файлі, код впаде, тому робимо перевірку
            if 'sku' not in fieldnames:
                 logging.error("У файлі нових товарів відсутня колонка 'sku'.")
                 return

            rows = []
            for row in reader:
                current_sku = row.get('sku', '').strip()
                
                # Якщо SKU порожній, присвоюємо новий
                if not current_sku:
                    row['sku'] = str(next_sku)
                    assigned_count += 1
                    next_sku += 1
                
                rows.append(row)

        # --- 4. Запис оновленого CSV ---
        if assigned_count > 0:
            with open(temp_path, mode='w', encoding='utf-8', newline='') as f:
                # Використовуємо DictWriter для запису словників
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader() # Записуємо заголовок
                writer.writerows(rows) # Записуємо дані
            
            os.replace(temp_path, new_product)
            logging.info(f"✅ Успішно присвоєно {assigned_count} нових SKU. Наступний SKU буде {next_sku}.")
        else:
            logging.info("Усі товари вже мають SKU. Змін не внесено.")

    except FileNotFoundError:
        logging.error(f"Файл нових товарів не знайдено")
    except Exception as e:
        logging.error(f"Непередбачена помилка під час присвоєння SKU: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)

# ОСНОВНА ФУНКЦІЯ 10: Завантаження зображень
def process_phase_1_download():
    """
    ЕТАП 1: 
    1. Очистка папок.
    2. Завантаження фото (з 'url').
    3. Запис списку файлів у 'img_name_jpg'.
    """
    oc_log_message()
    logging.info("🚀 ФАЗА 1. Початок завантаження зображень...")

    settings = load_oc_settings()
    try:
        csv_path = settings['paths']['csv_path_new_product']
        jpg_path = settings['paths']['img_path_jpg']
        webp_path = settings['paths']['img_path_webp']
        cat_map = settings['categories']
    except KeyError as e:
        logging.error(f"❌ Не знайдено шлях у settings.json: {e}")
        return

    # 1️⃣ Очистка
    clear_directory(jpg_path)
    clear_directory(webp_path)
    logging.info("1. ✅ Очистка папок JPG та WEBP завершена.")

    # 2️⃣ Завантаження
    rows = []
    fieldnames = []

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames # Отримуємо список заголовків
        
        # Перевірка, чи існує колонка для запису результату, якщо ні — додаємо
        if 'img_name_jpg' not in fieldnames:
            fieldnames.append('img_name_jpg')

        for row in reader:
            # 👉 ПРЯМЕ ВИКОРИСТАННЯ НАЗВ КОЛОНОК ТУТ:
            url = row.get('url_lutsk', '').strip()
            sku = row.get('sku', '').strip()
            cat = row.get('category', '').strip()

            if url and sku and cat:
                # Завантажуємо зображення
                imgs = download_product_images(url, sku, cat, jpg_path, cat_map)
                
                # Записуємо імена файлів у колонку 'img_name_jpg'
                row['img_name_jpg'] = ', '.join(imgs) if imgs else ''
            
            rows.append(row)
            time.sleep(random.uniform(0.1, 0.5))

    # Збереження оновленого CSV
    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logging.info(f"2. 📥 ФАЗА 1 завершена. Оброблено {len(rows)} рядків.")

def process_phase_2_finish():
    """
    ЕТАП 2:
    1. Переміщення GIF.
    2. Конвертація у WEBP.
    3. Оновлення колонки 'image_name_webp'.
    4. Копіювання на сайт.
    """
    oc_log_message()
    logging.info("⚙️ ФАЗА 2. Початок обробки файлів...")
    
    settings = load_oc_settings()
    try:
        csv_path = settings['paths']['csv_path_new_product']
        jpg_path = settings['paths']['img_path_jpg']
        webp_path = settings['paths']['img_path_webp']
        site_path = settings['paths']['site_path_images']
    except KeyError as e:
        logging.error(f"❌ Помилка налаштувань: {e}")
        return

    # 3️⃣ GIF
    move_gifs(jpg_path, webp_path)
    logging.info("3. ✅ Переміщення GIF завершено.")

    # 4️⃣ WEBP
    convert_to_webp_square(jpg_path, webp_path)
    logging.info("4. ✅ Конвертація JPG у WEBP завершена.")

    # 5️⃣ Оновлення CSV (викликаємо нову функцію синхронізації)
    sync_webp_column_named(csv_path, webp_path)
    logging.info("5. ✅ Оновлення колонки WEBP у CSV завершено.")

    # 6️⃣ На основі колонки 'image_name_webp' заповнюємо шляхи для OpenCart
    # Тут скрипт робить: "catalog/product/.../sku.webp"
    fill_opencart_paths_single_file()  # <--- НОВА ФУНКЦІЯ

    # 7️⃣ Копіювання
    copy_to_site(webp_path, site_path)
    logging.info("6. ✅ Копіювання зображень на сайт завершено.")


    
    logging.info("🏁 ФАЗА 2 завершена успішно.")

    import csv

# ОСНОВНА ФУНКЦІЯ 11: Переклад на рос
def translate_and_prepare_csv():
    """
    Переклад назви і опису з допомогою Deepl
    Також заповнюються допоміжні СЕОполя
    """
    
    oc_log_message() 
    logging.info("🚀 Початок обробки CSV для OpenCart (Повний цикл)...")

    settings = load_oc_settings()
    
    # --- ВИКОРИСТОВУЄМО ОДИН ФАЙЛ ---
    csv_path = settings['paths']['csv_path_new_product']
    api_key = settings['deepl_api_key'] # Якщо закінчаться ляміти на цьому, то треба взяти deepl_api_key2
    api_url = settings['DEEPL_API_URL']

    if not csv_path or not api_key:
        logging.error("❌ Не вказано шлях до файлу (csv_path_new_product) або API ключ")
        return

    # 1. Завантажуємо словник позначок (виклик без аргументів, бо шлях береться з settings всередині функції)
    tags_map = load_poznachky_csv()

    # 2. Перевірка ліміту DeepL
    get_deepl_usage(api_key)

    try:

        # --- КРОК 1: ЧИТАННЯ ФАЙЛУ ---
        rows = []
        fieldnames = []
        
        with open(csv_path, 'r', encoding='utf-8') as f_in:
            reader = csv.DictReader(f_in)
            fieldnames = list(reader.fieldnames) # Копіюємо список заголовків
            rows = list(reader)                  # Читаємо всі дані в пам'ять

        total_rows = len(rows)
        logging.info(f"📦 Зчитано {total_rows} товарів з {csv_path}")

        # --- КРОК 2: ДОДАВАННЯ НОВИХ КОЛОНОК (ЯКЩО ЇХ НЕМАЄ) ---
        required_cols = [
            "name|ru", "description|ru", 
            "meta_title|ua", "meta_title|ru", 
            "meta_keywords|ru", 
            "meta_description|ua", "meta_description|ru"
        ]
        for col in required_cols:
            if col not in fieldnames:
                fieldnames.append(col)

        # --- КРОК 3: ОБРОБКА ДАНИХ ---
        processed_rows = []
        
        for idx, row in enumerate(rows, start=1):
            # --- ЗЧИТУВАННЯ ВИХІДНИХ ДАНИХ ---
            name_ua = row.get("name|ua", "").strip()
            # Обережно з назвою колонки опису (у вас було "Описание")
            desc_ua = row.get("Описание", "").strip() 
            tags_ua_raw = row.get("Позначки", "").strip()
            

            # 1. ПЕРЕКЛАД НАЗВИ (якщо пусто)
            if name_ua and not row.get("name|ru"):
                row["name|ru"] = translate_text_deepl(name_ua, "RU", api_key, api_url)
            
            name_ru = row.get("name|ru", "") # Актуальне значення

            # 2. ПЕРЕКЛАД ОПИСУ (якщо пусто)
            if desc_ua and not row.get("description|ru"):
                # Примітка: is_html не потрібен для нової "розумної" функції
                row["description|ru"] = translate_text_deepl(desc_ua, "RU", api_key, api_url)
            
            desc_ru = row.get("description|ru", "")

            # 3. META TITLE
            # UA: Назва + суфікс
            if name_ua:
                row["meta_title|ua"] = f"{name_ua} 💕 Інтим-Бутік ЕРОС ❱❱ Купити секс іграшки в Україні"
            # RU: Назва RU + суфікс
            if name_ru:
                row["meta_title|ru"] = f"{name_ru} 💕 Интим-Бутик ЕРОС ❱❱ Купить секс игрушки в Украине"

            # 4. META KEYWORDS (RU) - Словник
            if tags_ua_raw:
                # Розбиваємо по комі
                source_tags = [t.strip() for t in tags_ua_raw.split(',') if t.strip()]
                translated_tags = []
                
                for tag in source_tags:
                    # Шукаємо в словнику (lower() для точності)
                    translated_tag = tags_map.get(tag.lower())
                    if translated_tag:
                        translated_tags.append(translated_tag)
                    else:
                        # Якщо перекладу немає - залишаємо оригінал
                        translated_tags.append(tag) 
                
                row["meta_keywords|ru"] = ", ".join(translated_tags)

            # 5. META DESCRIPTION
            # UA
            if desc_ua:
                first_sent_ua = get_first_sentence(desc_ua)
                row["meta_description|ua"] = f"{first_sent_ua} | Низька ціна | Швидка, безкоштовна, анонімна доставка"

            # RU
            if desc_ru:
                first_sent_ru = get_first_sentence(desc_ru)
                row["meta_description|ru"] = f"{first_sent_ru} | Низкая цена | Быстрая, бесплатная, анонимная доставка"

            processed_rows.append(row)

            if idx % 10 == 0:
                logging.info(f"✅ Оброблено {idx}/{total_rows} товарів...")

        # --- КРОК 4: ЗАПИС (ПЕРЕЗАПИС) ФАЙЛУ ---
        with open(csv_path, 'w', encoding='utf-8', newline='') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_rows)
            
        logging.info(f"🎉 Готово! Файл оновлено: {csv_path}")
        
        # Фінальна перевірка ліміту
        get_deepl_usage(api_key)

    except Exception as e:
        logging.error(f"❌ Критична помилка: {e}")