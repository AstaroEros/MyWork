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
                                load_category_csv, save_category_csv, load_poznachky_csv

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
        "Марка/Лінія": "Бренд|ua",
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

def fill_product_category():
    """
    Заповнює службові колонки у csv:
    - Q (категорія) на основі M, N, O
    - T (позначки) на основі назви товару G
    - U (Rank Math) на основі назви товару G
    - AV (pa_used) на основі category.csv
    - V, W, X, Y, AZ фіксованими значеннями
    - Z (короткий опис) з H
    - AX (дата)
    Працює тільки для постачальника з ID=1
    """
    oc_log_message()
    logging.info("ФУНКЦІЯ 5. Починаю заповнення категорії та службових колонок...")

    settings = load_oc_settings()
    try:
        csv_path = settings['paths']['csv_path_supliers_1_new']
        supplier_id = 1
        name_ukr = settings['suppliers']['1']['name_ukr']
    except (TypeError, KeyError) as e:
        logging.error(f"Помилка налаштувань: {e}")
        return

    # Індекси колонок
    M, N, O = 12, 13, 14
    G, H = 6, 7
    Q, T, U = 16, 19, 20
    Z, V, W, X, Y = 25, 21, 22, 23, 24
    AV, AX, AZ = 47, 49, 51

    # Завантаження правил категорій і позначок
    category_map, raw_category = load_category_csv()
    rules_category = category_map.get(supplier_id, {})
    poznachky_list = load_poznachky_csv()
    changes_category = False
    max_row_len_category = len(raw_category[0]) if raw_category else 5

    # Створюємо мапу для pa_used
    pa_used_map = {}
    for row in raw_category:
        if len(row) > 5 and (row[0].strip() == str(supplier_id) or row[0].strip() == ''):
            key = tuple(v.strip().lower() for v in row[1:4])
            pa_used_map[key] = row[5].strip()

    logging.info(f"Завантажено {len(pa_used_map)} правил pa_used")

    current_date = datetime.now().strftime('%Y-%m-%dT00:00:00')

    # Функція для вставки нового рядка у category.csv
    def get_insert_index(supplier_id, raw_data):
        insert_index = len(raw_data)
        found_block = False
        for i, r in enumerate(raw_data):
            if r and r[0].strip().isdigit():
                try:
                    cur_id = int(r[0].strip())
                    if cur_id == supplier_id:
                        found_block = True
                        insert_index = i + 1
                    elif cur_id > supplier_id and found_block:
                        return i
                except ValueError:
                    continue
            elif found_block:
                insert_index = i + 1
        return insert_index

    temp_path = csv_path + '.category_temp'
    try:
        with open(csv_path, 'r', encoding='utf-8') as infile, \
             open(temp_path, 'w', encoding='utf-8', newline='') as outfile:

            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            headers = next(reader)
            writer.writerow(headers)

            for idx, row in enumerate(reader):
                # Розширюємо рядок за потреби
                max_col = max(M, N, O, Q, T, U, V, W, X, Y, Z, AV, AX, AZ, G, H)
                if len(row) <= max_col:
                    row.extend([''] * (max_col + 1 - len(row)))

                product_name = row[G].strip()
                product_desc = row[H]

                key = tuple(row[i].strip().lower() for i in (M, N, O))

                # --- Категорія Q ---
                category_val = rules_category.get(key)
                if category_val is not None:
                    row[Q] = category_val or ""
                else:
                    # Додаємо новий рядок у category.csv
                    insert_idx = get_insert_index(supplier_id, raw_category)
                    new_row = [''] + list(row[M:O+1]) + [''] * (max_row_len_category - 4)
                    raw_category.insert(insert_idx, new_row)
                    rules_category[key] = ""
                    changes_category = True
                    logging.warning(f"Рядок {idx + 2}: Додана нова комбінація категорії {key}")

                # --- Позначки T ---
                if product_name and poznachky_list:
                    found_tags = []
                    covered = []
                    name_lower = product_name.lower()
                    for tag in poznachky_list:
                        if tag in name_lower:
                            start, end = name_lower.find(tag), name_lower.find(tag) + len(tag)
                            if not any(s <= start and end <= e for s, e in covered):
                                found_tags.append(tag.capitalize())
                                covered.append((start, end))
                                covered.sort(key=lambda x: x[1]-x[0], reverse=True)
                    if found_tags:
                        row[T] = ', '.join(found_tags)

                # --- Rank Math U ---
                if product_name:
                    cleaned = re.sub(r'[а-яА-Я0-9]', '', product_name)
                    cleaned = re.sub(r'[^a-zA-Z\s]', '', cleaned)
                    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
                    row[U] = cleaned

                # --- pa_used AV ---
                pa_val = pa_used_map.get(key)
                if pa_val:
                    row[AV] = pa_val


                # --- Фіксовані колонки ---
                row[V] = name_ukr
                row[W] = "draft"
                row[X] = "yes"
                row[Y] = "none"
                row[AZ] = "simple"
                row[AX] = current_date

                # --- Короткий опис Z ---
                if product_desc:
                    row[Z] = product_desc.split('\\n', 1)[0].strip()
                else:
                    row[Z] = ""

                writer.writerow(row)

        os.replace(temp_path, csv_path)
        logging.info("Заповнення категорій та службових колонок завершено.")

        if changes_category:
            save_category_csv(raw_category)
        else:
            logging.info("Збереження category.csv не потрібне. Змін: False.")

    except Exception as e:
        logging.error(f"Помилка при заповненні колонок: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)

def refill_product_category():
    """
    Повторно заповнює колонки Q (Категорія) та AV (pa_used) у 1.csv
    на основі оновлених правил у category.csv.
    НЕ додає нові рядки у category.csv.
    Логування показує, які рядки оновлені.
    """
    oc_log_message()
    logging.info("Функція 6. Починаю повторне заповнення категорій та pa_used у 1.csv...")

    # --- 1. Завантаження налаштувань ---
    settings = load_oc_settings()
    try:
        csv_path = settings['paths']['csv_path_supliers_1_new']
        supplier_id = 1
    except (TypeError, KeyError) as e:
        logging.error(f"Помилка доступу до налаштувань: {e}")
        return

    # --- 2. Індекси колонок CSV ---
    # Використовуємо одразу числа, без довгих змінних
    M, N, O = 12, 13, 14        # name_1, name_2, name_3
    Q, AV = 16, 47              # Категорія та pa_used
    max_index = max(M, N, O, Q, AV)
    missing_category_rows = []  # список рядків з порожньою категорією

    # --- 3. Завантаження правил категорій та pa_used ---
    category_map, raw_category = load_category_csv()
    rules_category = {}
    pa_used_map = {}
    supplier_str = str(supplier_id)

    for row in raw_category:
        if len(row) > 5:
            supplier_value = row[0].strip()
            if supplier_value == supplier_str or supplier_value == '':
                key = tuple(v.strip().lower() for v in row[1:4])  # комбінація M,N,O
                rules_category[key] = row[4].strip() if len(row) > 4 else ""
                pa_used_map[key] = row[5].strip() if len(row) > 5 else ""

    logging.info(f"Зчитано {len(rules_category)} правил для Категорії (Q) та {len(pa_used_map)} правил для pa_used (AV)")

    # --- 4. Обробка CSV ---
    temp_path = csv_path + '.refill_temp'
    updated_rows = 0

    try:
        with open(csv_path, 'r', encoding='utf-8') as infile, \
             open(temp_path, 'w', encoding='utf-8', newline='') as outfile:

            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            headers = next(reader)
            writer.writerow(headers)

            for idx, row in enumerate(reader):
                # Розширюємо рядок, щоб не виходити за межі
                if len(row) <= max_index:
                    row.extend([''] * (max_index + 1 - len(row)))

                # --- 4.1 Ключ пошуку ---
                key = tuple(row[i].strip().lower() for i in (M, N, O))
                initial_category = row[Q].strip()
                initial_pa_used = row[AV].strip()
                row_changed = False

                # --- 4.2 Повторне заповнення Категорії Q ---
                category_val = rules_category.get(key)
                if category_val and category_val != initial_category:
                    row[Q] = category_val
                    row_changed = True
                    logging.info(f"Рядок {idx + 2}: Q (Категорія) оновлено. Ключ: {key}, Значення: '{category_val}'")

                # --- 4.3 Повторне заповнення pa_used AV ---
                pa_val = pa_used_map.get(key)
                if pa_val and pa_val != initial_pa_used:
                    row[AV] = pa_val
                    row_changed = True
                    logging.info(f"Рядок {idx + 2}: AV (pa_used) оновлено. Ключ: {key}, Значення: '{pa_val}'")

                # Перевірка порожньої категорії після оновлення
                if not row[Q].strip():
                    missing_category_rows.append(idx + 2)  # зберігаємо номер рядка у файлі

                if row_changed:
                    updated_rows += 1

                writer.writerow(row)

        # --- 5. Замінюємо оригінальний CSV ---
        os.replace(temp_path, csv_path)
        logging.info(f"Повторне заповнення завершено. Оновлено {updated_rows} рядків.")

        # --- Логування рядків з порожньою категорією ---
        for row_num in missing_category_rows:
            logging.warning(f"УВАГА рядок {row_num} не заповнена категорія!")

    except Exception as e:
        logging.error(f"Непередбачена помилка при повторному заповненні: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)

def separate_existing_products():
    """
    Звіряє штрихкоди 1.csv з базою (zalishki.csv),
    переносить знайдені товари у old_prod_new_SHK.csv,
    видаляє їх з 1.csv та формує підсумкову статистику.
    Колонки та відповідності old -> new винесені у settings.json.
    """
    oc_log_message()
    logging.info("ФУНКЦІЯ 7. Починаю звірку 1.csv зі штрихкодами бази (zalishki.csv)...")

    settings = load_oc_settings()
    try:
        sl_new_path = settings['paths']['csv_path_supliers_1_new']
        zalishki_path = settings['paths']['output_file']
        sl_old_prod_shk_path = settings['paths']['csv_path_sl_old_prod_new_shk']
        column_mapping = settings['suppliers']['1']['column_mapping_sl_old_to_sl_new']
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

def assign_new_sku_to_products():
    """
    Знаходить найбільший SKU у zalishki.csv (сортує по колонці B(1))
    і присвоює послідовні SKU товарам без SKU у колонці P(15) файлу 1.csv.
    """
    oc_log_message()
    logging.info("ФУНКЦІЯ 8. Починаю присвоєння нових SKU товарам у 1.csv...")

    # --- 1. Завантаження налаштувань ---
    settings = load_oc_settings()
    try:
        sl_new_path = settings['paths']['csv_path_supliers_1_new']
        zalishki_path = settings['paths']['output_file']
    except KeyError as e:
        logging.error(f"Помилка конфігурації. Не знайдено шлях: {e}")
        return

    # --- 2. Визначення індексу SKU у 1.csv ---
    SKU_COL_INDEX = 15  # P
    ZALISHKI_SKU_INDEX = 1  # B

    # --- 3. Знаходимо максимальний SKU у zalishki.csv ---
    try:
        with open(zalishki_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # пропускаємо заголовок
            sku_list = []
            for row in reader:
                if len(row) > ZALISHKI_SKU_INDEX:
                    val = row[ZALISHKI_SKU_INDEX].strip()
                    if val.isdigit():
                        sku_list.append(int(val))

            if not sku_list:
                logging.warning("У базі не знайдено жодного числового SKU. Присвоєння неможливе.")
                return

            sku_list.sort()
            last_sku = sku_list[-1]
            logging.info(f"Максимальний SKU у базі: {last_sku}")

    except FileNotFoundError:
        logging.error(f"Файл бази zalishki.csv не знайдено за шляхом: {zalishki_path}")
        return
    except Exception as e:
        logging.error(f"Помилка при читанні zalishki.csv: {e}")
        return

    # --- 4. Присвоєння нових SKU у 1.csv ---
    next_sku = last_sku + 1
    assigned_count = 0
    temp_path = sl_new_path + '.temp'

    try:
        with open(sl_new_path, mode='r', encoding='utf-8', newline='') as input_file:
            reader = csv.reader(input_file)
            header = next(reader, None)
            rows = [header] if header else []

            for row in reader:
                if len(row) <= SKU_COL_INDEX:
                    row.extend([''] * (SKU_COL_INDEX + 1 - len(row)))

                current_sku = row[SKU_COL_INDEX].strip()
                if not current_sku:
                    row[SKU_COL_INDEX] = str(next_sku)
                    assigned_count += 1
                    next_sku += 1

                rows.append(row)

        # --- 5. Запис оновленого CSV ---
        if assigned_count > 0:
            with open(temp_path, mode='w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(rows)
            os.replace(temp_path, sl_new_path)
            logging.info(f"✅ Успішно присвоєно {assigned_count} нових SKU. Наступний SKU буде {next_sku}.")
        else:
            logging.info("Усі товари вже мають SKU. Змін не внесено.")

    except FileNotFoundError:
        logging.error(f"Файл 1.csv не знайдено за шляхом")
    except Exception as e:
        logging.error(f"Непередбачена помилка під час присвоєння SKU: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)