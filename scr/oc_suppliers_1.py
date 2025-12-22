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

def find_change_art_shtrihcod():
    """
    Перевіряє розбіжності:
    1) по штрихкоду (порівняння по артикулу)
    2) по артикулу (порівняння по штрихкоду)
    Усі розбіжності записує у change_art_shtrihcod
    """

    oc_log_message("▶ Старт перевірки артикулів і штрихкодів (2 напрямки)")
    logging.info("find_change_art_shtrihcod START")

    settings = load_oc_settings()
    if not settings:
        oc_log_message("❌ settings.json не завантажено")
        return

    site_csv = settings["paths"]["output_file"]
    supplier_csv = settings["suppliers"]["1"]["csv_path"]
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

    oc_log_message(
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

    oc_log_message("🧹 change_art_shtrihcod очищено")

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
    oc_log_message(f"✅ Перевірено позицій: {checked}")
    oc_log_message(f"⚠ Знайдено розбіжностей: {diff_count}")
    logging.info("find_change_art_shtrihcod END")

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
    
    # --- 3. Отримання шляхів до потрібних файлів ---
    zalishki_path = settings['paths']['output_file']                   # База існуючих товарів
    supliers_new_path = settings['paths']['csv_path_supliers_1_new']         # Файл, куди буде записано нові товари
    supliers_csv_path = settings['suppliers']['1']['csv_path']               # Прайс-лист постачальника 1
    delimiter = settings['suppliers']['1']['delimiter']                      # Роздільник у CSV
    
    # --- 4. Отримання допоміжних параметрів постачальника ---
    sku_prefix = settings['suppliers']['1']['search']                        # Префікс для пошуку
       
    # --- 5. Отримання структури заголовків нового файлу ---
    new_product_headers = [
        settings['column_supliers_1_new_name'][str(i)]
        for i in range(len(settings['column_supliers_1_new_name']))
    ]
    num_new_columns = len(new_product_headers)

    logging.info("Зчитую існуючі артикули з файлу, вказаного за ключем 'csv_path_zalishki'.")

    try:
        # --- 6. Зчитування існуючих артикулів із бази (zalishki.csv) ---
        with open(zalishki_path, mode='r', encoding='utf-8') as zalishki_file:
            zalishki_reader = csv.reader(zalishki_file)
            next(zalishki_reader, None)  # пропускаємо заголовок
            existing_skus = {row[9].strip().lower() for row in zalishki_reader if len(row) > 9}
            logging.info(f"Зчитано {len(existing_skus)} унікальних артикулів із бази.")

        # --- 7. Підготовка нового файлу для запису нових товарів ---
        logging.info("Відкриваю файл для запису нових товарів...")
        with open(supliers_new_path, mode='w', encoding='utf-8', newline='') as new_file:
            writer = csv.writer(new_file)
            writer.writerow(new_product_headers)  # записуємо заголовки
            
            # --- 8. Зчитування прайс-листа постачальника ---
            logging.info("Порівнюю дані з прайс-листом постачальника 1...")
            with open(supliers_csv_path, mode='r', encoding='utf-8') as supliers_file:
                supliers_reader = csv.reader(supliers_file, delimiter=delimiter)
                next(supliers_reader, None)  # пропускаємо заголовок
                
                # --- 9. Ініціалізація лічильника ---
                new_products_count = 0

                # --- 10. Головний цикл: перевірка кожного товару ---
                for row in supliers_reader:
                    if not row:
                        continue
                    
                    sku = row[0].strip().lower()
                    
                    # --- 11. Перевіряємо, чи товар новий (відсутній у базі) ---
                    if sku and sku not in existing_skus:
                        
                        # --- 12. Формуємо новий рядок за структурою SL_new.csv ---
                        new_row = [''] * num_new_columns
                        
                        # Додаємо префікс до SKU
                        sku_with_prefix = sku_prefix + row[0]
                        new_row[0] = sku_with_prefix

                        # --- 13. Мапування колонок з прайсу у новий CSV ---
                        column_mapping = [
                            (18, 2),  # s(18) -> с(2)
                            (0, 5),   # a(0) -> f(5)
                            (1, 6),   # b(1) -> g(6)
                            (2, 7),   # c(2) -> h(7)
                            (3, 8),   # d(3) -> i(8)
                            (6, 9),   # g(6) -> j(9)
                            (7, 10),  # h(7) -> k(10)
                            (8, 11),  # i(8) -> l(11)
                            (9, 12),  # j(9) -> m(12)
                            (10, 13), # k(10) -> n(13)
                            (11, 14), # l(11) -> o(14)
                        ]
                        for source_index, dest_index in column_mapping:
                            if len(row) > source_index:
                                new_row[dest_index] = row[source_index]
                                
                        # --- 14. Додаємо у файл нових товарів ---
                        new_products_count += 1
                        writer.writerow(new_row)

        # --- 17. Підсумкове логування ---
        logging.info(f"✅ Знайдено {new_products_count} нових товарів.")
        logging.info(f"Дані записано у файл csv 'supliers_new_path'.")

    # --- 18. Обробка помилок ---
    except FileNotFoundError as e:
        logging.info(f"❌ Помилка: Файл не знайдено - {e}")
    except Exception as e:
        logging.info(f"❌ Виникла непередбачена помилка: {e}")

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
    supliers_new_path = settings['paths']['csv_path_supliers_1_new']  # вхідний CSV (1.csv)
    site_url = settings['suppliers']['1']['site']                     # базовий URL сайту (щоб додавати відносні посилання)
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
            reader = csv.reader(input_file)
            headers = next(reader)  # читаємо і зберігаємо заголовки (щоб переписати в тимчасовий файл)

            # --- 4. Відкриваємо тимчасовий файл для поступового запису результатів ---
            with open(temp_file_path, mode='w', encoding='utf-8', newline='') as output_file:
                writer = csv.writer(output_file)
                writer.writerow(headers) # записуємо заголовки у тимчасовий файл

                # --- 5. Ітерація по рядках вхідного файлу ---
                for idx, row in enumerate(reader):
                    total_rows += 1
                    # 5.1. Витягуємо ключові поля із рядка
                    search_url = row[0].strip()    # у вихідному файлі у колонці A може бути "посилання для пошуку"
                    file_sku = row[5].strip()      # артикул (SKU) з колонки, яка відповідає індексу 5

                    # --- 6. Перевірка валідності URL для пошуку ---
                    # Якщо URL пустий або вже позначений як помилка запиту, пропускаємо рядок
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

                        # --- 10. Запис результату у колонку B (індекс 1) або логування якщо не знайдено ---
                        if found_url:
                            row[1] = found_url
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
                        # --- 11. Обробка помилок HTTP-запиту: логування та маркування рядка ---
                        logging.error(f"Рядок {idx + 2}: Помилка при запиті до урл: {e}")
                        row[0] = f'Помилка запиту: {e}'  # позначаємо поле пошуку як помилкове
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

def parse_product_attributes():
    """
    Парсить сторінки товарів, застосовує заміну з attribute.csv (блочна структура)
    і додає нові невідомі значення одразу перед наступним блоком-заголовком.

    """

    # --- 0. Лог старту ---
    oc_log_message("▶ ФУНКЦІЯ: Парсинг атрибутів товарів (без штрих-кодів)")
    logging.info("Починаю парсинг сторінок товарів для вилучення атрибутів")

    # --- 1. Завантаження налаштувань ---
    settings = load_oc_settings()
    try:
        supliers_new_path = settings["paths"]["csv_path_supliers_1_new"]
        product_data_map = settings["suppliers"]["1"]["product_data_columns"]
        other_attrs_index = settings["suppliers"]["1"]["other_attributes_column"]
    except (TypeError, KeyError) as e:
        logging.error(f"Помилка доступу до налаштувань settings.json: {e}")
        return

    # --- 2. Мапа оброблюваних атрибутів (без штрих-коду) ---
    processing_map = {
        attr_name: col_index
        for attr_name, col_index in product_data_map.items()
        if attr_name != "Штрих-код"
    }

    # --- 3. Завантаження attribute.csv ---
    replacements_map, raw_data = load_attributes_csv()
    changes_made = False
    max_raw_row_len = len(raw_data[0]) if raw_data and raw_data[0] else 10

    # --- 4. Підготовка точок вставки для нових атрибутів ---
    insertion_points = {}
    current_col_index = None

    for i, row in enumerate(raw_data[1:], start=1):
        if row and row[0].strip().isdigit():
            col_index = int(row[0].strip())
            if current_col_index is not None and current_col_index not in insertion_points:
                insertion_points[current_col_index] = i
            current_col_index = col_index
            insertion_points[col_index] = i + 1
        elif current_col_index is not None:
            insertion_points[current_col_index] = i + 1

    logging.debug(f"Точки вставки attribute.csv: {insertion_points}")

    # --- 5. Лічильник нових атрибутів ---
    new_attributes_counter = {}  # {col_index: count}

    # --- 6. Обробка CSV постачальника ---
    temp_file_path = supliers_new_path + ".temp"

    try:
        with open(supliers_new_path, mode="r", encoding="utf-8") as input_file, \
             open(temp_file_path, mode="w", encoding="utf-8", newline="") as output_file:

            reader = csv.reader(input_file)
            writer = csv.writer(output_file)

            headers = next(reader)
            writer.writerow(headers)

            for idx, row in enumerate(reader, start=2):  # start=2 → з урахуванням заголовка
                product_url = row[1].strip() if len(row) > 1 else ""

                # --- Розширення рядка при необхідності ---
                max_index = max(
                    max(product_data_map.values(), default=0),
                    other_attrs_index
                )
                if len(row) <= max_index:
                    row.extend([""] * (max_index + 1 - len(row)))

                # --- Пропуск некоректних URL ---
                if not product_url or product_url.startswith("Помилка запиту"):
                    writer.writerow(row)
                    continue

                try:
                    # --- 6.1 Запит сторінки ---
                    response = requests.get(product_url, timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "html.parser")

                    characteristics_div = soup.find("div", id="w0-tab0")
                    parsed_attributes = {}

                    if characteristics_div and characteristics_div.find("table"):
                        for tr in characteristics_div.find("table").find_all("tr"):
                            cells = tr.find_all("td")
                            if len(cells) == 2:
                                key = cells[0].get_text(strip=True).replace(":", "")
                                value = cells[1].get_text(strip=True)
                                parsed_attributes[key] = value

                    other_attributes = []

                    # --- 6.2 Обробка атрибутів ---
                    for attr_name, attr_value in parsed_attributes.items():

                        # ❗ Повністю ігноруємо штрих-код
                        if attr_name == "Штрих-код":
                            continue

                        target_col_index = processing_map.get(attr_name)
                        original_value_lower = attr_value.strip().lower()

                        if target_col_index is not None:
                            replacement_rules = replacements_map.get(target_col_index, {})
                            new_value = replacement_rules.get(original_value_lower)

                            if new_value:
                                row[target_col_index] = new_value
                            else:
                                if original_value_lower not in replacement_rules:
                                    insert_index = insertion_points.get(target_col_index)

                                    if insert_index is None:
                                        logging.error(
                                            f"Атрибут '{attr_value}' (I={target_col_index}) "
                                            f"не додано: відсутня точка вставки"
                                        )
                                        row[target_col_index] = attr_value
                                        continue

                                    new_raw_row = [""] * max_raw_row_len
                                    new_raw_row[2] = original_value_lower
                                    raw_data.insert(insert_index, new_raw_row)

                                    replacements_map.setdefault(
                                        target_col_index, {}
                                    )[original_value_lower] = ""

                                    changes_made = True

                                    # Зсув точок вставки
                                    for col, point in insertion_points.items():
                                        if point >= insert_index:
                                            insertion_points[col] += 1

                                    new_attributes_counter[target_col_index] = (
                                        new_attributes_counter.get(target_col_index, 0) + 1
                                    )

                                row[target_col_index] = attr_value
                        else:
                            other_attributes.append(f"{attr_name}:{attr_value}")

                    if other_attributes:
                        row[other_attrs_index] = ", ".join(other_attributes)

                    writer.writerow(row)

                except requests.RequestException as req_err:
                    logging.error(f"Помилка запиту URL {product_url}: {req_err}")
                    writer.writerow(row)

                except Exception as e:
                    logging.error(f"Помилка парсингу URL {product_url}: {e}")
                    writer.writerow(row)

                time.sleep(random.uniform(1, 3))

        # --- 7. Заміна файлу ---
        os.replace(temp_file_path, supliers_new_path)
        logging.info("Парсинг атрибутів завершено. CSV постачальника оновлено.")

        # --- 8. Збереження attribute.csv ---
        if changes_made:
            save_attributes_csv(raw_data)
        else:
            logging.info("attribute.csv не змінювався.")

        # --- 9. Підсумкове логування ---
        if new_attributes_counter:
            logging.info("Додані нові атрибути:")
            for col_index, count in sorted(new_attributes_counter.items()):
                logging.info(f"Колонка {col_index}: +{count}")
        else:
            logging.info("Нових атрибутів не додано.")

    except Exception as e:
        logging.error(f"Критична помилка виконання: {e}")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

def apply_final_standardization():
    """
    Застосовує фінальні правила стандартизації з attribute.csv до файлу 1.csv.
    Замінює атрибути на значення з колонки 'attr_site_name', якщо воно існує.
    Проігноровані атрибути (з порожнім 'attr_site_name') очищаються.
    Атрибути, для яких не знайдено правил, залишаються без змін.
    Логування включає інформацію про кількість замін та очищень.
    """
    oc_log_message()
    logging.info("ФУНКЦІЯ 4. Починаю фінальну стандартизацію атрибутів у 1.csv...")

    # --- 1. Завантаження налаштувань ---
    settings = load_oc_settings()
    try:
        csv_path = settings['paths']['csv_path_supliers_1_new']
        product_map = settings['suppliers']['1']['product_data_columns']
    except TypeError as e:
        logging.error(f"Помилка доступу до налаштувань: {e}")
        return

    # --- 2. Підготовка мапи для обробки (без Штрих-коду) ---
    processing_map = {k: v for k, v in product_map.items() if k != "Штрих-код"}

    # --- 3. Завантаження правил заміни ---
    replacements_map, _ = load_attributes_csv()

    # --- 4. Підготовка статистики замін ---
    replacement_counter = {}  # {col_index: count}
    cleared_counter = {}      # {col_index: count}

    # --- 5. Обробка CSV ---
    temp_file_path = csv_path + '.final_temp'
    try:
        with open(csv_path, 'r', encoding='utf-8') as infile, \
             open(temp_file_path, 'w', encoding='utf-8', newline='') as outfile:

            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            headers = next(reader)
            writer.writerow(headers)

            # Словник для логування назв колонок
            column_names = {index: name for name, index in processing_map.items()}

            for idx, row in enumerate(reader):
                max_index = max(product_map.values(), default=0)
                if len(row) <= max_index:
                    row.extend([''] * (max_index + 1 - len(row)))

                for col_index, rules in replacements_map.items():
                    if col_index >= len(row):
                        continue

                    current_value = row[col_index].strip()
                    if not current_value:
                        continue

                    current_lower = current_value.lower()
                    col_name = column_names.get(col_index, f"I={col_index}")
                    new_value = rules.get(current_lower)

                    if new_value is not None:
                        if new_value:
                            if new_value != current_value:
                                row[col_index] = new_value
                                replacement_counter[col_index] = replacement_counter.get(col_index, 0) + 1
                                logging.info(f"Рядок {idx + 2}: ЗАМІНА ({col_name}): '{current_value}' -> '{new_value}'")
                        else:
                            row[col_index] = ""
                            cleared_counter[col_index] = cleared_counter.get(col_index, 0) + 1
                            logging.warning(f"Рядок {idx + 2}: ІГНОРУВАННЯ/ОЧИЩЕННЯ ({col_name}): '{current_value}' очищено")

                writer.writerow(row)

        os.replace(temp_file_path, csv_path)
        logging.info("Фінальна стандартизація завершена. csv оновлено.")

        # --- 6. Підсумкове логування ---
        if replacement_counter:
            for col, count in sorted(replacement_counter.items()):
                logging.info(f"Атрибут {col}: виконано {count} замін")
        if cleared_counter:
            for col, count in sorted(cleared_counter.items()):
                logging.info(f"Атрибут {col}: очищено {count} значень")

    except FileNotFoundError as e:
        logging.error(f"Файл не знайдено: {e}")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
    except Exception as e:
        logging.error(f"Непередбачена помилка при стандартизації: {e}")
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