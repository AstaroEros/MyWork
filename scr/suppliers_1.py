import csv
import os
import time
import requests
import shutil
import re
import pandas as pd
import mimetypes
from bs4 import BeautifulSoup
import random 
from PIL import Image
import logging
from typing import Dict, Tuple, List, Optional, Any
from scr.base_function import get_wc_api, load_settings, setup_new_log_file, log_message_to_existing_file, load_attributes_csv, \
                                save_attributes_csv, load_category_csv, save_category_csv, load_poznachky_csv, \
                                _process_batch_update, find_media_ids_for_sku, _process_batch_create, clear_directory, \
                                download_product_images, move_gifs, convert_to_webp_square, sync_webp_column, copy_to_site
from datetime import datetime, timedelta


def find_new_products():
    """
    Порівнює артикули товарів з прайс-листа постачальника з артикулами,
    що є на сайті, і записує нові товари в окремий файл.
    """
    # --- 1. Ініціалізація логування ---
    log_message_to_existing_file()
    logging.info("ФУНКЦІЯ 1. Починаю пошук нових товарів...")
    
    # --- 2. Завантаження налаштувань з settings.json ---
    settings = load_settings()
    
    # --- 3. Отримання шляхів до потрібних файлів ---
    zalishki_path = settings['paths']['csv_path_zalishki']                   # База існуючих товарів
    supliers_new_path = settings['paths']['csv_path_supliers_1_new']         # Файл, куди буде записано нові товари
    supliers_csv_path = settings['suppliers']['1']['csv_path']               # Прайс-лист постачальника 1
    delimiter = settings['suppliers']['1']['delimiter']                      # Роздільник у CSV
    
    # --- 4. Отримання допоміжних параметрів постачальника ---
    sku_prefix = settings['suppliers']['1']['search']                        # Префікс для пошуку
    bad_words = [word.lower() for word in settings['suppliers']['1'].get('bad_words', [])]  # Заборонені слова (фільтр)
    
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
                
                # --- 9. Ініціалізація лічильників ---
                new_products_count = 0
                filtered_out_count = 0

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
                                
                        # --- 14. Фільтрація заборонених слів ---
                        should_skip = False
                        check_columns_indices = [6, 7, 10]  # колонки, де шукаємо заборонені слова
                        
                        for index in check_columns_indices:
                            if len(new_row) > index:
                                cell_content = new_row[index].lower()
                                for bad_word in bad_words:
                                    if bad_word in cell_content:
                                        logging.info(
                                            f"Пропускаю товар '{row[0]}' через слово '{bad_word}' "
                                            f"в колонці {index} нового файлу."
                                        )
                                        should_skip = True
                                        filtered_out_count += 1
                                        break
                                if should_skip:
                                    break
                        
                        # --- 15. Якщо товар має заборонене слово — пропускаємо ---
                        if should_skip:
                            continue
                        
                        # --- 16. Якщо ні — додаємо у файл нових товарів ---
                        new_products_count += 1
                        writer.writerow(new_row)

        # --- 17. Підсумкове логування ---
        logging.info(f"✅ Знайдено {new_products_count} нових товарів.")
        logging.info(f"🚫 Відфільтровано {filtered_out_count} товарів за забороненими словами.")
        logging.info(f"Дані записано у файл csv 'supliers_new_path'.")

    # --- 18. Обробка помилок ---
    except FileNotFoundError as e:
        logging.info(f"❌ Помилка: Файл не знайдено - {e}")
    except Exception as e:
        logging.info(f"❌ Виникла непередбачена помилка: {e}")

def find_product_data():
    """
    Зчитує файл з новими товарами, переходить за URL-адресою,
    знаходить URL-адресу простого або варіативного товару,
    і записує знайдену URL-адресу в колонку B(1) в тимчасовий файл.
    """

    # --- 1. Ініціалізація логування (підключаємо існуючий лог-файл) ---
    log_message_to_existing_file()
    logging.info("ФУНКЦІЯ 2. Починаю пошук URL-адрес товарів...")
    
    # --- 2. Завантаження налаштувань та формування шляхів/тимчасового файлу ---
    settings = load_settings()
    supliers_new_path = settings['paths']['csv_path_supliers_1_new']  # вхідний CSV (1.csv)
    site_url = settings['suppliers']['1']['site']                    # базовий URL сайту (щоб додавати відносні посилання)
    temp_file_path = supliers_new_path + '.temp'                     # тимчасовий файл під час запису

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
    Логування включає підсумок доданих атрибутів по колонках.
    """
    log_message_to_existing_file()
    logging.info("ФУНКЦІЯ 3. Починаю парсинг сторінок товарів для вилучення атрибутів...")

    # --- 1. Завантаження налаштувань ---
    settings = load_settings()
    try:
        supliers_new_path = settings['paths']['csv_path_supliers_1_new']
        product_data_map = settings['suppliers']['1']['product_data_columns']
        other_attrs_index = settings['suppliers']['1']['other_attributes_column']
    except (TypeError, KeyError) as e:
        logging.error(f"Помилка доступу до налаштувань. Перевірте settings.json: {e}")
        return

    # --- 2. Підготовка мапи для обробки (без Штрих-коду) ---
    processing_map = {k: v for k, v in product_data_map.items() if k != "Штрих-код"}

    # --- 3. Завантаження правил заміни та сирих даних ---
    replacements_map, raw_data = load_attributes_csv()
    changes_made = False
    max_raw_row_len = len(raw_data[0]) if raw_data and raw_data[0] else 10

    # --- 4. Підготовка точок вставки нових атрибутів ---
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

    logging.debug(f"Точки вставки (insertion_points): {insertion_points}")

    # --- 5. Словник для підрахунку нових атрибутів по колонках ---
    new_attributes_counter = {}  # {col_index: count}

    # --- 5.1 Список для контролю рядків без штрихкодів ---
    missing_shk_rows = []  # [рядок_у_csv]

    # --- 6. Обробка CSV постачальника ---
    temp_file_path = supliers_new_path + '.temp'
    try:
        with open(supliers_new_path, mode='r', encoding='utf-8') as input_file, \
             open(temp_file_path, mode='w', encoding='utf-8', newline='') as output_file:

            reader = csv.reader(input_file)
            writer = csv.writer(output_file)
            headers = next(reader)
            writer.writerow(headers)

            for idx, row in enumerate(reader):
                product_url = row[1].strip()
                file_sku = row[5].strip()

                # Розширення рядка, якщо потрібно
                max_index = max(max(product_data_map.values(), default=0), other_attrs_index)
                if len(row) <= max_index:
                    row.extend([''] * (max_index + 1 - len(row)))

                if not product_url or product_url.startswith('Помилка запиту'):
                    writer.writerow(row)
                    continue

                # --- 6.1 Парсинг сторінки ---
                try:
                    response = requests.get(product_url)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')
                    characteristics_div = soup.find('div', id='w0-tab0')
                    parsed_attributes = {}
                    if characteristics_div and characteristics_div.find('table'):
                        for tr in characteristics_div.find('table').find_all('tr'):
                            cells = tr.find_all('td')
                            if len(cells) == 2:
                                key = cells[0].get_text(strip=True).replace(':', '')
                                value = cells[1].get_text(strip=True)
                                parsed_attributes[key] = value

                    other_attributes = []

                    # --- 6.2 Обробка атрибутів ---
                    for attr_name, attr_value in parsed_attributes.items():
                        target_col_index = processing_map.get(attr_name)
                        original_value_lower = attr_value.strip().lower()

                        if target_col_index is not None:
                            replacement_rules = replacements_map.get(target_col_index, {})
                            new_value = replacement_rules.get(original_value_lower)

                            if new_value is not None and new_value != "":
                                row[target_col_index] = new_value
                            else:
                                if original_value_lower not in replacement_rules:
                                    insert_index = insertion_points.get(target_col_index)
                                    if insert_index is None:
                                        logging.error(f"Атрибут '{attr_value}' (I={target_col_index}) не додано: відсутня точка вставки.")
                                        row[target_col_index] = attr_value
                                        continue

                                    # Додаємо новий атрибут у raw_data
                                    new_raw_row = [''] * max_raw_row_len
                                    new_raw_row[2] = original_value_lower
                                    raw_data.insert(insert_index, new_raw_row)
                                    replacements_map.setdefault(target_col_index, {})[original_value_lower] = ""
                                    changes_made = True

                                    # Зсуваємо точки вставки
                                    for col, point in insertion_points.items():
                                        if point >= insert_index:
                                            insertion_points[col] += 1

                                    # Підрахунок нових атрибутів
                                    new_attributes_counter[target_col_index] = new_attributes_counter.get(target_col_index, 0) + 1

                                row[target_col_index] = attr_value

                        elif attr_name == "Штрих-код":
                            shk_index = product_data_map.get("Штрих-код")
                            if shk_index is not None:
                                row[shk_index] = attr_value.strip()

                        else:
                            other_attributes.append(f"{attr_name}:{attr_value}")

                    # --- 6.3 Перевірка наявності штрихкоду ---
                    shk_index = product_data_map.get("Штрих-код")
                    if shk_index is not None:
                        if not row[shk_index].strip():
                            missing_shk_rows.append(idx + 2)  # +2, бо заголовок = рядок 1

                    if other_attributes:
                        row[other_attrs_index] = ', '.join(other_attributes)

                    writer.writerow(row)

                except requests.RequestException as req_err:
                    logging.error(f"Помилка запиту для URL {product_url}: {req_err}")
                    writer.writerow(row)
                except Exception as e:
                    logging.error(f"Непередбачена помилка парсингу для URL {product_url}: {e}")
                    writer.writerow(row)

                time.sleep(random.uniform(1, 3))

        os.replace(temp_file_path, supliers_new_path)
        logging.info("Парсинг атрибутів завершено. Файл 1.csv оновлено.")

        # --- 7. Збереження attribute.csv та підсумковий лог ---
        if changes_made:
            save_attributes_csv(raw_data)
        else:
            logging.info("Збереження attribute.csv не потрібне. Змін: False.")

        # --- 7.1 Підсумкове логування нових атрибутів ---
        if new_attributes_counter:
            logging.info("Підсумок доданих нових атрибутів по колонках:")
            for col_index, count in sorted(new_attributes_counter.items()):
                logging.info(f"Атрибут {col_index}, додано {count} нових атрибутів")
        else:
            logging.info("Нові атрибути не додані у жодну колонку.")

        # --- 7.2 Логування відсутніх штрихкодів ---
        if missing_shk_rows:
            rows_str = ', '.join(map(str, missing_shk_rows))
            logging.warning(f"УВАГА! Немає штрихкодів: {len(missing_shk_rows)} штуки (рядки {rows_str})")
        else:
            logging.info("Усі товари мають штрихкоди.")

    except Exception as e:
        logging.error(f"Виникла непередбачена помилка: {e}")
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
    log_message_to_existing_file()
    logging.info("ФУНКЦІЯ 4. Починаю фінальну стандартизацію атрибутів у 1.csv...")

    # --- 1. Завантаження налаштувань ---
    settings = load_settings()
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
    log_message_to_existing_file()
    logging.info("ФУНКЦІЯ 5. Починаю заповнення категорії та службових колонок...")

    settings = load_settings()
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
                row[W] = "0"
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
    log_message_to_existing_file()
    logging.info("Функція 6. Починаю повторне заповнення категорій та pa_used у 1.csv...")

    # --- 1. Завантаження налаштувань ---
    settings = load_settings()
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
    log_message_to_existing_file()
    logging.info("ФУНКЦІЯ 7. Починаю звірку 1.csv зі штрихкодами бази (zalishki.csv)...")

    settings = load_settings()
    try:
        sl_new_path = settings['paths']['csv_path_supliers_1_new']
        zalishki_path = settings['paths']['csv_path_zalishki']
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
    log_message_to_existing_file()
    logging.info("ФУНКЦІЯ 8. Починаю присвоєння нових SKU товарам у 1.csv...")

    # --- 1. Завантаження налаштувань ---
    settings = load_settings()
    try:
        sl_new_path = settings['paths']['csv_path_supliers_1_new']
        zalishki_path = settings['paths']['csv_path_zalishki']
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

def download_images_for_product():
    """
    6 етапів:
      1. Очистка папок JPG та WEBP
      2. Завантаження зображень у папку JPG
      3. Переміщення GIF у WEBP
      4. Конвертація JPG у WEBP
      5. Оновлення CSV
      6. Копіювання на сайт
    """
    log_message_to_existing_file()
    logging.info("ФУНКЦІЯ 9. Початок процесу обробки зображень...")

    settings = load_settings()
    try:
        sl_new = settings['paths']['csv_path_supliers_1_new']
        jpg_path = settings['paths']['img_path_jpg']
        webp_path = settings['paths']['img_path_webp']
        cat_map = settings['categories']
        site_path = settings['paths']['site_path_images']
    except KeyError as e:
        logging.error(f"❌ Не знайдено шлях у settings.json: {e}")
        return

    URL, SKU, CAT, IMG_LIST, WEBP_LIST = 1, 15, 16, 17, 18

    # 1️⃣ Очистка
    clear_directory(jpg_path)
    clear_directory(webp_path)

    # 2️⃣ Завантаження
    rows = []
    with open(sl_new, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        rows.append(header)
        for row in reader:
            if len(row) <= IMG_LIST:
                row.extend([''] * (IMG_LIST - len(row) + 1))
            url, sku, cat = row[URL].strip(), row[SKU].strip(), row[CAT].strip()
            if url and sku and cat:
                imgs = download_product_images(url, sku, cat, jpg_path, cat_map)
                row[IMG_LIST] = ', '.join(imgs) if imgs else ''
            rows.append(row)
            time.sleep(random.uniform(0.5, 1.5))

    with open(sl_new, 'w', encoding='utf-8', newline='') as f:
        csv.writer(f).writerows(rows)
    logging.info(f"📥 Завантаження зображень завершено ({len(rows)-1} рядків).")

    # 3️⃣ GIF
    move_gifs(jpg_path, webp_path)

    # 4️⃣ WEBP
    convert_to_webp_square(jpg_path, webp_path)

    # 5️⃣ CSV sync
    sync_webp_column(sl_new, webp_path, WEBP_LIST, SKU)

    # 6️⃣ Копіювання
    copy_to_site(webp_path, site_path)

    logging.info("✅ Усі 6 етапів обробки зображень виконано успішно.")

def create_new_products_import_file():
    """
    Створює оновлений файл `new_prod.csv` для імпорту нових товарів.

    🧩 Логіка роботи:
    1️⃣ Зчитує поточний заголовок файлу new_prod.csv (він ЗАЛИШАЄТЬСЯ без змін).
    2️⃣ Очищує файл від старих даних.
    3️⃣ Зчитує new.csv.
    4️⃣ Переносить дані згідно з COLUMN_MAP.
    5️⃣ Записує оновлені дані у new_prod.csv, залишаючи старий заголовок.

    ⚠️ Якщо файлу new_prod.csv ще немає, його потрібно створити вручну з правильними заголовками.
    """

    # -----------------------------------------------------------
    # 🔢 Мапа колонок: {індекс у new.csv → індекс у new_prod.csv}
    # -----------------------------------------------------------
    COLUMN_MAP = {
        15: 0, 1: 1, 2: 2, 5: 3, 6: 4, 7: 5, 8: 6, 9: 7, 16: 8, 18: 9,
        19: 10, 20: 11, 21: 12, 22: 13, 23: 14, 24: 15, 25: 16, 26: 17,
        27: 18, 28: 19, 29: 20, 30: 21, 31: 22, 32: 23, 33: 24, 34: 25,
        35: 26, 36: 27, 37: 28, 38: 29, 39: 30, 40: 31, 41: 32, 42: 33,
        43: 34, 44: 35, 45: 36, 46: 37, 47: 38, 48: 39, 49: 40, 51: 41
    }
    log_message_to_existing_file()
    logging.info("ФУНКЦІЯ 10. Починаю формування файлу new_prod.csv для імпорту нових товарів.")

    # -----------------------------------------------------------
    # 🧩 Крок 1: Завантаження налаштувань і шляхів
    # -----------------------------------------------------------
    settings = load_settings()
    try:
        sl_new_path = settings['paths']['csv_path_supliers_1_new']
        sl_new_prod_path = settings['paths']['csv_path_sl_new_prod']
    except KeyError as e:
        logging.critical(f"❌ Не знайдено шлях у settings.json: {e}")
        return

    temp_file = sl_new_prod_path + '.temp'

    # -----------------------------------------------------------
    # 📄 Крок 2: Зчитуємо існуючий заголовок new_prod.csv
    # -----------------------------------------------------------
    if not os.path.exists(sl_new_prod_path):
        logging.critical(
            f"⚠️ Файл new_prod_path не знайдено!\n"
            "Створіть його вручну з правильними заголовками перед запуском."
        )
        return

    try:
        with open(sl_new_prod_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                logging.critical(f"❌ У файлі {sl_new_prod_path} відсутній заголовок.")
                return
        logging.info("✅ Заголовок збережено, старі дані буде очищено.")
    except Exception as e:
        logging.error(f"Помилка при читанні заголовка: {e}", exc_info=True)
        return

    # -----------------------------------------------------------
    # 📦 Крок 3: Зчитуємо SL_new.csv
    # -----------------------------------------------------------
    if not os.path.exists(sl_new_path):
        logging.critical(f"❌ Файл new_path не знайдено.")
        return

    try:
        with open(sl_new_path, 'r', encoding='utf-8') as f:
            reader = list(csv.reader(f))
    except Exception as e:
        logging.critical(f"Помилка при читанні new_path: {e}", exc_info=True)
        return

    if len(reader) <= 1:
        logging.warning("⚠️ SL_new.csv порожній або містить лише заголовок.")
        # Записуємо лише заголовок у новий файл
        with open(sl_new_prod_path, 'w', encoding='utf-8', newline='') as f:
            csv.writer(f).writerow(header)
        return

    source_rows = reader[1:]  # Пропускаємо заголовок

    # -----------------------------------------------------------
    # 🔄 Крок 4: Формуємо нові рядки згідно з мапою COLUMN_MAP
    # -----------------------------------------------------------
    rows_to_write = [header]
    processed = 0
    max_src = max(COLUMN_MAP.keys())

    for i, src_row in enumerate(source_rows, start=2):  # рядки починаються з 2 (1 — заголовок)
        # Перевіряємо довжину рядка (щоб уникнути IndexError)
        if len(src_row) <= max_src:
            logging.warning(f"⚠️ Рядок {i}: пропущено — недостатньо колонок ({len(src_row)}/{max_src+1}).")
            continue

        # Створюємо новий рядок із такою самою кількістю колонок, як у заголовку
        tgt_row = [''] * len(header)

        # Переносимо значення згідно з COLUMN_MAP
        for src_idx, tgt_idx in COLUMN_MAP.items():
            if tgt_idx < len(tgt_row):
                tgt_row[tgt_idx] = src_row[src_idx].strip()

        rows_to_write.append(tgt_row)
        processed += 1

    logging.info(f"🔁 Оброблено {processed} рядків для запису у new_prod.csv.")

    # -----------------------------------------------------------
    # 💾 Крок 5: Запис у файл (очистка старих даних, але заголовок зберігається)
    # -----------------------------------------------------------
    try:
        with open(temp_file, 'w', encoding='utf-8', newline='') as f:
            csv.writer(f).writerows(rows_to_write)
        os.replace(temp_file, sl_new_prod_path)
        logging.info(f"✅ Файл new_prod_path оновлено ({processed} рядків).")
    except Exception as e:
        logging.error(f"❌ Помилка запису new_prod_path: {e}", exc_info=True)
        if os.path.exists(temp_file):
            os.remove(temp_file)

def update_existing_products_batch():
    """
    Оновлює існуючі товари у WooCommerce на основі CSV-файлу old_prod_new_SHK.csv.

    Функціонал:
    1. Зчитує налаштування та підключається до WooCommerce API.
    2. Читає CSV та формує список товарів для пакетного оновлення.
    3. Для кожного товару:
       - Перевіряє ID
       - Збирає стандартні поля (sku, post_date, product_type, tax_status)
       - Формує meta_data (ACF, Rank Math)
       - Обробляє атрибути:
            * зберігає старі атрибути, яких немає в CSV
            * оновлює існуючі та додає нові
            * глобальний атрибут pa_manufacturer прив'язується до WooCommerce
       - Оновлює теги (tags)
       - Оновлює excerpt та content
    4. Надсилає товари пакетами по BATCH_SIZE.
    5. Логує результат та помилки.
    """

    log_message_to_existing_file()
    logging.info("ФУНКЦІЯ 11. Починаю пакетне оновлення існуючих товарів з old_prod_new_SHK.csv...")

    # --- Завантаження налаштувань ---
    settings = load_settings()
    if not settings:
        logging.critical("❌ Не вдалося завантажити налаштування.")
        return

    try:
        csv_path = settings['paths']['csv_path_sl_old_prod_new_shk']
    except KeyError:
        logging.error("❌ Не знайдено шлях до CSV у налаштуваннях.")
        return

    # --- Підключення до WooCommerce API ---
    wcapi = get_wc_api(settings)
    if not wcapi:
        logging.critical("❌ Не вдалося створити об'єкт WooCommerce API.")
        return

    # --- Параметри пакетного оновлення ---
    BATCH_SIZE = 50
    products_to_update = []
    total_products_read = 0
    total_updated = 0
    total_skipped = 0
    errors_list = []
    start_time = time.time()

    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            logging.info(f"Зчитано {len(headers)} заголовків: {', '.join(headers[:5])}...")

            STANDARD_FIELDS = ['sku', 'post_date', 'product_type', 'tax_status']
            ACF_PREFIX = 'Мета: '
            ATTRIBUTE_PREFIX = 'attribute:'
            field_map = {header: idx for idx, header in enumerate(headers)}

            for row in reader:
                total_products_read += 1

                # --- Перевірка ID ---
                product_id_str = row[field_map.get('id', -1)].strip()
                if not product_id_str.isdigit():
                    errors_list.append(f"Рядок {total_products_read}: Пропущено. Некоректний ID.")
                    total_skipped += 1
                    continue

                product_id = int(product_id_str)
                product_data = {"id": product_id}
                meta_data = []
                new_attributes = []
                tags = []

                # --- Проходимо по всіх колонках ---
                for key, index in field_map.items():
                    if index >= len(row):
                        continue
                    value = row[index].strip()
                    if not value:
                        continue

                    # --- Стандартні поля ---
                    if key in STANDARD_FIELDS:
                        if key == 'post_date':
                            product_data['date_created'] = value
                            try:
                                dt = datetime.fromisoformat(value)
                                product_data['date_created_gmt'] = (dt - timedelta(hours=3)).isoformat()
                            except ValueError:
                                errors_list.append(
                                    f"⚠️ Рядок {total_products_read}: некоректний формат post_date '{value}'"
                                )
                        else:
                            product_data[key] = value

                    # --- Meta Data ---
                    elif key.startswith(ACF_PREFIX) or key == 'rank_math_focus_keyword':
                        meta_key = key.replace(ACF_PREFIX, '') if key.startswith(ACF_PREFIX) else key
                        meta_data.append({"key": meta_key, "value": value})

                    # --- Теги ---
                    elif key == 'Позначки':
                        tag_names = [t.strip() for t in value.split(',') if t.strip()]
                        tags.extend([{"name": t} for t in tag_names])

                    # --- Не додаю Короткий та повний опис ---
                    # elif key == 'excerpt':
                    #    product_data['excerpt'] = value
                    # elif key == 'content':
                    #    product_data['description'] = value  # WooCommerce API

                    # --- Атрибути ---
                    elif key.startswith(ATTRIBUTE_PREFIX):
                        attr_name = key.replace(ATTRIBUTE_PREFIX, '')
                        options = [v.strip() for v in value.split(',') if v.strip()]
                        if options:
                            attr_dict = {
                                "name": attr_name,
                                "position": len(new_attributes),
                                "visible": True,
                                "variation": False,
                                "options": options
                            }
                            # Глобальний атрибут manufacturer
                            if attr_name == "pa_manufacturer":
                                attr_dict["id"] = 1  # ID глобального атрибута manufacturer у WooCommerce
                            new_attributes.append(attr_dict)

                # --- Додаємо meta_data та теги до product_data ---
                if meta_data:
                    product_data['meta_data'] = meta_data
                if tags:
                    product_data['tags'] = tags

                # --- Обробка атрибутів: merge з існуючими ---
                if new_attributes:
                    try:
                        # Отримуємо існуючі атрибути товару
                        existing_attributes = wcapi.get(f"products/{product_id}").json().get("attributes", [])
                        attr_map = {attr['name']: attr for attr in existing_attributes}

                        # Оновлюємо існуючі або додаємо нові
                        for new_attr in new_attributes:
                            name = new_attr['name']
                            if name in attr_map:
                                attr_map[name]['options'] = new_attr['options']
                                attr_map[name]['position'] = new_attr['position']
                                attr_map[name]['visible'] = new_attr['visible']
                                attr_map[name]['variation'] = new_attr['variation']
                                if 'id' in new_attr:
                                    attr_map[name]['id'] = new_attr['id']
                            else:
                                attr_map[name] = new_attr

                        product_data['attributes'] = list(attr_map.values())
                    except Exception as e:
                        logging.error(f"Рядок {total_products_read}: Помилка merge атрибутів: {e}")

                products_to_update.append(product_data)

                # --- Надсилання пакету на оновлення ---
                if len(products_to_update) >= BATCH_SIZE:
                    total_updated += _process_batch_update(wcapi, products_to_update, errors_list)
                    products_to_update = []

            # --- Обробка залишку ---
            if products_to_update:
                total_updated += _process_batch_update(wcapi, products_to_update, errors_list)

    except Exception as e:
        logging.critical(f"❌ Критична помилка при обробці CSV: {e}", exc_info=True)
        return

    # --- Підсумок ---
    elapsed_time = int(time.time() - start_time)
    logging.info("--- 🏁 Підсумок оновлення існуючих товарів ---")
    logging.info(f"Всього рядків: {total_products_read}")
    logging.info(f"Успішно оновлено: {total_updated}")
    logging.info(f"Пропущено/з помилками: {total_products_read - total_updated}")
    logging.info(f"Загальна тривалість: {elapsed_time} сек.")

    if errors_list:
        logging.warning(f"⚠️ Знайдено {len(errors_list)} помилок. Перші 5:")
        for err in errors_list[:5]:
            logging.warning(f"-> {err}")
    else:
        logging.info("✅ Оновлення завершено без помилок.")

def create_new_products_batch():
    """Пакетне створення нових товарів у WooCommerce з CSV."""
    log_message_to_existing_file()
    logging.info("🚀 Починаю пакетне СТВОРЕННЯ нових товарів з SL_new_prod.csv...")

    settings = load_settings()
    if not settings:
        logging.critical("❌ Не вдалося завантажити налаштування.")
        return

    try:
        csv_path = settings['paths']['csv_path_sl_new_prod']
        uploads_path = '/var/www/html/erosinua/public_html/wp-content/uploads/products'
    except KeyError as e:
        logging.error(f"❌ Не знайдено шлях до CSV або uploads_path: {e}")
        return

    wcapi = get_wc_api(settings)
    if not wcapi:
        logging.critical("❌ Не вдалося створити об'єкт WooCommerce API.")
        return

    BATCH_SIZE = 50
    products_to_create: List[Dict[str, Any]] = []
    total_products_read = 0
    total_created = 0
    total_skipped = 0
    errors_list: List[str] = []
    start_time = time.time()

    STANDARD_FIELDS = ['sku', 'post_date', 'excerpt', 'content', 'product_type']
    ACF_PREFIX = 'Мета: '
    ATTRIBUTE_PREFIX = 'attribute:'

    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            field_map = {header: idx for idx, header in enumerate(headers)}

            for row in reader:
                total_products_read += 1
                sku = row[field_map.get('sku', -1)].strip()
                name = row[field_map.get('name', -1)].strip()

                if not sku or not name:
                    errors_list.append(f"Рядок {total_products_read}: Пропущено. Відсутній SKU або Name.")
                    total_skipped += 1
                    continue

                product_data: Dict[str, Any] = {"sku": sku, "name": name, "status": "draft"}
                meta_data: List[Dict[str, Any]] = []
                attributes: List[Dict[str, Any]] = []
                tags: List[Dict[str, Any]] = []
                images: List[Dict[str, Any]] = []

                for key, index in field_map.items():
                    if index >= len(row):
                        continue
                    value = row[index].strip()
                    if not value:
                        continue

                    # --- стандартні поля ---
                    if key in STANDARD_FIELDS:
                        if key == 'product_type':
                            product_data['type'] = value or 'simple'
                        elif key == 'excerpt':
                            product_data['short_description'] = value
                        elif key == 'content':
                            product_data['description'] = value
                        elif key == 'post_date':
                            product_data['date_created'] = value
                    # --- meta_data ---
                    elif key.startswith(ACF_PREFIX):
                        meta_data.append({"key": key.replace(ACF_PREFIX, ''), "value": value})
                    elif key == 'rank_math_focus_keyword':
                        meta_data.append({"key": key, "value": value})
                    # --- атрибути ---
                    elif key.startswith(ATTRIBUTE_PREFIX):
                        attr_name = key.replace(ATTRIBUTE_PREFIX, '')
                        options = [v.strip() for v in value.split(',') if v.strip()]
                        if options:
                            attr = {"name": attr_name, "position": len(attributes), "visible": True,
                                    "variation": False, "options": options}
                            if attr_name == "pa_manufacturer":
                                attr["id"] = 1
                            attributes.append(attr)
                    # --- категорії ---
                    elif key == 'categories':
                        category_names = [c.strip() for c in value.split('|') if c.strip()]
                        if category_names:
                            product_data['categories'] = [{"name": c} for c in category_names]
                    # --- теги ---
                    elif key == 'Позначки':
                        tags.extend([{"name": t.strip()} for t in value.split(',') if t.strip()])
                    # --- зображення ---
                    elif key == 'image_name':
                        images = find_media_ids_for_sku(wcapi, sku, uploads_path)
                    # --- ціна, запаси, оподаткування, статус ---
                    elif key == 'regular_price':
                        product_data['regular_price'] = str(value)
                    elif key == 'manage_stock':
                        product_data['manage_stock'] = value.strip() in ['1', 'yes', 'true']
                    elif key == 'tax_status':
                        product_data['tax_status'] = 'taxable' if value.strip() in ['1', 'yes', 'true'] else 'none'
                    elif key == 'status':
                        product_data['status'] = 'publish' if value.strip() in ['1', 'yes', 'true'] else 'draft'
                    else:
                        product_data[key] = value

                # --- додавання списків у product_data ---
                if meta_data:
                    product_data['meta_data'] = meta_data
                if attributes:
                    product_data['attributes'] = attributes
                if tags:
                    product_data['tags'] = tags
                if images:
                    product_data['images'] = images

                products_to_create.append(product_data)

                # --- пакетна відправка ---
                if len(products_to_create) >= BATCH_SIZE:
                    total_created += _process_batch_create(wcapi, products_to_create, errors_list)
                    products_to_create = []

            # --- обробка залишку ---
            if products_to_create:
                total_created += _process_batch_create(wcapi, products_to_create, errors_list)

    except FileNotFoundError:
        logging.critical(f"❌ Файл {csv_path} не знайдено.")
        return
    except Exception as e:
        logging.critical(f"❌ Критична помилка при читанні CSV: {e}", exc_info=True)
        return

    # --- підсумок ---
    elapsed_time = int(time.time() - start_time)
    logging.info("--- 🏁 Підсумок створення нових товарів ---")
    logging.info(f"Всього прочитано рядків: {total_products_read}")
    logging.info(f"Успішно створено товарів: {total_created}")
    logging.info(f"Пропущено/з помилками: {total_products_read - total_created}")
    logging.info(f"Загальна тривалість: {elapsed_time} сек.")

    if errors_list:
        logging.warning(f"⚠️ Знайдено {len(errors_list)} помилок/пропусків. Перші 5 помилок:")
        for error in errors_list[:5]:
            logging.warning(f"-> {error}")
    else:
        logging.info("✅ Створення нових товарів завершено успішно.")