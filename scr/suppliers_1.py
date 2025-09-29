import csv
import os
import time
import requests
import shutil
import re
import pandas as pd
from bs4 import BeautifulSoup
import random 
import logging
from scr.base_function import get_wc_api, load_settings, setup_new_log_file, log_message_to_existing_file, load_attributes_csv, save_attributes_csv
from datetime import datetime, timedelta

def find_new_products():
    """
    Порівнює артикули товарів з прайс-листа постачальника з артикулами,
    що є на сайті, і записує нові товари в окремий файл.
    """
    log_message_to_existing_file()
    logging.info("Починаю пошук нових товарів...")
    
    settings = load_settings()
    
    zalishki_path = settings['paths']['csv_path_zalishki']
    supliers_new_path = settings['paths']['csv_path_supliers_1_new']
    supliers_csv_path = settings['suppliers']['1']['csv_path']
    delimiter = settings['suppliers']['1']['delimiter']
    
    sku_prefix = settings['suppliers']['1']['search']
    # Приводимо слова до нижнього регістру, щоб порівняння було незалежним від регістру
    bad_words = [word.lower() for word in settings['suppliers']['1'].get('bad_words', [])]

    new_product_headers = [
        settings['column_supliers_1_new_name'][str(i)]
        for i in range(len(settings['column_supliers_1_new_name']))
    ]
    num_new_columns = len(new_product_headers)

    logging.info("Зчитую існуючі артикули з файлу, вказаного за ключем 'csv_path_zalishki'.")

    try:
        with open(zalishki_path, mode='r', encoding='utf-8') as zalishki_file:
            zalishki_reader = csv.reader(zalishki_file)
            next(zalishki_reader, None)
            existing_skus = {row[9].strip().lower() for row in zalishki_reader if len(row) > 9}
            logging.info(f"Зчитано {len(existing_skus)} унікальних артикулів.")

        logging.info("Відкриваю файл для запису нових товарів, вказаний за ключем 'csv_path_supliers_1_new'.")
        with open(supliers_new_path, mode='w', encoding='utf-8', newline='') as new_file:
            writer = csv.writer(new_file)
            writer.writerow(new_product_headers)
            
            logging.info("Порівнюю дані з прайс-листом, вказаним за ключем 'csv_path' для постачальника 1.")
            with open(supliers_csv_path, mode='r', encoding='utf-8') as supliers_file:
                supliers_reader = csv.reader(supliers_file, delimiter=delimiter)
                next(supliers_reader, None)
                
                new_products_count = 0
                filtered_out_count = 0
                for row in supliers_reader:
                    if not row:
                        continue
                    
                    sku = row[0].strip().lower()
                    
                    if sku and sku not in existing_skus:
                        
                        new_row = [''] * num_new_columns
                        
                        sku_with_prefix = sku_prefix + row[0]
                        new_row[0] = sku_with_prefix

                        column_mapping = [
                            (0, 5),  # a(0) -> f(5)
                            (1, 6),  # b(1) -> g(6)
                            (2, 7),  # c(2) -> h(7)
                            (3, 8),  # d(3) -> i(8)
                            (6, 9),  # g(6) -> j(9)
                            (7, 10), # h(7) -> k(10)
                            (8, 11), # i(8) -> l(11)
                            (9, 12), # j(9) -> m(12)
                            (10, 13), # k(10) -> n(13)
                            (11, 14), # l(11) -> o(14)
                        ]
                        
                        for source_index, dest_index in column_mapping:
                            if len(row) > source_index:
                                new_row[dest_index] = row[source_index]
                                
                        should_skip = False
                        check_columns_indices = [6, 7, 10]
                        
                        for index in check_columns_indices:
                            if len(new_row) > index:
                                # Приводимо вміст комірки до нижнього регістру
                                cell_content = new_row[index].lower()
                                for bad_word in bad_words:
                                    if bad_word in cell_content:
                                        logging.info(f"Пропускаю товар з артикулом '{row[0]}' через заборонене слово '{bad_word}' в колонці {index} нового файлу.")
                                        should_skip = True
                                        filtered_out_count += 1
                                        break
                                if should_skip:
                                    break
                        
                        if should_skip:
                            continue
                        
                        new_products_count += 1
                        writer.writerow(new_row)

        logging.info(f"Знайдено {new_products_count} нових товарів. Відфільтровано {filtered_out_count} товарів.")
        logging.info(f"Дані записано у файл, вказаний за ключем 'csv_path_supliers_1_new'.")
    
    except FileNotFoundError as e:
        logging.info(f"Помилка: Файл не знайдено - {e}")
    except Exception as e:
        logging.info(f"Виникла непередбачена помилка: {e}")



def find_product_data():
    """
    Зчитує файл з новими товарами, переходить за URL-адресою,
    знаходить URL-адресу товару та штрих-код на сторінці,
    і записує знайдену URL-адресу в колонку B(1) одразу після обробки.
    """
    log_message_to_existing_file()
    logging.info("Починаю пошук URL-адрес товарів та штрих-кодів...")
    
    settings = load_settings()
    supliers_new_path = settings['paths']['csv_path_supliers_1_new']
    site_url = settings['suppliers']['1']['site']
    temp_file_path = supliers_new_path + '.temp'

    try:
        # Зчитуємо дані з оригінального файлу
        with open(supliers_new_path, mode='r', encoding='utf-8') as input_file:
            reader = csv.reader(input_file)
            headers = next(reader)
            
            # Відкриваємо тимчасовий файл для запису
            with open(temp_file_path, mode='w', encoding='utf-8', newline='') as output_file:
                writer = csv.writer(output_file)
                writer.writerow(headers)
                
                for idx, row in enumerate(reader):
                    search_url = row[0].strip()
                    file_sku = row[5].strip()

                    if not search_url or search_url.startswith('Помилка запиту'):
                        logging.warning(f"Рядок {idx + 2}: Пропускаю товар з артикулом '{file_sku}' через відсутність URL пошуку.")
                        writer.writerow(row)
                        continue
                    
                    try:
                        response = requests.get(search_url)
                        response.raise_for_status()
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        found_url = None
                        
                        # Пошук варіативних товарів
                        variant_inputs = soup.find_all('input', class_='variant_control', attrs={'data-code': True})
                        for input_tag in variant_inputs:
                            site_sku = input_tag.get('data-code', '').strip()
                            if file_sku == site_sku:
                                parent_div = input_tag.find_parent('div', class_='card-block')
                                if parent_div:
                                    link_tag = parent_div.find('h4', class_='card-title').find('a')
                                    if link_tag and link_tag.has_attr('href'):
                                        found_url = site_url + link_tag['href']
                                        break
                        
                        if not found_url:
                            # Пошук простих товарів
                            simple_divs = soup.find_all('div', class_='radio')
                            for div_tag in simple_divs:
                                site_sku = div_tag.get_text(strip=True).strip()
                                if file_sku == site_sku:
                                    parent_div = div_tag.find_parent('div', class_='card-block')
                                    if parent_div:
                                        link_tag = parent_div.find('h4', class_='card-title').find('a')
                                        if link_tag and link_tag.has_attr('href'):
                                            found_url = site_url + link_tag['href']
                                            break
                        
                        if found_url:
                            row[1] = found_url
                            logging.info(f"Рядок {idx + 2}: Артикул '{file_sku}' - URL товару знайдено: {found_url}")
                        else:
                            logging.warning(f"Рядок {idx + 2}: Артикул '{file_sku}' - URL товару не знайдено.")

                        writer.writerow(row)
                    
                    except requests.RequestException as e:
                        logging.error(f"Рядок {idx + 2}: Помилка при запиті до {search_url}: {e}")
                        row[0] = f'Помилка запиту: {e}'
                        writer.writerow(row)
                    
                    time.sleep(random.uniform(1, 3))

        # Після успішної обробки перейменовуємо тимчасовий файл
        os.replace(temp_file_path, supliers_new_path)
        logging.info("Пошук завершено. Файл оновлено.")

    except FileNotFoundError as e:
        logging.error(f"Помилка: Файл не знайдено - {e}")
    except Exception as e:
        # Якщо сталася помилка, видаляємо тимчасовий файл, щоб уникнути пошкодження
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        logging.error(f"Виникла непередбачена помилка: {e}")



def parse_product_attributes():
    """
    Парсить сторінки товарів, застосовує заміну з attribute.csv (блочна структура) 
    і додає нові невідомі значення одразу перед наступним блоком-заголовком.
    """
    log_message_to_existing_file()
    logging.info("Починаю парсинг сторінок товарів для вилучення атрибутів...")

    settings = load_settings()
    try:
        supliers_new_path = settings['paths']['csv_path_supliers_1_new']
        product_data_map = settings['suppliers']['1']['product_data_columns']
        other_attrs_index = settings['suppliers']['1']['other_attributes_column']
    except TypeError as e:
        logging.error(f"Помилка доступу до налаштувань. Перевірте settings.json: {e}")
        return
    
    processing_map = product_data_map.copy()
    if "Штрих-код" in processing_map:
        processing_map.pop("Штрих-код")

    replacements_map, raw_data = load_attributes_csv()
    changes_made = False 

    max_raw_row_len = len(raw_data[0]) if raw_data and raw_data[0] else 10

    # === ЛОГІКА ДЛЯ ПОШУКУ МІСЦЯ ВСТАВКИ (КІНЕЦЬ БЛОКУ) ===
    # insertion_points: {col_index: індекс_в_raw_data_для_вставки}
    # Це індекс ПЕРЕД яким потрібно вставити новий рядок.
    
    insertion_points = {}
    current_col_index = None
    
    # Ігноруємо заголовок (raw_data[0])
    for i, row in enumerate(raw_data[1:], start=1):
        
        is_header = row and row[0].strip().isdigit()
        
        if is_header:
            try:
                col_index = int(row[0].strip())
                
                # 1. Завершуємо попередній блок: якщо ми перейшли до нового заголовка
                if current_col_index is not None and current_col_index not in insertion_points:
                    # Якщо попередній блок був порожній (нічого не оновлювалося), точка вставки буде тут (перед поточним заголовком)
                    insertion_points[current_col_index] = i
                
                # 2. Починаємо новий блок: точка вставки для ТЕПЕРІШНЬОГО блоку
                current_col_index = col_index
                # Початкова точка вставки - одразу після поточного заголовка
                insertion_points[col_index] = i + 1 
                
            except ValueError:
                current_col_index = None
        
        # Якщо це рядок з атрибутом (або порожній рядок в межах блоку), оновлюємо місце вставки
        elif current_col_index is not None:
            insertion_points[current_col_index] = i + 1
        
    logging.debug(f"Точки вставки (insertion_points): {insertion_points}")
    # ==========================================


    try:
        with open(supliers_new_path, mode='r', encoding='utf-8') as input_file:
            reader = csv.reader(input_file)
            headers = next(reader)
            
            temp_file_path = supliers_new_path + '.temp'
            with open(temp_file_path, mode='w', encoding='utf-8', newline='') as output_file:
                writer = csv.writer(output_file)
                writer.writerow(headers)
                
                for idx, row in enumerate(reader):
                    product_url = row[1].strip()
                    file_sku = row[5].strip()
                    max_index = max(max(product_data_map.values(), default=0), other_attrs_index)
                    if len(row) <= max_index:
                        row.extend([''] * (max_index + 1 - len(row)))
                        
                    if not product_url or product_url.startswith('Помилка запиту'):
                        writer.writerow(row)
                        continue

                    try:
                        response = requests.get(product_url)
                        response.raise_for_status()
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        characteristics_div = soup.find('div', id='w0-tab0')
                        
                        if characteristics_div and characteristics_div.find('table'):
                            parsed_attributes = {}
                            for tr in characteristics_div.find('table').find_all('tr'):
                                cells = tr.find_all('td')
                                if len(cells) == 2:
                                    key = cells[0].get_text(strip=True).replace(':', '')
                                    value = cells[1].get_text(strip=True)
                                    parsed_attributes[key] = value

                            other_attributes = []
                            for attr_name, attr_value in parsed_attributes.items():
                                
                                target_col_index = processing_map.get(attr_name)
                                
                                if target_col_index is not None:
                                    
                                    replacement_rules = replacements_map.get(target_col_index, {})
                                    original_value_lower = attr_value.strip().lower() 
                                    
                                    new_value = replacement_rules.get(original_value_lower)
                                    
                                    # 1. Застосування заміни
                                    if new_value is not None and new_value != "":
                                        attr_value = new_value
                                        
                                    else:
                                        # 2. Додавання нового атрибута, якщо його немає
                                        if original_value_lower not in replacement_rules:
                                            
                                            insert_index = insertion_points.get(target_col_index)
                                            
                                            if insert_index is None:
                                                logging.error(f"Атрибут '{attr_value}' (I={target_col_index}) не додано: відсутня точка вставки (заголовок не знайдено).")
                                                attr_value = attr_value 
                                                continue
                                            
                                            logging.warning(f"НОВИЙ АТРИБУТ БУДЕ ДОДАНО: '{attr_value}' (I={target_col_index}) в індекс {insert_index}.")

                                            # Новий рядок: ['', '', original, '', '', ...] (col 0 і col 1 порожні)
                                            new_raw_row = ['', '', original_value_lower] + [''] * (max_raw_row_len - 3)
                                            
                                            # Вставляємо новий рядок у кінець поточного блоку
                                            raw_data.insert(insert_index, new_raw_row)
                                            
                                            # Оновлюємо мапу для поточної сесії
                                            replacements_map.setdefault(target_col_index, {})[original_value_lower] = ""
                                            changes_made = True 
                                            
                                            # ОНОВЛЕННЯ ТОЧОК ВСТАВКИ: Зсуваємо всі подальші індекси на 1
                                            for col, point in insertion_points.items():
                                                if point >= insert_index:
                                                    insertion_points[col] += 1
                                            
                                            # Використовуємо оригінальне значення
                                            attr_value = attr_value 

                                    row[target_col_index] = attr_value
                                
                                elif attr_name == "Штрих-код":
                                     pass 
                                else:
                                    other_attributes.append(f"{attr_name}:{attr_value}")

                            if other_attributes:
                                row[other_attrs_index] = ', '.join(other_attributes)
                                
                        writer.writerow(row)
                    
                    except requests.RequestException:
                        writer.writerow(row)
                    
                    time.sleep(random.uniform(1, 3))

        os.replace(temp_file_path, supliers_new_path)
        logging.info("Парсинг атрибутів завершено. Файл оновлено.")

        # Збереження оновлених сирих даних у CSV
        if changes_made:
            save_attributes_csv(raw_data)
        else:
            logging.info("Збереження attribute.csv не потрібне. Змін: False.")

    except Exception as e:
        logging.error(f"Виникла непередбачена помилка: {e}")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)