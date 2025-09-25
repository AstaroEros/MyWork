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
from scr.base_function import get_wc_api, load_settings, setup_new_log_file, log_message_to_existing_file
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
    і записує знайдену URL-адресу в колонку B(1).
    """
    log_message_to_existing_file()
    logging.info("Починаю пошук URL-адрес товарів та штрих-кодів...")
    
    settings = load_settings()
    supliers_new_path = settings['paths']['csv_path_supliers_1_new']
    site_url = settings['suppliers']['1']['site']

    try:
        with open(supliers_new_path, mode='r', encoding='utf-8') as input_file:
            reader = csv.reader(input_file)
            headers = next(reader)
            rows = list(reader)

        updated_rows = [headers]
        for row in rows:
            search_url = row[0].strip()
            # Зчитуємо артикул з колонки f(5) (індекс 5)
            file_sku = row[5].strip() 

            if not search_url or search_url.startswith('Помилка запиту'):
                logging.warning(f"Пропускаю товар без URL: {file_sku}")
                updated_rows.append(row)
                continue

            logging.info(f"Обробляю товар з артикулом: {file_sku}. URL пошуку: {search_url}")
            
            try:
                response = requests.get(search_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                
                found_url = None
                
                # Пошук варіативних товарів
                variant_inputs = soup.find_all('input', class_='variant_control', attrs={'data-code': True})
                for input_tag in variant_inputs:
                    site_sku = input_tag.get('data-code', '').strip()
                    logging.info(f"Артикул товару з файлу = {file_sku}, артикул для звірки = {site_sku}")
                    if file_sku == site_sku:
                        parent_div = input_tag.find_parent('div', class_='card-block')
                        if parent_div:
                            link_tag = parent_div.find('h4', class_='card-title').find('a')
                            if link_tag and link_tag.has_attr('href'):
                                found_url = site_url + link_tag['href']
                                logging.info(f"Знайдено варіативний товар: {file_sku}. URL: {found_url}")
                                break
                
                if not found_url:
                    # Пошук простих товарів
                    simple_divs = soup.find_all('div', class_='radio')
                    for div_tag in simple_divs:
                        site_sku = div_tag.get_text(strip=True).strip()
                        logging.info(f"Артикул товару з файлу = {file_sku}, артикул для звірки = {site_sku}")
                        if file_sku == site_sku:
                            parent_div = div_tag.find_parent('div', class_='card-block')
                            if parent_div:
                                link_tag = parent_div.find('h4', class_='card-title').find('a')
                                if link_tag and link_tag.has_attr('href'):
                                    found_url = site_url + link_tag['href']
                                    logging.info(f"Знайдено простий товар: {file_sku}. URL: {found_url}")
                                    break
                
                if found_url:
                    # Записуємо URL в колонку B(1)
                    row[1] = found_url
                else:
                    logging.warning(f"Не вдалося знайти URL-адресу для товару: {file_sku}")

                updated_rows.append(row)
            
            except requests.RequestException as e:
                logging.error(f"Помилка при запиті до {search_url}: {e}")
                row[0] = f'Помилка запиту: {e}'
                updated_rows.append(row)
            
            time.sleep(2)

        with open(supliers_new_path, mode='w', encoding='utf-8', newline='') as output_file:
            writer = csv.writer(output_file)
            writer.writerows(updated_rows)

    except FileNotFoundError as e:
        logging.error(f"Помилка: Файл не знайдено - {e}")
    except Exception as e:
        logging.error(f"Виникла непередбачена помилка: {e}")

    logging.info("Пошук завершено. Файл оновлено.")