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
    
    # Отримуємо значення префікса з налаштувань
    sku_prefix = settings['suppliers']['1']['search']

    # Отримуємо заголовки та визначаємо кількість колонок у новому файлі
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
                for row in supliers_reader:
                    if not row:
                        continue
                    
                    sku = row[0].strip().lower()
                    
                    if sku and sku not in existing_skus:
                        new_products_count += 1
                        
                        new_row = [''] * num_new_columns
                        
                        # ✨ Додаємо префікс до значення з колонки A(0)
                        # Індекс 0 у прайс-листі - це A(0)
                        sku_with_prefix = sku_prefix + row[0]
                        
                        # Записуємо нове значення в колонку a(0) нового файлу
                        new_row[0] = sku_with_prefix

                        # Вставляємо інші дані в потрібні позиції
                        new_row[5] = row[0] # a(0) -> f(5)
                        new_row[6] = row[1] # b(1) -> g(6)
                        new_row[7] = row[2] # c(2) -> h(7)
                        new_row[8] = row[3] # d(3) -> i(8)
                        new_row[9] = row[6] # g(6) -> j(9)
                        new_row[10] = row[7] # h(7) -> k(10)
                        new_row[11] = row[8] # i(8) -> l(11)
                        new_row[12] = row[9] # j(9) - m(12)
                        new_row[13] = row[10] # k(10) -> n(13)
                        new_row[14] = row[11] # l(11) -> o(14)
                        new_row[15] = row[12] # m(12) -> p(15)
                        new_row[16] = row[13] # n(13) -> q(16)
                        new_row[17] = row[14] # o(14) -> r(17)
                        new_row[18] = row[15] # p(15) -> s(18)
                        new_row[19] = row[16] # q(16) -> t(19)
                        new_row[20] = row[17] # r(17) -> u(20)
                        
                        writer.writerow(new_row)

        logging.info(f"Знайдено {new_products_count} нових товарів. Дані записано у файл, вказаний за ключем 'csv_path_supliers_1_new'.")
    
    except FileNotFoundError as e:
        logging.info(f"Помилка: Файл не знайдено - {e}")
    except Exception as e:
        logging.info(f"Виникла непередбачена помилка: {e}")