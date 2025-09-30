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
from scr.base_function import get_wc_api, load_settings, setup_new_log_file, log_message_to_existing_file, load_attributes_csv, \
                                save_attributes_csv, load_category_csv, save_category_csv, load_poznachky_csv
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

def apply_final_standardization():
    """
    Застосовує фінальні правила стандартизації з attribute.csv до файлу SL_new.csv.
    Замінює атрибути на значення з колонки 'attr_site_name', якщо воно існує.
    Проігноровані атрибути (з порожнім 'attr_site_name') очищаються.
    Атрибути, для яких не знайдено правил, залишаються без змін.
    """
    log_message_to_existing_file()
    logging.info("Починаю фінальну стандартизацію атрибутів у SL_new.csv...")

    settings = load_settings()
    try:
        supliers_new_path = settings['paths']['csv_path_supliers_1_new']
        # Ми використовуємо process_data_columns, щоб знайти назви колонок для логування
        product_data_map = settings['suppliers']['1']['product_data_columns']
    except TypeError as e:
        logging.error(f"Помилка доступу до налаштувань. Перевірте settings.json: {e}")
        return
    
    # Виключення Штрих-коду
    processing_map = product_data_map.copy()
    if "Штрих-код" in processing_map:
        processing_map.pop("Штрих-код")

    # 1. Завантаження фінальної мапи заміни
    # replacements_map: {col_index: {original_value (lower): new_value (attr_site_name)}}
    replacements_map, _ = load_attributes_csv()
    
    # 2. Обробка файлу SL_new.csv
    try:
        temp_file_path = supliers_new_path + '.final_temp'
        
        with open(supliers_new_path, mode='r', encoding='utf-8') as input_file, \
             open(temp_file_path, mode='w', encoding='utf-8', newline='') as output_file:
            
            reader = csv.reader(input_file)
            writer = csv.writer(output_file)
            
            headers = next(reader)
            writer.writerow(headers)
            
            # Словник для швидкого пошуку назви колонки за індексом для логування
            column_indices = {index: col_name for col_name, index in processing_map.items()}
            
            for idx, row in enumerate(reader):
                
                # Перевірка та розширення рядка
                max_index = max(product_data_map.values(), default=0)
                if len(row) <= max_index:
                    row.extend([''] * (max_index + 1 - len(row)))

                for col_index, rules in replacements_map.items():
                    
                    # 3. Перевірка та застосування правил
                    if col_index < len(row):
                        current_value = row[col_index].strip()
                        
                        if not current_value:
                            continue # Пропускаємо порожні клітинки

                        original_value_lower = current_value.lower()
                        col_name = column_indices.get(col_index, f"I={col_index}")
                        
                        # Знаходимо стандартизоване значення (attr_site_name)
                        new_value = rules.get(original_value_lower)
                        
                        # Перевірка: чи ЗНАЙДЕНО правило заміни (new_value is not None)
                        if new_value is not None:
                            
                            # A) Якщо new_value (attr_site_name) НЕ порожнє - застосовуємо заміну
                            if new_value:
                                # Заміна лише якщо значення відрізняється
                                if new_value != current_value:
                                    row[col_index] = new_value
                                    logging.info(f"Рядок {idx + 2}: ЗАМІНА ({col_name}): '{current_value}' -> '{new_value}'")
                                else:
                                    # Значення вже відповідає стандарту (new_value == current_value)
                                    logging.debug(f"Рядок {idx + 2}: ЗНАЙДЕНО ({col_name}): Значення '{current_value}' вже стандартизовано.")
                            
                            # B) Якщо new_value (attr_site_name) ПОРОЖНЄ (вирішено ігнорувати/очистити)
                            else:
                                if current_value:
                                    row[col_index] = "" # Очищаємо поле
                                    logging.warning(f"Рядок {idx + 2}: ІГНОРУВАННЯ/ОЧИЩЕННЯ ({col_name}): Значення '{current_value}' очищено згідно з attribute.csv.")

                        # Якщо правило НЕ ЗНАЙДЕНО в replacements_map
                        else:
                            # Це означає, що атрибут не був раніше знайдений і доданий,
                            # або ж є проблема з його форматуванням (пробіли, коми/крапки).
                            # Ми залишаємо його без змін (якщо він був заповнений) і логуємо попередження.
                            logging.warning(
                                f"Рядок {idx + 2}: НЕ ЗНАЙДЕНО ПРАВИЛО ({col_name}): Значення '{current_value}' "
                                f"залишено без змін, оскільки відсутнє у attribute.csv."
                            )


                writer.writerow(row)

        os.replace(temp_file_path, supliers_new_path)
        logging.info("Фінальна стандартизація завершена. SL_new.csv оновлено.")

    except FileNotFoundError as e:
        logging.error(f"Помилка: Файл не знайдено - {e}")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
    except Exception as e:
        logging.error(f"Виникла непередбачена помилка під час фіналізації: {e}")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

def fill_product_category():
    """
    1. Заповнює колонку Q (Категорія) на основі комбінацій M, N, O.
    2. Заповнює колонку T (Позначки) на основі співпадінь у назві товару (G).
    3. Заповнює колонку U (Rank Math Focus Keyword) очищеною назвою товару (G).
    4. Заповнює колонки V, W, X, Y, AZ фіксованими значеннями.
    5. Копіює перший абзац з H до Z (з урахуванням літерального \n).
    6. Встановлює поточну дату у AX.
    7. Встановлює значення pa_used із category.csv у колонку AV.
    Працює лише з ID постачальника = 1.
    """
    log_message_to_existing_file()
    logging.info("Починаю фінальне заповнення службових колонок у csv...")

    settings = load_settings()
    try:
        supliers_new_path = settings['paths']['csv_path_supliers_1_new'] 
        FIXED_SUPPLIER_ID = 1 
        
        # Отримання значення для колонки V (name_ukr)
        name_ukr_value = settings['suppliers']['1']['name_ukr'] 
        
        # Індекси колонок csv:
        name_1_index = 12       # M
        name_2_index = 13       # N
        name_3_index = 14       # O
        product_name_index = 6  # G (Ім'я товару)
        description_index = 7   # H (Опис)
        category_index = 16     # Q (Категорія)
        poznachky_index = 19    # T (Позначки)
        rank_math_index = 20    # U (Rank Math Focus Keyword)
        
        # --- НОВІ ІНДЕКСИ (ВНЕСЕНІ ЗМІНИ) ---
        v_index = 21            # V (name_ukr)
        w_index = 22            # W (фіксоване "0")
        x_index = 23            # X (фіксоване "yes")
        y_index = 24            # Y (фіксоване "none")
        z_index = 25            # Z (Короткий опис)
        av_index = 47           # AV (pa_used)
        ax_index = 49           # AX (Дата)
        az_index = 51           # AZ (Тип товару, "simple")
        # ---------------------
        
        # Оновлюємо максимальний індекс
        max_col_index = max(name_1_index, name_2_index, name_3_index, category_index, product_name_index, poznachky_index, rank_math_index, v_index, w_index, x_index, y_index, z_index, av_index, ax_index, az_index, description_index)
        
    except (TypeError, KeyError) as e:
        logging.error(f"Помилка доступу до налаштувань. Перевірте settings.json: {e}")
        return

    # Завантаження правил категорій та позначок
    category_map, raw_data_category = load_category_csv()
    changes_made_category = False 
    max_raw_row_len_category = len(raw_data_category[0]) if raw_data_category and raw_data_category[0] else 5
    rules_category = category_map.get(FIXED_SUPPLIER_ID, {})
    poznachky_list = load_poznachky_csv() 
    
    # === СТВОРЕННЯ МАПИ ДЛЯ AV (pa_used) ===
    pa_used_map = {}
    supplier_id_str = str(FIXED_SUPPLIER_ID)
    
    # category.csv має колонки: postachalnyk(0), name_1(1), name_2(2), name_3(3), category(4), pa_used(5)
    for row in raw_data_category:
        # Перевіряємо, чи рядок має мінімум 6 колонок
        if len(row) > 5:
            postachalnyk_value = row[0].strip()
            
            # ВАЖЛИВА ЗМІНА: Додаємо правило, якщо постачальник - "1" АБО порожній ""
            is_valid_supplier = (postachalnyk_value == supplier_id_str) or (postachalnyk_value == '')
            
            if is_valid_supplier:
                # Ключ: (name_1, name_2, name_3) у нижньому регістрі
                key = tuple(v.strip().lower() for v in row[1:4])
                # Значення: pa_used (індекс 5)
                # Якщо правило з ID=1 вже є, порожнє правило його НЕ замінить.
                # Якщо пусте правило вже є, правило з ID=1 його ЗАМІНИТЬ (якщо ми йдемо по порядку CSV).
                # Оскільки rules_category (для Q) використовує логіку злиття, ми просто додаємо.
                pa_used_map[key] = row[5].strip()
            
    logging.info(f"Створено pa_used_map. Знайдено {len(pa_used_map)} правил (включно з правилами без ID постачальника).")
    # =======================================

    # Визначаємо поточну дату у потрібному форматі (AX)
    current_date_str = datetime.now().strftime('%Y-%m-%dT00:00:00') 
    
    # === ХЕЛПЕР: Пошук місця для вставки нового правила категорії ===
    def get_category_insertion_point(supplier_id, raw_data):
        insert_row_index = len(raw_data)
        found_block = False
        
        for i, row in enumerate(raw_data):
            if row and row[0].strip().isdigit():
                try:
                    current_id = int(row[0].strip())
                    
                    if current_id == supplier_id:
                        found_block = True
                        insert_row_index = i + 1 
                    
                    elif current_id > supplier_id and found_block:
                        return i
                    
                except ValueError:
                    continue
            
            elif found_block:
                insert_row_index = i + 1 

        return insert_row_index
    # ===============================================================

    try:
        temp_file_path = supliers_new_path + '.category_temp' 
        
        with open(supliers_new_path, mode='r', encoding='utf-8') as input_file, \
             open(temp_file_path, mode='w', encoding='utf-8', newline='') as output_file:
            
            reader = csv.reader(input_file)
            writer = csv.writer(output_file)
            
            headers = next(reader)
            writer.writerow(headers)
            

            for idx, row in enumerate(reader):
                
                # Гарантуємо, що рядок достатньої довжини
                if len(row) <= max_col_index:
                    row.extend([''] * (max_col_index + 1 - len(row)))

                # Отримання назви товару та опису
                product_name = row[product_name_index].strip()
                product_description = row[description_index]
                
                # Обчислюємо ключ, який буде використаний і для Q, і для AV
                key_values = (row[name_1_index].strip(), row[name_2_index].strip(), row[name_3_index].strip())
                search_key = tuple(v.lower() for v in key_values)
                
                # ===============================================
                #           ЛОГІКА ЗАПОВНЕННЯ КАТЕГОРІЇ (Q/16)
                # ===============================================
                category_value = rules_category.get(search_key)
                
                if category_value is not None:
                    if category_value:
                        row[category_index] = category_value
                    else:
                        row[category_index] = ""
                else:
                    insert_index = get_category_insertion_point(FIXED_SUPPLIER_ID, raw_data_category)
                    # NOTE: Тут додається новий рядок без ID постачальника в index 0! 
                    new_raw_row = [''] + list(key_values) + [''] * (max_raw_row_len_category - 4)
                    raw_data_category.insert(insert_index, new_raw_row)
                    rules_category[search_key] = "" # Оновлюємо мапу для уникнення дублікатів
                    changes_made_category = True 
                    logging.warning(f"Рядок {idx + 2}: НОВА КОМБІНАЦІЯ КАТЕГОРІЇ: {key_values} додано.")
                    
                
                # ===============================================
                #           ЛОГІКА ЗАПОВНЕННЯ ПОЗНАЧОК (T/19)
                # ===============================================
                found_tags = []
                if product_name and poznachky_list:
                    search_name = product_name.lower()
                    covered_ranges = [] 
                    
                    for tag in poznachky_list:
                        tag_len = len(tag)
                        if tag in search_name:
                            start_index = search_name.find(tag)
                            end_index = start_index + tag_len
                            
                            is_covered = False
                            for covered_start, covered_end in covered_ranges:
                                if start_index >= covered_start and end_index <= covered_end:
                                    is_covered = True
                                    break
                                
                            if not is_covered:
                                found_tags.append(tag.capitalize()) 
                                covered_ranges.append((start_index, end_index))
                                covered_ranges.sort(key=lambda x: x[1] - x[0], reverse=True)

                    if found_tags:
                        row[poznachky_index] = ', '.join(found_tags)
                        
                        
                # ===============================================
                #           ЛОГІКА ЗАПОВНЕННЯ RANK MATH (U/20)
                # ===============================================
                if product_name:
                    cleaned_name = re.sub(r'[а-яА-Я]', '', product_name)
                    cleaned_name = re.sub(r'[0-9]', '', cleaned_name)
                    cleaned_name = re.sub(r'[^a-zA-Z\s]', '', cleaned_name)
                    cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()
                    row[rank_math_index] = cleaned_name


                # ===============================================
                #           ЛОГІКА ЗАПОВНЕННЯ AV (pa_used)
                # ===============================================
                pa_used_value = pa_used_map.get(search_key)
                
                if pa_used_value:
                    row[av_index] = pa_used_value
                    logging.info(f"Рядок {idx + 2}: AV (pa_used) УСПІШНО ЗАПОВНЕНО значенням: '{pa_used_value}'. Ключ: {search_key}")
                else:
                    logging.warning(f"Рядок {idx + 2}: AV (pa_used) не заповнено. Шуканий ключ: {search_key}. (Знайдено правил: {len(pa_used_map)})")

                
                # ===============================================
                #           ЛОГІКА ЗАПОВНЕННЯ ФІКСОВАНИХ КОЛОНОК
                # ===============================================
                
                # V (21): Значення з settings.json
                row[v_index] = name_ukr_value
                
                # W (22): Фіксоване "0"
                row[w_index] = "0"
                
                # X (23): Фіксоване "yes"
                row[x_index] = "yes"
                
                # Y (24): Фіксоване "none"
                row[y_index] = "none"
                
                # AZ (51): Фіксоване "simple"
                row[az_index] = "simple"
                
                # AX (49): Сьогоднішня дата
                row[ax_index] = current_date_str
                
                # Z (25): Копіювати H(7) до першого абзацу
                if product_description:
                    # Шукаємо літеральну послідовність символів '\n'
                    first_paragraph = product_description.split('\\n', 1)[0]
                    
                    row[z_index] = first_paragraph.strip()
                else:
                    row[z_index] = ""
                
                
                writer.writerow(row)

        os.replace(temp_file_path, supliers_new_path)
        logging.info("Заповнення категорій, позначок та ключових слів завершено. SL_new.csv оновлено.")

        if changes_made_category:
            save_category_csv(raw_data_category)
        else:
            logging.info("Збереження category.csv не потрібне. Змін: False.")

    except Exception as e:
        logging.error(f"Виникла непередбачена помилка під час заповнення: {e}")
        if 'supliers_new_path' in locals() and os.path.exists(temp_file_path): 
            os.remove(temp_file_path)

def refill_product_category():
    """
    Повторно заповнює колонки Q (Категорія) та AV (pa_used) 
    на основі оновлених правил у category.csv.
    НЕ додає нові порожні рядки до category.csv.
    """
    log_message_to_existing_file()
    logging.info("Починаю повторне заповнення категорій та pa_used у SL_new.csv...")

    settings = load_settings()
    try:
# ... (блок ініціалізації індексів та змінних без змін) ...
        supliers_new_path = settings['paths']['csv_path_supliers_1_new'] 
        FIXED_SUPPLIER_ID = 1 
        
        # Індекси колонок SL_new.csv:
        name_1_index = 12       # M
        name_2_index = 13       # N
        name_3_index = 14       # O
        category_index = 16     # Q (Категорія)
        av_index = 47           # AV (pa_used)
        
        # Визначаємо максимальний індекс, щоб забезпечити довжину рядка
        max_col_index = max(name_1_index, name_2_index, name_3_index, category_index, av_index)
        
    except (TypeError, KeyError) as e:
        logging.error(f"Помилка доступу до налаштувань. Перевірте settings.json: {e}")
        return

    # Завантаження правил категорій. Нам потрібні category_map та raw_data_category для pa_used.
    category_map, raw_data_category = load_category_csv()
    
    rules_category = {}
    pa_used_map = {}
    supplier_id_str = str(FIXED_SUPPLIER_ID)

    for row in raw_data_category:
        if len(row) > 5:
            postachalnyk_value = row[0].strip()
            
            # Включаємо правила з ID=1 АБО порожнім ID
            is_valid_supplier = (postachalnyk_value == supplier_id_str) or (postachalnyk_value == '')
            
            if is_valid_supplier:
                key = tuple(v.strip().lower() for v in row[1:4])
                
                # Мапа для Категорії (Q)
                if len(row) > 4:
                    rules_category[key] = row[4].strip() 
                
                # Мапа для pa_used (AV)
                if len(row) > 5:
                    pa_used_map[key] = row[5].strip()
    
    logging.info(f"Зчитано {len(rules_category)} правил для Категорії (Q) та {len(pa_used_map)} правил для pa_used (AV).")


    try:
        temp_file_path = supliers_new_path + '.refill_temp' 
        updated_rows_count = 0
        
        with open(supliers_new_path, mode='r', encoding='utf-8') as input_file, \
             open(temp_file_path, mode='w', encoding='utf-8', newline='') as output_file:
            
            reader = csv.reader(input_file)
            writer = csv.writer(output_file)
            
            headers = next(reader)
            writer.writerow(headers)
            

            for idx, row in enumerate(reader):
                
                if len(row) <= max_col_index:
                    row.extend([''] * (max_col_index + 1 - len(row)))

                # 1. Готуємо ключ пошуку (M, N, O)
                key_values = (row[name_1_index].strip(), row[name_2_index].strip(), row[name_3_index].strip())
                search_key = tuple(v.lower() for v in key_values)
                
                initial_category = row[category_index].strip()
                initial_pa_used = row[av_index].strip()
                row_changed = False
                
                # ===============================================
                #           ПОВТОРНЕ ЗАПОВНЕННЯ КАТЕГОРІЇ (Q/16)
                # ===============================================
                category_value = rules_category.get(search_key)
                
                if category_value and category_value != initial_category: # Перевірка, що не порожнє і не те саме
                    row[category_index] = category_value
                    row_changed = True
                    # ЗМІНА ЛОГУВАННЯ
                    logging.info(f"Рядок {idx + 2}: Q (Категорія) оновлено. Ключ: {search_key}. Значення: '{category_value}'")
                
                
                # ===============================================
                #           ПОВТОРНЕ ЗАПОВНЕННЯ AV (pa_used/47)
                # ===============================================
                pa_used_value = pa_used_map.get(search_key)
                
                if pa_used_value and pa_used_value != initial_pa_used: # Перевірка, що не порожнє і не те саме
                    row[av_index] = pa_used_value
                    row_changed = True
                    # ЗМІНА ЛОГУВАННЯ
                    logging.info(f"Рядок {idx + 2}: AV (pa_used) оновлено. Ключ: {search_key}. Значення: '{pa_used_value}'")
                
                if row_changed:
                    updated_rows_count += 1

                writer.writerow(row)

        os.replace(temp_file_path, supliers_new_path)
        logging.info(f"Повторне заповнення завершено. У csv оновлено {updated_rows_count} рядків.")


    except Exception as e:
        logging.error(f"Виникла непередбачена помилка під час повторного заповнення: {e}")
        if 'supliers_new_path' in locals() and os.path.exists(temp_file_path): 
            os.remove(temp_file_path)






