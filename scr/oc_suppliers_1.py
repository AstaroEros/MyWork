import csv
import logging
from scr.oc_base_function import (
    oc_log_message,
    load_oc_settings
)

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