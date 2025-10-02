import argparse
from scr.base_function import check_version, check_csv_data
from scr.products import export_products, download_supplier_price_list, process_supplier_1_price_list, \
                        process_supplier_2_price_list, process_supplier_3_price_list, process_and_combine_all_data, \
                        prepare_for_website_upload, update_products
from scr.suppliers_1 import find_new_products, find_product_data, parse_product_attributes, apply_final_standardization, \
                        fill_product_category, refill_product_category, separate_existing_products, assign_new_sku_to_products, \
                        download_images_for_product, create_new_products_import_file, update_existing_products_batch


def main():
    """
    Основна функція для обробки аргументів командного рядка та запуску відповідних функцій.
    """
    # 1. Створення парсера
    parser = argparse.ArgumentParser(description="Інструмент для автоматизації оновлення WooCommerce.")
    
    # 2. Додавання аргументів
    parser.add_argument(
        "--export", 
        action="store_true", 
        help="Експортувати всі товари магазину у CSV файл."
    )
    parser.add_argument(
        "--check-version", 
        action="store_true", 
        help="Перевірити версію WooCommerce REST API."
    )

    # Оновлений аргумент для перевірки CSV
    parser.add_argument(
        "--check-csv",
        type=str,
        help="Перевірити CSV-файл за ID профілю (наприклад, '1')."
    )

    parser.add_argument(
        "--download-supplier",
        nargs="?",
        const=1,  # За замовчуванням ID постачальника = 1
        type=int,
        help="Завантажити прайс-лист від постачальника за його ID (наприклад, --download-supplier 1)."
    )

    parser.add_argument(
        "--process-supplier-1",
        action="store_true",
        help="Обробка прайс-листа для постачальника 1."
    )

    parser.add_argument(
        "--process-supplier-2",
        action="store_true",
        help="Обробка прайс-листа для постачальника 2."
    )

    parser.add_argument(
        "--process-supplier-3",
        action="store_true",
        help="Обробка прайс-листа для постачальника 3 (конвертація .xls в .csv)."
    )

    parser.add_argument(
        "--combine-tables",
        action="store_true",
        help="Об'єднати всі прайси та залишки в одну зведену таблицю."
    )
    # Додаємо новий аргумент для підготовки файлу для завантаження
    parser.add_argument(
        "--prepare-upload",
        action="store_true",
        help="Підготувати файл для завантаження на сайт."
    )
    # Додано новий аргумент для запуску функції оновлення товарів
    parser.add_argument(    
        "--update-products", 
        type=int, 
        choices=[1, 2],
        help="Запустіть оновлення товарів: 1 для залишків/цін, 2 для акцій.")

    # ✨ Новий аргумент для пошуку нових товарів
    parser.add_argument(
        "--find-new-products",
        action="store_true",
        help="Знайти нові товари у прайс-листах, яких немає на сайті."
    )

    # ✨ Додаємо новий аргумент для пошуку даних про товар
    parser.add_argument(
        "--find-product-data",
        action="store_true",
        help="Знайти URL, штрих-код та атрибути для нових товарів."
    )


    parser.add_argument(
        "--parse-attributes",
        action="store_true",
        help="Парсити сторінки товарів для вилучення атрибутів."
    )

    # === НОВИЙ АРГУМЕНТ для фінальної стандартизації ===
    parser.add_argument(
        "--standardize-final",
        action="store_true",
        help="Застосувати фінальні правила заміни з attribute.csv до SL_new.csv."
    )

    # ✨ НОВИЙ АРГУМЕНТ для заповнення категорій
    parser.add_argument(
        "--fill-categories",
        action="store_true",
        help="Заповнити колонку Q (Категорія) на основі комбінацій M, N, O та category.csv."
    )

    # ✨ НОВИЙ АРГУМЕНТ для ПОВТОРНОГО заповнення категорій
    parser.add_argument( # <--- ДОДАНО
        "--refill-category",
        action="store_true",
        help="Повторно заповнити колонки Категорія (Q) та pa_used (AV) на основі оновлених правил у category.csv."
    )

    # ✨ НОВИЙ АРГУМЕНТ для звірки штрихкодів
    parser.add_argument( # <--- ДОДАНО
    "--separate-existing",
    action="store_true",
    help="Звірити SL_new.csv з базою (zalishki.csv) за штрихкодом, перенести існуючі товари у SL_old_prod_new_SHK.csv та видалити їх з SL_new.csv."
    )

    # ✨ НОВИЙ АРГУМЕНТ для присвоєння нових SKU
    parser.add_argument(
    "--assign-sku",
    action="store_true",
    help="Знайти найбільший SKU у zalishki.csv та присвоїти послідовні SKU новим товарам у SL_new.csv (колонка P/15)."
    )

# ✨ НОВИЙ АРГУМЕНТ для комплексного завантаження зображень
    parser.add_argument(
    "--download-images",
    action="store_true",
    help="Комплексний процес: Завантаження зображень з URL (B/1) у папки категорій (Q/16), перейменування за SKU (P/15), оновлення SL_new.csv (R/17) та сортування GIF-файлів."
    )


    # ✨ НОВИЙ АРГУМЕНТ для створення файлу імпорту нових товарів
    parser.add_argument(
    "--create-import-file",
    action="store_true",
    help="Створює файл SL_new_prod.csv для імпорту нових товарів, очищуючи його та переносячи дані з SL_new.csv."
    )


    # --- НОВА КОМАНДА ДЛЯ ОНОВЛЕННЯ ІСНУЮЧИХ ТОВАРІВ ---
    parser.add_argument('--update-old-products', 
    action='store_true', 
    help='Завантажити дані з SL_old_prod_new_SHK.csv і оновити існуючі товари у базі.'
    )
    

    # 3. Парсинг аргументів
    args = parser.parse_args()

    # 4. Вибір функції для запуску на основі аргументів
    if args.export:
        print("🚀 Запускаю експорт товарів...")
        export_products()
    elif args.check_version:
        print("🔍 Запускаю перевірку версії WooCommerce...")
        check_version()
    elif args.download_supplier:
        print(f"🌐 Запускаю завантаження прайс-листа постачальника з ID {args.download_supplier}...")
        download_supplier_price_list(args.download_supplier)
    elif args.process_supplier_1:
        print("⚙️ Запускаю обробку прайс-листа постачальника 1...")
        process_supplier_1_price_list()
        # Оновлена логіка для check-csv
    elif args.check_csv:
        print(f"⚙️ Запускаю перевірку CSV-файлу з профілем '{args.check_csv}'...")
        if check_csv_data(args.check_csv):
            print("✅ Перевірка успішна. Файл валідний.")
        else:
            print("❌ Перевірка не пройшла. Перегляньте лог-файл для деталей.")
    elif args.process_supplier_2:
        print("⚙️ Запускаю обробку прайс-листа постачальника 2...")
        process_supplier_2_price_list()
    elif args.process_supplier_3:
        print("⚙️ Запускаю обробку прайс-листа постачальника 3...")
        process_supplier_3_price_list()
    elif args.combine_tables:
        print("⚙️ Запускаю створення зведеної таблиці...")
        process_and_combine_all_data()
    elif args.prepare_upload:
        print("⚙️ Запускаю підготовку файлу для завантаження на сайт...")
        prepare_for_website_upload()
    elif args.update_products: 
        print("📦 Запускаю оновлення товарів на сайті...")
        # ТЕПЕР МИ ПЕРЕДАЄМО ЗНАЧЕННЯ АРГУМЕНТУ ДО ФУНКЦІЇ update_products
        update_products(str(args.update_products))
    
        
    # ✨ Нова логіка для пошуку нових товарів
    elif args.find_new_products:
        print("🔍 Запускаю пошук нових товарів...")
        find_new_products()
    
    # ✨ Додаємо новий elif блок для запуску нової функції
    elif args.find_product_data:
        print("🔍 Запускаю пошук даних про товари...")
        find_product_data()

    elif args.parse_attributes:
        print("⚙️ Запускаю парсинг атрибутів...")
        parse_product_attributes()

    # ✨ Додаємо новий elif блок для фінальної стандартизації
    elif args.standardize_final:
        print("✅ Запускаю фінальну стандартизацію SL_new.csv...")
        apply_final_standardization()

    # ✨ Додаємо новий elif блок для заповнення категорій
    elif args.fill_categories:
        print("🗂️ Запускаю заповнення категорій...")
        fill_product_category()
   
    # ✨ Додаємо новий elif блок для ПОВТОРНОГО заповнення
    elif args.refill_category: # <--- ДОДАНО
        print("🔄 Запускаю повторне заповнення категорій та pa_used...")
        refill_product_category()

    # ✨ Додаємо новий elif блок для звірки та перенесення
    elif args.separate_existing: # <--- ДОДАНО
        print("🔍 Запускаю звірку штрихкодів з базою та перенесення існуючих товарів...")
        separate_existing_products()

    # ✨ Додаємо новий elif блок для присвоєння SKU
    elif args.assign_sku:
        print("🔢 Запускаю присвоєння нових SKU...")
        assign_new_sku_to_products()


    # ✨ Додаємо новий elif блок для завантаження зображень
    elif args.download_images:
        print("🖼️ Запускаю комплексний процес завантаження, перейменування та сортування зображень...")
        download_images_for_product()

    # ✨ Додаємо новий elif блок для створення файлу імпорту
    elif args.create_import_file:
        print("📋 Запускаю створення файлу SL_new_prod.csv...")
        create_new_products_import_file()

    # --- НОВИЙ ВИКЛИК ---
    if args.update_old_products:
        print("⬆️ Починаю пакетне оновлення існуючих товарів...")
        update_existing_products_batch()

    else:
        # Якщо аргументи не вказано, вивести довідку
        print("❌ Не вказано жодної дії. Використайте -h або --help для довідки.")
        parser.print_help()


if __name__ == "__main__":
    main()