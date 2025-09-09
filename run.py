import argparse
from scr.updater import check_version
from scr.products import export_products, check_exported_csv, download_supplier_price_list, process_supplier_1_price_list, \
                        process_supplier_2_price_list, process_supplier_3_price_list, process_and_combine_all_data
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

    parser.add_argument(
        "--check-csv",
        action="store_true",
        help="Перевірити та відсортувати експортований CSV файл."
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
    # Сюди можна буде додати інші аргументи, наприклад, для імпорту
    # parser.add_argument(
    #     "--import-products", 
    #     action="store_true", 
    #     help="Імпортувати товари з файлів постачальників."
    # )

    # 3. Парсинг аргументів
    args = parser.parse_args()

    # 4. Вибір функції для запуску на основі аргументів
    if args.export:
        print("🚀 Запускаю експорт товарів...")
        export_products()
    elif args.check_version:
        print("🔍 Запускаю перевірку версії WooCommerce...")
        check_version()
    elif args.check_csv:
        print("⚙️ Запускаю перевірку експортованого CSV...")
        check_exported_csv()
    elif args.download_supplier:
        print(f"🌐 Запускаю завантаження прайс-листа постачальника з ID {args.download_supplier}...")
        download_supplier_price_list(args.download_supplier)
    elif args.process_supplier_1:
        print("⚙️ Запускаю обробку прайс-листа постачальника 1...")
        process_supplier_1_price_list()
    elif args.process_supplier_2:
        print("⚙️ Запускаю обробку прайс-листа постачальника 2...")
        process_supplier_2_price_list()
    elif args.process_supplier_3:
        print("⚙️ Запускаю обробку прайс-листа постачальника 3...")
        process_supplier_3_price_list()
    elif args.combine_tables:
        print("⚙️ Запускаю створення зведеної таблиці...")
        process_and_combine_all_data()
    # elif args.import_products:
    #     # print("📦 Запускаю імпорт товарів...")
    #     # import_products() # Цю функцію ми створимо пізніше
    else:
        # Якщо аргументи не вказано, вивести довідку
        print("❌ Не вказано жодної дії. Використайте -h або --help для довідки.")
        parser.print_help()

if __name__ == "__main__":
    main()