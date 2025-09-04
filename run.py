import argparse
from scr.updater import check_version
from scr.products import export_products, check_exported_csv

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
    # elif args.import_products:
    #     # print("📦 Запускаю імпорт товарів...")
    #     # import_products() # Цю функцію ми створимо пізніше
    else:
        # Якщо аргументи не вказано, вивести довідку
        print("❌ Не вказано жодної дії. Використайте -h або --help для довідки.")
        parser.print_help()

if __name__ == "__main__":
    main()