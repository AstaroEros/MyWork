import argparse

from scr.oc_base_function import oc_import_categories_from_csv, check_csv_data
from scr.oc_products import oc_export_products, download_supplier_price_list, \
                            process_supplier_1_price_list, process_supplier_2_price_list, process_supplier_3_price_list
from scr.oc_suppliers_1 import find_new_products, find_change_art_shtrihcod, find_product_url, parse_product_attributes, apply_final_standardization, \
                                fill_product_category, refill_product_category, separate_existing_products, assign_new_sku_to_products


def main():
    """
    Основна функція для обробки аргументів командного рядка та запуску OpenCart-інструментів.
    """
    # 1. Створення парсера
    parser = argparse.ArgumentParser(
        description="Інструмент для автоматизації оновлення OpenCart."
    )

    # 2. Додавання аргументів

    parser.add_argument(
        "--oc-export",
        action="store_true",
        help="Експортувати товари OpenCart у CSV файл згідно з обраним пресетом."
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

    # Оновлений аргумент для перевірки CSV
    parser.add_argument(
        "--check-csv",
        type=str,
        help="Перевірити CSV-файл за ID профілю (наприклад, '1')."
    )

    # ✨ Новий аргумент для пошуку нових товарів
    parser.add_argument(
        "--find-new-products",
        action="store_true",
        help="Знайти нові товари у прайс-листах, яких немає на сайті."
    )

    # 🆕 ПЕРЕВІРКА АРТИКУЛІВ І ШТРИХКОДІВ
    parser.add_argument(
        "--find-change-art-shtrihcod",
        action="store_true",
        help="Знайти товари з розбіжностями між артикулами та штрихкодами (сайт ↔ постачальник)."
    )

    # ✨ Додаємо новий аргумент для пошуку даних про товар
    parser.add_argument(
        "--find-product-url",
        action="store_true",
        help="Знайти URL нових товарів."
    )

    # ✨ Додаємо новий аргумент для парсингу атрибутів
    parser.add_argument(
        "--parse-attributes",
        action="store_true",
        help="Парсити сторінки товарів для вилучення атрибутів."
    )

    # ✨ Додаємо новий аргумент для імпорту категорій з CSV                        
    parser.add_argument(
    "--import-categories",
    action="store_true",
    help="Імпортувати категорії з CSV напряму в БД OpenCart."
    )

    # ✨ НОВИЙ АРГУМЕНТ для фінальної стандартизації
    parser.add_argument(
    "--standardize-final",
    action="store_true",
    help="Застосувати фінальні правила заміни з attribute.csv до new.csv."
    )
    
    # ✨ НОВИЙ АРГУМЕНТ для заповнення категорій
    parser.add_argument(
    "--fill-categories",
    action="store_true",
    help="Заповнити колонку Q (Категорія) на основі комбінацій M, N, O та category.csv."
    )

    # ✨ НОВИЙ АРГУМЕНТ для ПОВТОРНОГО заповнення категорій
    parser.add_argument( 
    "--refill-category",
    action="store_true",
    help="Повторно заповнити колонки Категорія (Q) та pa_used (AV) на основі оновлених правил у category.csv."
    )

    # ✨ НОВИЙ АРГУМЕНТ для звірки штрихкодів
    parser.add_argument( 
    "--separate-existing",
    action="store_true",
    help="Звірити SL_new.csv з базою (zalishki.csv) за штрихкодом, перенести існуючі товари у old_prod_new_SHK.csv та видалити їх з SL_new.csv."
    )
    
    # ✨ НОВИЙ АРГУМЕНТ для присвоєння нових SKU
    parser.add_argument(
    "--assign-sku",
    action="store_true",
    help="Знайти найбільший SKU у zalishki.csv та присвоїти послідовні SKU новим товарам у new.csv (колонка P/15)."
    )

    # 3. Парсинг аргументів
    args = parser.parse_args()

    # 4. Вибір функції для запуску

    if args.oc_export:
        print("🚀 Запускаю експорт товарів OpenCart...")
        oc_export_products()
    elif args.check_csv:
        print(f"⚙️ Запускаю перевірку CSV-файлу з профілем '{args.check_csv}'...")
        if check_csv_data(args.check_csv):
            print("✅ Перевірка успішна. Файл валідний.")
        else:
            print("❌ Перевірка не пройшла. Перегляньте лог-файл для деталей.")
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
    elif args.find_new_products:
        print("🔍 Запускаю пошук нових товарів...")
        find_new_products()
    elif args.find_change_art_shtrihcod:
        print("🔎 Перевірка розбіжностей артикулів і штрихкодів...")
        find_change_art_shtrihcod()
    elif args.find_product_url:
        print("🔍 Запускаю пошук урл товару...")
        find_product_url()
    elif args.parse_attributes:
        print("⚙️ Запускаю парсинг атрибутів...")
        parse_product_attributes()
    elif args.import_categories:
        print("📂 Імпорт категорій у OpenCart...")
        oc_import_categories_from_csv()
    elif args.standardize_final:
        print("✅ Запускаю фінальну стандартизацію new_product.csv...")
        apply_final_standardization()
    elif args.fill_categories:
        print("🗂️ Запускаю заповнення категорій...")
        fill_product_category()
    elif args.refill_category: 
        print("🔄 Запускаю повторне заповнення категорій та pa_used...")
        refill_product_category()
    elif args.separate_existing: 
        print("🔍 Запускаю звірку штрихкодів з базою та перенесення існуючих товарів...")
        separate_existing_products()
    elif args.assign_sku:
        print("🔢 Запускаю присвоєння нових SKU...")
        assign_new_sku_to_products()

    else:
        print("❌ Не вказано жодної дії. Використайте --help для отримання списку команд.\n")
        parser.print_help()


if __name__ == "__main__":
    main()
