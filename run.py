import argparse
from scr.base_function import check_version, check_csv_data, export_product_by_id, update_image_seo_by_sku, translate_csv_to_ru, \
                        log_global_attributes, convert_local_attributes_to_global, test_search_console_access, \
                        check_and_index_url_in_google, process_indexing_for_new_products
from scr.products import export_products, download_supplier_price_list, process_supplier_1_price_list, \
                        process_supplier_2_price_list, process_supplier_3_price_list, process_and_combine_all_data, \
                        prepare_for_website_upload, update_products
from scr.suppliers_1 import find_new_products, find_product_data, parse_product_attributes, apply_final_standardization, \
                        fill_product_category, refill_product_category, separate_existing_products, assign_new_sku_to_products, \
                        download_images_for_product, create_new_products_import_file, update_existing_products_batch, \
                        create_new_products_batch, update_image_seo_from_csv, translate_and_prepare_new_prod_csv, \
                        upload_ru_translation_to_wp, fill_wpml_translation_group, update_image_seo_ru_from_csv


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

    # ✨ Додаємо новий аргумент для парсингу атрибутів
    parser.add_argument(
        "--parse-attributes",
        action="store_true",
        help="Парсити сторінки товарів для вилучення атрибутів."
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
    parser.add_argument( # <--- ДОДАНО
        "--refill-category",
        action="store_true",
        help="Повторно заповнити колонки Категорія (Q) та pa_used (AV) на основі оновлених правил у category.csv."
    )

    # ✨ НОВИЙ АРГУМЕНТ для звірки штрихкодів
    parser.add_argument( # <--- ДОДАНО
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

    # ✨ НОВИЙ АРГУМЕНТ для комплексного завантаження зображень
    parser.add_argument(
    "--download-images",
    action="store_true",
    help="Комплексний процес: Завантаження зображень з URL (B/1) у папки категорій (Q/16), перейменування за SKU (P/15), оновлення new.csv (R/17) та сортування GIF-файлів."
    )

    # ✨ НОВИЙ АРГУМЕНТ для створення файлу імпорту нових товарів
    parser.add_argument(
    "--create-import-file",
    action="store_true",
    help="Створює файл SL_new_prod.csv для імпорту нових товарів, очищуючи його та переносячи дані з new.csv."
    )

    # ✨ НОВА КОМАНДА ДЛЯ ОНОВЛЕННЯ ІСНУЮЧИХ ТОВАРІВ
    parser.add_argument('--update-old-products', 
    action='store_true', 
    help='Завантажити дані з SL_old_prod_new_SHK.csv і оновити існуючі товари у базі.'
    )
    
    # ✨ НОВА КОМАНДА ДЛЯ СТВОРЕННЯ НОВИХ ТОВАРІВ
    parser.add_argument('--create-new-products', 
        action='store_true', 
        help='Завантажити дані з SL_new_prod.csv і створити нові товари у базі.'
    )
    
    # ✨ НОВА КОМАНДА ДЛЯ ПАРСИНГУ ВСІХ ДАНИХ ТОВАРУ ПО ЙОГО ID
    parser.add_argument("--export-product-by-id", 
        action="store_true", 
        help="Експорт усіх даних товару за ID у CSV"
    )

    # ✨ НОВА КОМАНДА ДЛЯ ОНОВЛЕННЯ SEO-АТРИБУТІВ ЗОБРАЖЕНЬ
    parser.add_argument(
        "--update-image-seo",
        action="store_true",
        help="Оновити SEO-атрибути зображень за SKU."
    )

    # ✨ НОВА КОМАНДА ДЛЯ ПЕРЕКЛАДУ CSV НА РОСІЙСЬКУ
    parser.add_argument(
        "--translate-ru",
        action="store_true",
        help="Перекласти CSV SL_new_prod.csv на російську через DeepL"
    )

    # ✨ НОВА КОМАНДА ДЛЯ ОНОВЛЕННЯ SEO-АТРИБУТІВ ЗОБРАЖЕНЬ З CSV
    parser.add_argument(
        "--update-image-seo-from-csv",
        action="store_true",
        help="Оновлює SEO-атрибути зображень товарів з CSV (csv_path_sl_new_prod) використовуючи seo_tag."
    )

    # ✨ НОВА КОМАНДА ДЛЯ ПЕРЕКЛАДУ НОВОГО CSV ТА ПІДГОТОВКИ ЙОГО ДЛЯ ЗАВАНТАЖЕННЯ НА САЙТ
    parser.add_argument(
        "--translate-new-prod",
        action="store_true",
        help="Перекласти name, content та short_description нового CSV і підготувати для завантаження на сайт."
    )

    # ✨ НОВА КОМАНДА ДЛЯ ЗАВАНТАЖЕННЯ RU ПЕРЕКЛАДІВ НА САЙТ ЧЕРЕЗ WOOCOMMERCE + WPML
    parser.add_argument(
        "--upload-ru-translations",
        action="store_true",
        help="Завантажити RU переклад товарів на сайт через WooCommerce + WPML."
    )
    
    # ✨ НОВА КОМАНДА ДЛЯ ОНОВЛЕННЯ SEO-АТРИБУТІВ ЗОБРАЖЕНЬ RU З CSV
    parser.add_argument(
        "--update-image-seo-ru-from-csv",
        action="store_true",
        help="Оновити RU SEO-атрибути головного зображення за CSV (без перезапису UA)."
    )

    # ✨ НОВИЙ АРГУМЕНТ для завантаження перекладів WPML
    parser.add_argument(
        "--fill-wpml-translation-group",
        action="store_true", # Використовуємо прапорець, бо шлях у settings.json
        help="Завантажити російські переклади з CSV-файлу, шлях до якого вказано у settings.json (paths.csv_path_sl_new_prod_ru)."
    )

    # ✨ Нова команда для виведення глобальних атрибутів у лог
    parser.add_argument(
        "--list-global-attributes",
        action="store_true",
        help="Вивести список глобальних атрибутів WooCommerce у лог."
    )

    # ✨ Нова команда для конвертації локальних атрибутів у глобальні
    parser.add_argument(
        "--convert-local-attributes-to-global",
        action="store_true",
        help="Вивести список глобальних атрибутів WooCommerce у лог."
    )

    # ✨ Нова команда для перевірки підключення до Google Search Console API
    parser.add_argument(
        "--check-searchconsole",
        action="store_true",
        help="Перевірити підключення до Google Search Console API."
    )

    parser.add_argument(
        "--check-url-index",
        action="store_true",
        help="Перевірити сторінку на індексацію та за потреби надіслати у Google."
    )

    parser.add_argument(
        "--index-new-products",
        action="store_true",
        help="Перевірити індексацію нових товарів і відправити відсутні сторінки на індексацію."
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
    elif args.update_old_products:
        print("⬆️ Починаю пакетне оновлення існуючих товарів...")
        update_existing_products_batch()

    elif args.create_new_products:
        print("✨ Починаю пакетне створення нових товарів...")
        create_new_products_batch()

    elif args.export_product_by_id:
        print("✨ Запуск функції для парсингу всіх данних товару по його ID...")
        export_product_by_id()

    elif args.update_image_seo:
        print("🖼️ Запускаю оновлення SEO-атрибутів зображень...")
        update_image_seo_by_sku()

    elif args.translate_ru:
        print("🌐 Запускаю переклад CSV new_prod.csv на російську через DeepL...")
        translate_csv_to_ru()

    elif args.update_image_seo_from_csv:
        print("🖼️ Починаю оновлення SEO-атрибутів зображень для всіх SKU з CSV...")
        update_image_seo_from_csv()

    elif args.translate_new_prod:
        print("🔄 Запускаю переклад нового CSV та підготовку для завантаження...")
        translate_and_prepare_new_prod_csv()

    elif args.upload_ru_translations:
        print("🌍 Завантажую RU переклади товарів на сайт...")
        upload_ru_translation_to_wp()
    
    elif args.fill_wpml_translation_group:
        print("🌐 Починаю завантаження російських перекладів...")
        fill_wpml_translation_group() 

    elif args.update_image_seo_ru_from_csv:
        print("🌐 Починаю завантаження російських перекладів...")
        update_image_seo_ru_from_csv()

    elif args.list_global_attributes:
        print("🔍 Отримую список глобальних атрибутів...")
        log_global_attributes()

    elif args.convert_local_attributes_to_global:
        print("🔍 Конвертація локальних атрибутів у глобальні")
        convert_local_attributes_to_global()

    elif args.check_searchconsole:
        print("🌐 Перевіряю доступ до Google Search Console...")
        test_search_console_access()

    elif args.check_url_index:
        print("🔍 Перевіряю сторінку у Search Console...")
        check_and_index_url_in_google()

    elif args.index_new_products:
        print("🌐 Запускаю перевірку індексації нових товарів...")
        process_indexing_for_new_products()

    else:
        # Якщо аргументи не вказано, вивести довідку
        print("❌ Не вказано жодної дії. Використайте -h або --help для довідки.")
        parser.print_help()


if __name__ == "__main__":
    main()