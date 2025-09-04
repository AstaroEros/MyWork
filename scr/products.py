import csv
import json
import os
import time
from scr.updater import get_wc_api


def load_settings():
    """
    Завантаження конфігурації з settings.json
    """
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "settings.json")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def export_products():
    """
    Експорт усіх товарів у CSV пачками по 100.
    """

    # Шлях до CSV
    csv_path = os.path.join(os.path.dirname(__file__), "..", "csv", "input", "zalishki.csv")

    # Заголовки CSV
    headers = [
        "ID", "Артикул", "Назва", "Опубліковано", "Запаси", "Звичайна ціна", "Категорії",
        "Мета: shtrih_cod", "Мета: postachalnyk", "Мета: artykul_lutsk", "Мета: url_lutsk",
        "Мета: artykul_blizklub", "Мета: url_blizklub",
        "Мета: artykul_sexopt", "Мета: url_sexopt",
        "Мета: artykul_biorytm", "Мета: url_biorytm",
        "Мета: artykul_berdiansk"
    ]

    wcapi = get_wc_api()

    start_time = time.time()

    # Отримати загальну кількість товарів
    response = wcapi.get("products", params={"per_page": 1})
    if response.status_code != 200:
        print(f"❌ Помилка {response.status_code} при підрахунку товарів")
        return

    total_products = int(response.headers.get("X-WP-Total", 0))
    print(f"🔎 Загальна кількість товарів: {total_products}")

    exported_count = 0

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        page = 1
        while exported_count < total_products:
            response = wcapi.get(
                "products",
                params={
                    "per_page": 100,
                    "page": page,
                    "_fields": "id,sku,name,status,stock_quantity,regular_price,categories,meta_data"
                }
            )

            if response.status_code != 200:
                print(f"❌ Помилка {response.status_code} на сторінці {page}")
                break

            products = response.json()
            if not products:
                break

            for product in products:
                product["meta_data_dict"] = {m["key"]: m["value"] for m in product.get("meta_data", [])}

                row = [
                    product.get("id"),
                    product.get("sku"),
                    product.get("name"),
                    "yes" if product.get("status") == "publish" else "no",
                    product.get("stock_quantity"),
                    product.get("regular_price"),
                    ", ".join([cat["name"] for cat in product.get("categories", [])]),
                    product["meta_data_dict"].get("shtrih_cod", ""),
                    product["meta_data_dict"].get("postachalnyk", ""),
                    product["meta_data_dict"].get("artykul_lutsk", ""),
                    product["meta_data_dict"].get("url_lutsk", ""),
                    product["meta_data_dict"].get("artykul_blizklub", ""),
                    product["meta_data_dict"].get("url_blizklub", ""),
                    product["meta_data_dict"].get("artykul_sexopt", ""),
                    product["meta_data_dict"].get("url_sexopt", ""),
                    product["meta_data_dict"].get("artykul_biorytm", ""),
                    product["meta_data_dict"].get("url_biorytm", ""),
                    product["meta_data_dict"].get("artykul_berdiansk", "")
                ]
                writer.writerow(row)

                exported_count += 1
                if exported_count % 100 == 0 or exported_count == total_products:
                    elapsed = int(time.time() - start_time)
                    print(f"✅ Вивантажено {exported_count} з {total_products} ({elapsed} сек)")

            page += 1
            time.sleep(1)  # ⏳ Пауза 1 секунда між запитами

    print("🎉 Експорт завершено")