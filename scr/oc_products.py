import csv
import html
import os
from scr.oc_base_function import (
    oc_setup_new_log_file,
    oc_log_message,
    oc_connect_db,
    load_oc_settings
)


def oc_export_products():

    # 1. Створюємо новий лог
    oc_setup_new_log_file()
    oc_log_message("▶ Старт експорту товарів OpenCart")

    # 2. Завантажуємо налаштування
    settings = load_oc_settings()
    if not settings or "presets" not in settings:
        print("❌ Не знайдено пресети в oc_settings.json")
        return

    presets = settings["presets"]
    csv_path = settings.get("csv", {}).get("output_file", None)

    # 3. Запитуємо пресет у користувача
    print("\nВиберіть пресет для експорту:\n")

    for key, preset in presets.items():
        print(f"{key} - {preset['name']}")

    preset_id = input("\nВаш вибір: ").strip()
    

    if preset_id not in presets:
        oc_log_message(f"❌ Невідомий пресет: {preset_id}")
        print("Помилка: неправильний номер пресету.")
        return

    sql = presets[preset_id]["sql"]
    preset_name = presets[preset_id]["name"]

    oc_log_message(f"▶ Обраний пресет {preset_id}: {preset_name}")

    # 4. Підключення до бази
    conn = oc_connect_db()
    if not conn:
        oc_log_message("❌ Неможливо підключитися до БД")
        return

    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    # 5. Підготовка CSV-файлу
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # 6. Запис CSV — з декодуванням HTML
    if rows:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=list(rows[0].keys()),
                quoting=csv.QUOTE_MINIMAL,  # ← Заголовки без лапок, дані — тільки коли треба
                delimiter=",",              # або ";" — як хочеш
                escapechar="\\"
            )

            writer.writeheader()

            for row in rows:
                decoded_row = {
                    k: html.unescape(v) if isinstance(v, str) else v
                    for k, v in row.items()
                }

                writer.writerow(decoded_row)

        oc_log_message(f"✔ Експорт виконано: {len(rows)} записів")
        print(f"Готово! Записано {len(rows)} рядків у {csv_path}")
    else:
        oc_log_message("⚠ Результат пустий")
        print("Немає записів для експорту.")