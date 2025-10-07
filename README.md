# Автоматизація оновлення товарних даних на сайті (MyWork project)

## Table of Contents
- [Українська версія](#українська-версія)
  - [Опис проєкту](#опис-проєкту)
  - [Основні завдання проєкту](#основні-завдання-проєкту)
  - [Структура проєкту](#структура-проєкту)
  - [Логіка роботи та основні блоки](#логіка-роботи-та-основні-блоки)
  - [Використання](#використання)
  - [Логування](#логування)
  - [Технології та навички](#технології-та-навычки)
- [English Version](#english-version)
  - [Project Description](#project-description)
  - [Main Tasks of the Project](#main-tasks-of-the-project)
  - [Project Structure](#project-structure)
  - [Workflow and Main Steps](#workflow-and-main-steps)
  - [Usage](#usage-1)
  - [Logging](#logging-1)
  - [Technologies and Skills](#technologies-and-skills-1)

---

# Українська версія

## Опис проєкту
Цей проєкт — це система автоматизації, розроблена для підтримки актуальності даних про товари на сайті на базі **WooCommerce**. Система автоматично обробляє прайс-листи від різних постачальників, створює єдиний зведений файл та оновлює інформацію про залишки, ціни та акції, а також створює нові товарні позиції з коректними атрибутами та зображеннями безпосередньо через API сайту.

## Основні завдання проєкту
- Вигрузка залишків товарів з сайту
- Завантаження та обробка прайс-листів від різних постачальників
- Поділ даних на існуючі та нові товари
- Парсинг та стандартизація атрибутів, категорій і тегів
- Консолідація даних у єдину зведену таблицю
- Підготовка окремих файлів для оновлення залишків/цін та акційних пропозицій
- Завантаження та прикріплення зображень до товарів
- Оновлення даних на сайті через WooCommerce REST API

## Структура проєкту
```
/var/www/scripts/update/
├── config/
│   ├── settings.example.json
│   └── settings.json
├── csv/
│   ├── source/
│   ├── process/
│   └── output/
├── logs/
│   └── logs.log
├── scr/
│   ├── __init__.py
│   ├── base_function.py
│   └── products.py
├── .env.example
├── README.md
└── run.py
```

## Логіка роботи та основні блоки
1. Вигрузка залишків товарів з сайту
2. Завантаження та обробка прайс-листів
3. Обробка та очищення даних:
   - Пошук та стандартизація назв категорій та атрибутів.
   - Присвоєння SKU для нових товарів.
4. Формування фінальних CSV-файлів:
   - Розділення на товари для оновлення та товари для створення.
5. Завантаження медіа:
   - Завантаження зображень, необхідних для нових товарів.
6. Імпорт у WooCommerce через API:
   - Створення нових товарів.
   - Оновлення існуючих товарів (ціни, атрибути, акції, залишки).

## Використання
Порядок виконання більшості кроків є критичним, оскільки вони залежать від результатів попередніх операцій.

```bash
python3 run.py --export                       # Вигрузка залишків товарів з сайту
python3 run.py --download-supplier 1 (2, 3)   # Завантаження прайслиста постачальника 1, 2 або 3
python3 run.py --process-supplier-1 (2, 3)    # Обробка прайслиста постачальника 1, 2 або 3
python3 run.py --combine-tables               # Об'єднання прайсів та залишків на сайті
python3 run.py --find-product-data            # Збір та збагачення даних для товарів
python3 run.py --parse-attributes             # Парсинг, очищення та стандартизація атрибутів
python3 run.py --fill-categories              # Нормалізація та заповнення назв категорій
python3 run.py --separate-existing            # Розділення зведеної таблиці на файли для оновлення (існуючі) та створення (нові)
python3 run.py --assign-sku                   # Присвоєння SKU новим товарам, що готові до створення
sudo /var/www/scripts/update/venv/bin/python3 run.py --download-images   # Завантаження всіх необхідних зображень на сервер
python3 run.py --create-import-file           # Формування фінальних CSV для імпорту/оновлення
python3 run.py --update-old-products          # Пакетне оновлення існуючих товарів через WooCommerce API
python3 run.py --create-new-products          # Пакетне створення нових товарів через WooCommerce API
```

## Експортувати в Таблиці
Можливість експортувати дані у таблиці (наприклад, Google Sheets або Excel) для подальшого аналізу.

## Логування
Усі операції логуються у динамічні файли за датою та режимом (наприклад, `update_YYYY-MM-DD.log` або `create_YYYY-MM-DD.log`) у директорії:

```
/var/www/scripts/update/logs/
```

## Технології та навички
- Python 3
- WooCommerce REST API: основний метод взаємодії з сайтом
- Обробка даних: csv, os, logging, pandas (якщо використовується)
- Мережа та запити: requests, urllib
- Управління: argparse
- Автоматизація та обробка CSV

---

# English Version

## Project Description
This project is an automation system developed to keep product data on a WooCommerce-based website up to date. The system automatically processes price lists from various suppliers, creates a unified consolidated file, and updates information on stock, prices, and promotions. It also handles the creation of new products, ensuring they have correct attributes and images via the site's API.

## Main Tasks of the Project
- Export of product stock data from the website
- Download and process suppliers’ price lists
- Separation of data into existing and new products
- Parsing and standardization of attributes, categories, and tags
- Consolidate data into a single unified table
- Prepare separate files for stock/price updates and promotional offers
- Download and attach images to products
- Update website data via WooCommerce REST API

## Project Structure
```
/var/www/scripts/update/
├── config/
│   ├── settings.example.json
│   └── settings.json
├── csv/
│   ├── source/
│   ├── process/
│   └── output/
├── logs/
│   └── logs.log
├── scr/
│   ├── __init__.py
│   ├── base_function.py
│   └── products.py
├── .env.example
├── README.md
└── run.py
```

## Workflow and Main Steps
1. Export of product stock data from the website
2. Download and process price lists
3. Data Processing and Cleaning:
   - Parsing attributes and standardizing categories
   - Assigning SKUs for new products
4. Final CSV Generation:
   - Separating data for updating existing items and creating new ones
5. Media Upload:
   - Downloading necessary images to the server
6. WooCommerce API Import:
   - Creating new products
   - Batch updating existing products (price, stock, attributes, etc.)

## Usage
The execution order for most steps is critical as they depend on the results of previous operations.

```bash
python3 run.py --export                       # Export product stock data from the website
python3 run.py --download-supplier 1 (2, 3)   # Download supplier price list 1, 2 or 3
python3 run.py --process-supplier-1 (2, 3)    # Process supplier price list 1, 2 or 3
python3 run.py --combine-tables               # Merge price lists and stock data from the website
python3 run.py --find-product-data            # Gather and enrich data for products
python3 run.py --parse-attributes             # Parse, clean, and standardize attributes
python3 run.py --fill-categories              # Normalize and populate category names
python3 run.py --separate-existing            # Separate the consolidated table into update (existing) and create (new) files
python3 run.py --assign-sku                   # Assign SKU to new products ready for creation
sudo /var/www/scripts/update/venv/bin/python3 run.py --download-images   # Download all necessary images to the server
python3 run.py --create-import-file           # Generate final CSVs for import/update
python3 run.py --update-old-products          # Batch update existing products via WooCommerce API
python3 run.py --create-new-products          # Batch creation of new products via WooCommerce API
```

## Export to Spreadsheets
Ability to export data to spreadsheets (for example, Google Sheets or Excel) for further analysis.

## Logging
All operations are logged in a dynamic file named by date and mode (e.g., `update_YYYY-MM-DD.log` or `create_YYYY-MM-DD.log`) within the directory:

```
/var/www/scripts/update/logs/
```

## Technologies and Skills
- Python 3
- WooCommerce REST API: Main method of site interaction
- Data Processing: csv, os, logging, pandas (if used)
- Networking and Requests: requests, urllib
- Control: argparse
- Automation and CSV processing
