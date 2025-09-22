# Автоматизація оновлення товарних даних на сайті (MyWork project)

## Table of Contents
- [Українська версія](#автоматизація-оновлення-товарних-даних-на-сайті)
  - [Опис проєкту](#опис-проєкту)
  - [Основні завдання проєкту](#основні-завдання-проєкту)
  - [Структура проєкту](#структура-проєкту)
  - [Логіка роботи та основні блоки](#логіка-роботи-та-основні-блоки)
  - [Використання](#використання)
  - [Логування](#логування)
  - [Технології та навички](#технології-та-навички)
- [English Version](#mywork-project-english)
  - [Project Description](#project-description)
  - [Main Tasks of the Project](#main-tasks-of-the-project)
  - [Project Structure](#project-structure)
  - [Workflow and Main Steps](#workflow-and-main-steps)
  - [Usage](#usage)
  - [Logging](#logging)
  - [Technologies and Skills](#technologies-and-skills)

---

## Автоматизація оновлення товарних даних на сайті

### Опис проєкту
Цей проєкт — це система автоматизації, розроблена для підтримки актуальності даних про товари на сайті на базі **WooCommerce**.  
Система автоматично обробляє прайс-листи від різних постачальників, створює єдиний зведений файл та оновлює інформацію про залишки, ціни та акції безпосередньо через API сайту.

### Основні завдання проєкту:
- Вигрузка залишків товарів з сайту
- Завантаження та обробка прайс-листів від різних постачальників.
- Консолідація даних у єдину зведену таблицю.
- Підготовка окремих файлів для оновлення залишків/цін та акційних пропозицій.
- Оновлення даних на сайті через WooCommerce REST API.

---

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

---

## Логіка роботи та основні блоки
1. Вигрузка залишків товарів з сайту
2. Завантаження та обробка прайс-листів  
3. Створення зведеної таблиці  
4. Підготовка файлів для оновлення  
5. Оновлення даних на сайті  

---

## Використання
```bash
python3 run.py --export (вигрузка залишків товарів з сайту)
python3 run.py --download-supplier 1 (2, 3) (завантажження прайслиста постачальника 1, 2 або 3)
python3 run.py --process-supplier-1 (2, 3) (обробка прайслиста постачальника 1, 2 або 3)
python3 run.py --combine-tables (обєднання прайсів та залишків на сайті)
python3 run.py --prepare-upload (створення файлів "акції" та "залишки")
python3 run.py --update-products 1 (оновлення залишків і ціни)
python3 run.py --update-products 2 (оновлення акцій)
```

---

## Логування
Усі операції логуються у:
```
/var/www/scripts/update/logs/logs.log
```

---

## Технології та навички
- Python 3
- csv, os, logging, requests, argparse
- WooCommerce REST API
- Автоматизація та обробка CSV

---

# MyWork project (English)

### Project Description
This project is an automation system developed to keep product data on a **WooCommerce**-based website up to date.  

### Main Tasks of the Project:
- Еxport of product stock data from the website
- Download and process suppliers’ price lists.
- Consolidate data into a single unified table.
- Prepare separate files for stock/price updates and promotional offers.
- Update website data via WooCommerce REST API.

---

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

---

## Workflow and Main Steps
1. Export of product stock data from the website
2. Download and process price lists  
3. Create consolidated table  
4. Prepare files for update  
5. Update website data  

---

## Usage
```bash
python3 run.py --export (export of product stock data from the website)
python3 run.py --download-supplier 1 (2, 3) (download supplier price list 1, 2 or 3)
python3 run.py --process-supplier-1 (2, 3) (process supplier price list 1, 2 or 3)
python3 run.py --combine-tables (merge price lists and stock data from the website)
python3 run.py --prepare-upload (create "sales" and "stock" files)
python3 run.py --update-products 1 (update stock and prices)
python3 run.py --update-products 2 (update sales)
```

---

## Logging
All operations are logged in:
```
/var/www/scripts/update/logs/logs.log
```

---

## Technologies and Skills
- Python 3
- csv, os, logging, requests, argparse
- WooCommerce REST API
- Automation and CSV processing
