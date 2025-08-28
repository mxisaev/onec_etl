# Suppliers ETL Project

## Описание
ETL проект для загрузки данных о партнерах (поставщиках и клиентах) из Power BI в PostgreSQL.

## Архитектура
- **Источник данных**: Power BI (dataset: получается из Airflow Variable `datasets.partners.id`)
- **Назначение**: PostgreSQL база данных `data`
- **Оркестрация**: Apache Airflow

## Таблицы в БД
- **`partners`** - основная таблица с партнерами и контактами
- **`supplier_interactions`** - история взаимодействий с партнерами
- **`supplier_tasks`** - задачи по работе с партнерами
- **`supplier_messages`** - сообщения и переписка
- **`monthly_cycles`** - месячные циклы работы

## Особенности
- **Композитный ключ**: `id_1c_partner` + `id_1c_contact` для уникальности
- **UUID первичный ключ**: `partner_uid` автоматически генерируется PostgreSQL
- **Интеграция с 1C**: использует `id_1c_partner` и `id_1c_contact` для связи
- **Логирование**: использует общий модуль `utils.logger` с сохранением в `logs/etl.log`

## Расписание
- **Время**: 11:00 и 15:00 (UTC+5)
- **Cron**: `0 6,10 * * 1-5` (UTC)
- **Дни**: Понедельник-Пятница

## Структура проекта
```
suppliers_etl/
├── suppliers_etl.py          # Основной DAG
├── tasks/
│   ├── extract.py            # Извлечение из Power BI
│   └── load.py               # Загрузка в PostgreSQL
├── config/
│   ├── dax_mappings.py       # Маппинг DAX запросов
├── dax_prepare/              # Подготовка DAX запросов
│   ├── raw_queries.py        # Сырые DAX запросы
│   ├── prepare_dax.py        # Обработка запросов
│   └── update_airflow.sh     # Обновление переменных Airflow
├── tests/                    # Тесты и диагностика
│   ├── test_dax_diagnostic.py # Диагностика DAX
│   └── test_suppliers_dax.py  # Тест основного DAX
├── logs/                     # Логи проекта
│   └── suppliers_etl.log     # Основной лог-файл
└── docs/                     # Документация
```

## Логирование
- **Модуль**: `utils.logger` (общий для всех проектов)
- **Формат**: структурированные логи с эмодзи и цветами
- **Файл**: `logs/suppliers_etl.log` в папке проекта
- **Уровень**: DEBUG для файла, INFO для консоли

## Запуск
```bash
cd /var/www/vhosts/itland.uk/docker
docker exec docker-airflow-webserver-1 airflow dags trigger SuppliersETL
```

## Мониторинг
- **Airflow UI**: http://localhost:8080
- **DAG ID**: `SuppliersETL`
- **Логи**: доступны в Airflow UI и в файле `logs/suppliers_etl.log`

## Зависимости
- Apache Airflow
- PostgreSQL
- Power BI API
- Python 3.9+

