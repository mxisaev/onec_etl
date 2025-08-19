# Suppliers ETL Project

## Описание
ETL проект для загрузки данных о партнерах (поставщиках и клиентах) из Power BI в PostgreSQL.

## Архитектура
- **Источник данных**: Power BI (dataset: `afb5ea40-5805-4b0b-a082-81ca7333be85`)
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
│   ├── variables.py          # Конфигурация Airflow переменных
│   └── dax_mappings.py       # Маппинг DAX запросов
├── logs/                     # Логи проекта
│   └── etl.log              # Основной лог-файл
└── README.md                 # Документация
```

## Логирование
- **Модуль**: `utils.logger` (общий для всех проектов)
- **Формат**: структурированные логи с эмодзи и цветами
- **Файл**: `logs/etl.log` в папке проекта
- **Уровень**: DEBUG для файла, INFO для консоли

## Запуск
```bash
cd /var/www/vhosts/itland.uk/docker/dags/suppliers_etl
./run_etl.sh SuppliersETL
```

## Мониторинг
- **Airflow UI**: http://localhost:8080
- **DAG ID**: `SuppliersETL`
- **Логи**: доступны в Airflow UI и в файле `logs/etl.log`

## Зависимости
- Apache Airflow
- PostgreSQL
- Power BI API
- Python 3.9+

