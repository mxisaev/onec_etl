# Database Management

Эта папка содержит все компоненты для управления базой данных проекта.

## Структура папок

```
database/
├── migrations/     # SQL миграции для изменения структуры БД
├── schemas/        # Схемы таблиц и индексы
├── scripts/        # Скрипты для автоматизации работы с БД
├── backups/        # Резервные копии и дампы
└── README.md       # Этот файл
```

## Описание компонентов

### 📁 migrations/
SQL файлы для изменения структуры базы данных:
- `add_product_properties_column.sql` - добавление колонки product_properties
- `cleanup_companyproducts_structure.sql` - очистка структуры таблицы companyproducts
- `alter_*.sql` - изменения существующих таблиц
- `create_*.sql` - создание новых таблиц

### 📁 schemas/
Схемы и определения таблиц:
- Структуры таблиц
- Индексы
- Ограничения
- Триггеры

### 📁 scripts/
Python скрипты для автоматизации:
- `fix_column_mapping.py` - исправление проблем с маппированием колонок
- `cleanup_table_structure.py` - очистка структуры таблицы от лишних колонок
- `generate_table.py` - генерация SQL для создания таблиц
- `generate_dag.py` - генерация DAG файлов

### 📁 backups/
Резервные копии:
- Дампы базы данных
- Экспорт данных
- Резервные копии конфигураций

## Использование

### Очистка структуры таблицы companyproducts

```bash
cd /var/www/vhosts/itland.uk/docker/dags/oneC_etl/database/scripts
python3 cleanup_table_structure.py
```

Этот скрипт:
1. ✅ Показывает план очистки с подтверждением
2. ✅ Создает резервную копию таблицы
3. ✅ Удаляет лишние колонки
4. ✅ Обновляет маппирование в DAG и конфигурациях
5. ✅ Проверяет структуру таблицы
6. ✅ Предлагает восстановление при ошибках

### Правильная структура таблицы companyproducts

```sql
-- Нужные колонки (14 штук):
id                       uuid                     # Первичный ключ
description              text                     # Описание товара
brand                    character varying        # Бренд
category                 character varying        # Категория
withdrawn_from_range     boolean                  # Снят с продажи
item_number              character varying        # Номер товара
product_category         character varying        # Категория товара
on_order                 boolean                  # Под заказ
is_vector                boolean                  # Флаг вектора
count_rows               integer                  # Количество строк
merged                   text                     # Колонка для эмбеддинга ✅
upload_timestamp         timestamp with time zone # Время загрузки
updated_at               timestamp with time zone # Время обновления
vector                   USER-DEFINED             # Вектор для поиска
```

### Лишние колонки для удаления (11 штук):
- `companyproducts` - неправильное имя
- `isgrandtotalrowtotal` - служебная колонка Power BI
- `companyproducts_1` до `companyproducts_5` - дублирование
- `[выводится_без_остатков]` - неправильное имя
- `companyproducts[merged]` - неправильное имя
- `[product properties]` - дублирует merged
- `product_properties` - дублирует merged ❌

### Исправление проблем с маппированием колонок

```bash
cd /var/www/vhosts/itland.uk/docker/dags/oneC_etl/database/scripts
python3 fix_column_mapping.py
```

Этот скрипт:
1. ✅ Обновляет маппирование в DAG файле
2. ✅ Обновляет маппирование в dax_mappings.py
3. ✅ Создает SQL миграцию
4. ✅ Выполняет миграцию в PostgreSQL
5. ✅ Проверяет структуру таблицы

### Генерация таблиц

```bash
cd /var/www/vhosts/itland.uk/docker/dags/oneC_etl/database/scripts
python3 generate_table.py
```

### Выполнение миграций

```bash
# Подключение к PostgreSQL
cd /var/www/vhosts/itland.uk/docker/postgres
docker compose exec postgres psql -U postgresadmin -d data

# Выполнение миграции
\i /var/www/vhosts/itland.uk/docker/dags/oneC_etl/database/migrations/cleanup_companyproducts_structure.sql
```

## Полезные команды

### Проверка структуры таблицы
```sql
\d companyproducts
```

### Просмотр всех таблиц
```sql
\dt
```

### Проверка колонок таблицы
```sql
SELECT column_name, data_type, is_nullable
FROM information_schema.columns 
WHERE table_schema = 'public' 
AND table_name = 'companyproducts'
ORDER BY ordinal_position;
```

### Создание резервной копии
```sql
CREATE TABLE companyproducts_backup_$(date +%Y%m%d_%H%M%S) AS SELECT * FROM companyproducts;
```

### Восстановление из резервной копии
```sql
DROP TABLE companyproducts CASCADE;
CREATE TABLE companyproducts AS SELECT * FROM companyproducts_backup_YYYYMMDD_HHMMSS;
```

## Маппирование колонок

### Правильное маппирование в DAG:
```python
"columns": {
    "CompanyProducts[ID]": "id",
    "CompanyProducts[Description]": "description",
    "CompanyProducts[Brand]": "brand",
    "CompanyProducts[Category]": "category",
    "CompanyProducts[Withdrawn_from_range]": "withdrawn_from_range",
    "CompanyProducts[item_number]": "item_number",
    "УТ_Товарные категории[_description]": "product_category",
    "УТ_РСвДополнительныеСведения2_0[Под заказ]": "on_order",
    "Выводится_без_остатков": "is_vector",
    "CountRowsУТ_РСвДополнительныеСведения2_0": "count_rows",
    "Product Properties": "merged"  # ✅ Маппим в merged для эмбеддингов
}
```

## Примечания

- Все скрипты используют переменные окружения из `.env` файла
- Миграции выполняются в транзакциях для безопасности
- Перед выполнением миграций создаются резервные копии
- Все изменения логируются для отслеживания
- Интерактивный режим с подтверждением действий
- Возможность отката изменений из резервной копии 