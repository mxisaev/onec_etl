# Система характеристик продуктов (Product Properties)

## Описание
Система для хранения характеристик продуктов в формате key-value в отдельной таблице `product_properties`. Это позволяет эффективно искать и фильтровать продукты по характеристикам.

## Структура базы данных

### Таблица product_properties
```sql
CREATE TABLE product_properties (
    id SERIAL PRIMARY KEY,
    product_id UUID REFERENCES companyproducts(id) ON DELETE CASCADE,
    prop_key VARCHAR(255) NOT NULL,
    prop_value TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Индексы
- `idx_product_properties_product_id` - для быстрого поиска по ID продукта
- `idx_product_properties_key` - для поиска по ключу характеристики
- `idx_product_properties_key_value` - для поиска по ключу и значению
- `idx_product_properties_unique` - уникальный индекс для предотвращения дублирования

## Миграция данных

### Запуск миграции
```bash
cd /var/www/vhosts/itland.uk/docker/dags/oneC_etl/database/scripts
python3 migrate_product_properties.py
```

Этот скрипт:
1. Создает таблицу `product_properties`
2. Парсит строку `product_properties` из таблицы `companyproducts`
3. Сохраняет каждую характеристику как отдельную запись
4. Показывает статистику миграции

### Пример парсинга
Исходная строка:
```
"Бренд: Haiba | Назначение: для кухни | Материал: Латунь | Цвет: Черный"
```

Преобразуется в записи:
```
product_id | prop_key    | prop_value
-----------|-------------|------------
uuid-1     | Бренд       | Haiba
uuid-1     | Назначение  | для кухни
uuid-1     | Материал    | Латунь
uuid-1     | Цвет        | Черный
```

## Использование

### Поиск продуктов по характеристикам
```python
from database.scripts.product_properties_utils import search_products_by_properties

# Поиск смесителей черного цвета
products = search_products_by_properties({
    'Цвет': 'Черный',
    'Товарная категория': 'Смесители для кухни'
}, limit=10)

for product in products:
    print(f"{product['description']} - {product['brand']}")
    print(f"Характеристики: {product['properties']}")
```

### Получение характеристик продукта
```python
from database.scripts.product_properties_utils import get_product_properties

properties = get_product_properties('product-uuid-here')
print(properties)
# {'Бренд': 'Haiba', 'Цвет': 'Черный', 'Материал': 'Латунь', ...}
```

### Обновление характеристик
```python
from database.scripts.product_properties_utils import update_product_properties

update_product_properties('product-uuid-here', {
    'Цвет': 'Белый',
    'Новая характеристика': 'Новое значение'
})
```

## SQL запросы

### Поиск продуктов с определенной характеристикой
```sql
SELECT c.*
FROM companyproducts c
JOIN product_properties pp ON pp.product_id = c.id
WHERE pp.prop_key = 'Цвет' AND pp.prop_value = 'Черный';
```

### Поиск с несколькими фильтрами
```sql
SELECT c.*
FROM companyproducts c
JOIN product_properties pp1 ON pp1.product_id = c.id
JOIN product_properties pp2 ON pp2.product_id = c.id
WHERE pp1.prop_key = 'Цвет' AND pp1.prop_value = 'Черный'
  AND pp2.prop_key = 'Бренд' AND pp2.prop_value = 'Haiba';
```

### Поиск с использованием HAVING (все характеристики должны совпадать)
```sql
SELECT c.*
FROM companyproducts c
JOIN product_properties pp ON pp.product_id = c.id
WHERE (pp.prop_key = 'Цвет' AND pp.prop_value = 'Черный')
   OR (pp.prop_key = 'Бренд' AND pp.prop_value = 'Haiba')
GROUP BY c.id
HAVING COUNT(DISTINCT pp.prop_key) = 2;
```

## Интеграция с ETL

### Автоматическая обработка в ETL
При загрузке данных из Power BI характеристики автоматически парсятся и сохраняются в таблицу `product_properties`:

```python
# В PostgresClient добавлен метод merge_data_with_properties
client.merge_data_with_properties(
    table_name='companyproducts',
    data=dataframe,
    key_columns=['id']
)
```

### Статистика обработки
ETL возвращает статистику:
```python
{
    'updated_rows': 150,
    'properties_processed': 1200  # Количество обработанных характеристик
}
```

## Утилиты

### Получение доступных характеристик
```python
from database.scripts.product_properties_utils import get_available_property_keys

keys = get_available_property_keys()
print(keys)  # ['Бренд', 'Цвет', 'Материал', 'Назначение', ...]
```

### Получение значений характеристики
```python
from database.scripts.product_properties_utils import get_property_values

colors = get_property_values('Цвет')
print(colors)  # ['Черный', 'Белый', 'Хром', 'Золотой', ...]
```

### Статистика
```python
from database.scripts.product_properties_utils import get_products_statistics

stats = get_products_statistics()
print(f"Всего характеристик: {stats['total_properties']}")
print(f"Продуктов с характеристиками: {stats['products_with_properties']}")
print(f"Уникальных ключей: {stats['unique_keys']}")
```

## Преимущества новой системы

1. **Эффективный поиск**: Индексы позволяют быстро находить продукты по характеристикам
2. **Гибкость**: Легко добавлять новые характеристики без изменения схемы
3. **Нормализация**: Избегаем дублирования данных
4. **Масштабируемость**: Система работает с большим количеством характеристик
5. **Аналитика**: Легко получать статистику по характеристикам

## Миграция существующих данных

Если у вас уже есть данные в строке `product_properties`, запустите миграцию:

```bash
python3 migrate_product_properties.py
```

После миграции можно удалить старую колонку `product_properties` из таблицы `companyproducts`:

```sql
ALTER TABLE companyproducts DROP COLUMN product_properties;
```

## Мониторинг

### Проверка целостности данных
```sql
-- Продукты без характеристик
SELECT COUNT(*) FROM companyproducts c
LEFT JOIN product_properties pp ON pp.product_id = c.id
WHERE pp.product_id IS NULL;

-- Дублирующиеся характеристики
SELECT product_id, prop_key, COUNT(*)
FROM product_properties
GROUP BY product_id, prop_key
HAVING COUNT(*) > 1;
``` 