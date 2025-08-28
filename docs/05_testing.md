# Tests

Тесты для диагностики и отладки Power BI интеграции.

## Файлы

### `test_dax_diagnostic.py`
**Диагностика DAX запросов**
- Тестирует различные варианты DAX запросов через Power BI API
- Проверяет актуальный DAX из Airflow Variable
- Помогает диагностировать проблемы с DAX синтаксисом

**Запуск:**
```bash
docker exec docker-airflow-webserver-1 python3 /opt/airflow/dags/suppliers_etl/tests/test_dax_diagnostic.py
```

### `list_reports.py`
**Список отчётов и датасетов**
- Получает список всех отчётов в workspace
- Показывает структуру датасетов и таблиц
- Полезен для настройки и диагностики

**Запуск:**
```bash
docker exec docker-airflow-webserver-1 python3 /opt/airflow/dags/suppliers_etl/tests/list_reports.py
```

### `get_reports.py`
**Детальный анализ отчётов**
- Показывает страницы и визуализации отчётов
- Более детальная информация о структуре отчётов
- Помогает понять архитектуру Power BI

**Запуск:**
```bash
docker exec docker-airflow-webserver-1 python3 /opt/airflow/dags/suppliers_etl/tests/get_reports.py
```

### `test_suppliers_dax.py`
**Тест основного DAX запроса**
- Тестирует DAX запрос для партнеров
- Проверяет подключение к Power BI API
- Полезен для диагностики основных проблем

**Запуск:**
```bash
docker exec docker-airflow-webserver-1 python3 /opt/airflow/dags/suppliers_etl/tests/test_suppliers_dax.py
```

### `test_simplified_dax.py`
**Тест упрощенного DAX запроса**
- Тестирует упрощенную версию DAX без JOIN'ов
- Проверяет работу с основной таблицей УТ_Партнеры
- Помогает диагностировать проблемы сложных запросов

**Запуск:**
```bash
docker exec docker-airflow-webserver-1 python3 /opt/airflow/dags/suppliers_etl/tests/test_simplified_dax.py
```

## Использование

Все тесты используют Airflow Variables для подключения к Power BI:
- `powerbi_tenant_id`
- `powerbi_client_id` 
- `powerbi_client_secret`
- `powerbi_workspace_id` 