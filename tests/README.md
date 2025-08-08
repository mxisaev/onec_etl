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
docker compose exec airflow-webserver python /opt/airflow/dags/oneC_etl/tests/test_dax_diagnostic.py
```

### `list_reports.py`
**Список отчётов и датасетов**
- Получает список всех отчётов в workspace
- Показывает структуру датасетов и таблиц
- Полезен для настройки и диагностики

**Запуск:**
```bash
docker compose exec airflow-webserver python /opt/airflow/dags/oneC_etl/tests/list_reports.py
```

### `get_reports.py`
**Детальный анализ отчётов**
- Показывает страницы и визуализации отчётов
- Более детальная информация о структуре отчётов
- Помогает понять архитектуру Power BI

**Запуск:**
```bash
docker compose exec airflow-webserver python /opt/airflow/dags/oneC_etl/tests/get_reports.py
```

## Использование

Все тесты используют Airflow Variables для подключения к Power BI:
- `powerbi_tenant_id`
- `powerbi_client_id` 
- `powerbi_client_secret`
- `powerbi_workspace_id` 