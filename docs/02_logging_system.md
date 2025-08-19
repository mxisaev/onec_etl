# 📝 Логирование в Suppliers ETL

## 🌟 Особенности логирования

### **Цветные логи с эмодзи:**
- ✅ **SUCCESS** - зеленый цвет
- ❌ **ERROR** - красный цвет  
- ⚠️ **WARNING** - желтый цвет
- ℹ️ **INFO** - синий цвет
- 🔍 **DEBUG** - серый цвет

### **Структурированные сообщения:**
```
2025-08-13 17:56:20.686 | INFO | suppliers_etl:process_datasets_task:198 | === Starting process_datasets_task ===
```

## 📁 Расположение логов

### **1. Airflow логи:**
- **Путь:** `/opt/airflow/logs/suppliers_etl.log`
- **Ротация:** каждый день
- **Хранение:** 7 дней

### **2. Локальные логи проекта:**
- **Путь:** `docker/dags/suppliers_etl/logs/etl.log`
- **Ротация:** каждый день
- **Хранение:** 7 дней

### **3. Консольные логи:**
- **Вывод:** в терминале при запуске
- **Цвета:** включены
- **Уровень:** INFO+

## 🚀 Специальные функции логирования

### **ETL операции:**
```python
from utils.logger import log_etl_operation

log_etl_operation(
    operation_type="extract_load",
    source="Partners",
    target="partners", 
    records_count=7
)
```

### **Power BI операции:**
```python
from utils.logger import log_powerbi_operation

log_powerbi_operation(
    operation="query_execute",
    dataset_id="afb5ea40-5805-4b0b-a082-81ca7333be85",
    status="success",
    details="Extracted 7 rows"
)
```

### **PostgreSQL операции:**
```python
from utils.logger import log_postgres_operation

log_postgres_operation(
    operation="merge",
    table_name="partners",
    status="success",
    records_affected=7
)
```

## 🔧 Настройка уровней логирования

### **В коде:**
```python
import logging
from utils.logger import setup_logger

logger = setup_logger()
logger.setLevel(logging.DEBUG)  # Все логи
logger.setLevel(logging.INFO)   # INFO+
logger.setLevel(logging.WARNING) # WARNING+
```

### **В Airflow:**
- **UI:** Admin → Configuration → Logging
- **Переменная:** `logging_level = INFO`

## 📊 Примеры логов

### **Успешная загрузка:**
```
✅ ETL EXTRACT_LOAD SUCCESS | Partners → partners | Records: 7
```

### **Ошибка:**
```
❌ ETL EXTRACT_LOAD FAILED | Partners → partners | Error: column "id" does not exist
```

### **Power BI подключение:**
```
ℹ️ Power BI connect | Dataset: afb5ea40-5805-4b0b-a082-81ca7333be85 | Status: success | Successfully connected
```

## 🎯 Мониторинг в Airflow

### **Task Logs:**
- **Путь:** Task Instance → Log
- **Формат:** структурированный с цветами
- **Поиск:** по ключевым словам

### **DAG Logs:**
- **Путь:** DAG → Graph → Task Instance → Log
- **История:** все запуски
- **Анализ:** ошибок и производительности
