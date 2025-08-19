# Настройка переменных Airflow для Suppliers ETL

## 🔑 Переменные Power BI

Для работы с Power BI необходимо настроить следующие переменные в Airflow:

### 1. Power BI Authentication
```json
{
  "powerbi_client_id": "your_client_id_here",
  "powerbi_client_secret": "your_client_secret_here", 
  "powerbi_tenant_id": "your_tenant_id_here",
  "powerbi_workspace_id": "990a4fcf-b910-4d98-a679-69e23387daec"
}
```

### 2. Dataset Configuration (в коде DAG)
```json
{
  "dax_queries": {
    "suppliers": {
      "query": "EVALUATE SUMMARIZECOLUMNS('Suppliers'[ID], 'Suppliers'[Name], 'Suppliers'[Code], 'Suppliers'[INN], 'Suppliers'[MainManager], 'Suppliers'[Status], 'Suppliers'[CreatedAt], 'Suppliers'[UpdatedAt])",
      "description": "Suppliers ETL query"
    }
  }
}
```

### 3. PostgreSQL Connection
```json
{
  "postgres_connection": {
    "host": "postgres_production",
    "port": 5432,
    "database": "data",
    "user": "postgresadmin",
    "password": "J5-unaxda3SK"
  }
}
```

## 📋 Инструкция по настройке

### Шаг 1: Настройка общих переменных в Airflow
1. **Откройте Airflow UI** (http://your-domain:8080)
2. **Перейдите в Admin → Variables**
3. **Добавьте каждую переменную** с соответствующим значением
4. **Сохраните изменения**

### Шаг 2: Настройка специфичных переменных в коде DAG
- `dax_queries` - настраивается в `config/dax_mappings.py`
- `datasets` - настраивается в `config/datasets.py`

## 🔍 Где взять значения

### Power BI
- **Client ID & Secret**: Azure App Registration
- **Tenant ID**: Azure Active Directory → Properties
- **Workspace ID**: Из URL Power BI (уже есть: `990a4fcf-b910-4d98-a679-69e23387daec`)

### PostgreSQL  
- **Host**: `postgres_production` (имя контейнера)
- **Port**: `5432`
- **Database**: `data`
- **User**: `postgresadmin`
- **Password**: `J5-unaxda3SK`

## ⚠️ Важно

- Все переменные должны быть **зашифрованы** в Airflow
- **Client Secret** особенно чувствителен
- Проверьте права доступа приложения к Power BI workspace

## ✅ Что уже настроено в коде

- **DAX запросы** - в `config/dax_queries.py`
- **Маппинги колонок** - в `config/dax_mappings.py`
- **Конфигурация датасетов** - в `config/datasets.py`
- **Dataset ID** - `afb5ea40-5805-4b0b-a082-81ca7333be85`
- **Workspace ID** - `990a4fcf-b910-4d98-a679-69e23387daec`
