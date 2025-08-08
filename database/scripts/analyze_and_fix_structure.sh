#!/bin/bash
# Запуск анализа и исправления структуры таблицы companyproducts

docker compose exec airflow-scheduler python /opt/airflow/dags/oneC_etl/database/scripts/analyze_and_fix_structure_v2.py 