Структура проекта и описание компонентов:
Конфигурационные файлы (/config/):
dax_queries.json - DAX запросы для PowerBI
dax_mappings.py - Маппинг колонок между PowerBI и PostgreSQL
datasets.json - Конфигурация наборов данных
variables.py - Утилиты для работы с Airflow Variables
Основные DAG файлы:
company_products_etl.py - DAG для ETL процесса продуктов
powerbi2postgres_etl.py - Базовый DAG для ETL процессов
Задачи ETL (/tasks/):
extract.py - Извлечение данных из PowerBI
transform.py - Трансформация данных
load.py - Загрузка данных в PostgreSQL
Сервисы (/services/):
powerbi/client.py - Клиент для работы с PowerBI API
postgres/client.py - Клиент для работы с PostgreSQL
Процесс работы:
Инициализация:
Загрузка конфигурации из Airflow Variables
Создание DAG'а через фабрику
Инициализация клиентов PowerBI и PostgreSQL
Extract (Извлечение):
Получение DAX запроса из конфигурации
Выполнение запроса через PowerBI API
Получение результатов в формате DataFrame
Transform (Трансформация):
Применение маппингов колонок
Фильтрация данных (например, удаление NULL значений)
Преобразование типов данных
Load (Загрузка):
Создание временной таблицы
Загрузка данных во временную таблицу
MERGE операция для обновления основной таблицы
Очистка временных таблиц
Ключевые особенности:
Безопасность:
Использование Airflow Variables для хранения конфигурации
Безопасное хранение учетных данных
Масштабируемость:
Модульная структура
Возможность добавления новых наборов данных
Переиспользование кода через фабрику DAG'ов
Надежность:
Обработка ошибок на каждом этапе
Логирование всех операций
Транзакционность при загрузке данных
Мониторинг:
Детальное логирование
Отслеживание статуса операций
Сбор статистики по выполнению


Пример потока данных:
sequenceDiagram
    participant A as Airflow
    participant P as PowerBI
    participant T as Transform
    participant DB as PostgreSQL
    
    A->>P: Execute DAX Query
    P-->>A: Return Data
    A->>T: Transform Data
    T->>DB: Create Temp Table
    T->>DB: Load Data
    T->>DB: Merge Data
    T->>DB: Cleanup
    DB-->>A: Success