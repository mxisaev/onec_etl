import psycopg2
from airflow.models import Variable
import json

def get_postgres_connection():
    postgres_config = json.loads(Variable.get('postgres_connection'))
    conn = psycopg2.connect(
        host=postgres_config['host'],
        port=postgres_config['port'],
        database=postgres_config['database'],
        user=postgres_config['user'],
        password=postgres_config['password']
    )
    return conn

def list_columns(table_name='companyproducts'):
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position;
            """, (table_name,))
            columns = [row[0] for row in cursor.fetchall()]
            print(f"Колонки в таблице {table_name}:")
            for col in columns:
                print(f"- {col}")
    finally:
        conn.close()

if __name__ == '__main__':
    list_columns() 