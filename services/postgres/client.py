"""
PostgreSQL client for data loading
"""

import pandas as pd
import re
from loguru import logger
from sqlalchemy import create_engine, text
from airflow.models import Variable
from typing import List, Dict

def normalize_column_name(col):
    # Приводим к нижнему регистру, заменяем пробелы и дефисы на подчёркивания, убираем спецсимволы
    normalized = col.lower().replace(' ', '_').replace('-', '_')
    normalized = ''.join(c for c in normalized if c.isalnum() or c == '_')
    return normalized

class PostgresClient:
    """PostgreSQL client for data loading"""
    
    def __init__(self):
        """Initialize PostgreSQL client with connection from Airflow Variables"""
        try:
            # Get connection details from Airflow Variables
            conn = Variable.get('postgres_connection', deserialize_json=True)
            
            if not conn:
                raise ValueError("PostgreSQL connection not found in Airflow Variables")
            
            # Create SQLAlchemy engine
            self.engine = create_engine(
                f"postgresql://{conn['user']}:{conn['password']}@{conn['host']}:{conn['port']}/{conn['database']}"
            )
            
            # Define PostgreSQL type aliases
            self.PGTYPE_ALIAS = {
                "UUID": "UUID",
                "TEXT": "TEXT",
                "STRING": "TEXT",
                "BOOLEAN": "BOOLEAN",
                "BOOL": "BOOLEAN",
                "INTEGER": "INTEGER",
                "INT": "INTEGER",
                "NUMERIC": "NUMERIC",
                "TIMESTAMP": "TIMESTAMP"
            }
            

            
        except Exception as e:
            logger.exception(f"Error initializing PostgreSQL client: {str(e)}")
            raise
    
    def _quote_identifier(self, identifier):
        """
        Quote identifier for PostgreSQL
        
        Args:
            identifier (str): Identifier to quote
            
        Returns:
            str: Quoted identifier
        """
        # Remove any existing quotes
        identifier = identifier.strip('"')
        # Quote if contains special characters or is not a valid identifier
        if not identifier.isidentifier() or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in identifier):
            return f'"{identifier}"'
        return identifier

    def ensure_table_schema(self, table_name: str, columns: List[Dict[str, str]]) -> None:
        """Ensure table exists with correct schema"""
        try:
            # First, ensure pgvector extension exists
            self.engine.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            
            # Check if table exists
            result = self.engine.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                )
            """)
            table_exists = result.scalar()
            
            if not table_exists:
                # Create new table with correct schema
                column_definitions = []
                for col in columns:
                    col_name = self._quote_identifier(normalize_column_name(col['name']))
                    col_type = col['dataType'].upper()
                    if col_name == '"id"':
                        column_definitions.append(f"{col_name} {col_type} PRIMARY KEY")
                    else:
                        column_definitions.append(f"{col_name} {col_type}")

                create_table_sql = f"""
                CREATE TABLE {table_name} (
                    {', '.join(column_definitions)},
                    is_vector BOOLEAN DEFAULT FALSE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                self.engine.execute(create_table_sql)
                logger.info(f"Created new table {table_name}")
            else:
                # Add any missing columns
                existing_columns = set()
                result = self.engine.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                """)
                for row in result:
                    existing_columns.add(row[0].lower())
                
                # Add missing columns
                missing_columns = []
                for col in columns:
                    col_name = normalize_column_name(col['name'])
                    if col_name not in existing_columns:
                        quoted_name = self._quote_identifier(col_name)
                        pg_type = col['dataType'].upper()
                        missing_columns.append(f"ADD COLUMN {quoted_name} {pg_type}")
                
                if missing_columns:
                    alter_table_sql = f"""
                    ALTER TABLE {table_name}
                    {', '.join(missing_columns)}
                    """
                    self.engine.execute(alter_table_sql)
                    logger.info(f"Successfully added {len(missing_columns)} columns to {table_name}")
            
        except Exception as e:
            logger.exception(f"Error ensuring table schema: {str(e)}")
            raise
    
    def _get_column_type(self, col_name: str, col_type: str) -> str:
        """
        Get PostgreSQL type for column
        
        Args:
            col_name (str): Column name
            col_type (str): Column type from mapping
            
        Returns:
            str: PostgreSQL type
        """
        # Special handling for boolean columns
        if col_name in ['is_vector', 'on_order', 'withdrawn_from_range', 'is_client', 'is_supplier']:
            return 'BOOLEAN'
        
        # Map type from mapping to PostgreSQL type
        return self.PGTYPE_ALIAS.get(col_type.upper(), 'TEXT')

    def _get_column_cast(self, col_name: str, col_type: str) -> str:
        """
        Get PostgreSQL type cast for column
        
        Args:
            col_name (str): Column name
            col_type (str): Column type from mapping
            
        Returns:
            str: PostgreSQL type cast
        """
        pg_type = self._get_column_type(col_name, col_type)
        if pg_type == 'BOOLEAN':
            return f"(source.{col_name}::text)::boolean"
        elif pg_type == 'INTEGER':
            return f"(source.{col_name}::text)::integer"
        elif pg_type == 'NUMERIC':
            return f"(source.{col_name}::text)::numeric"
        elif pg_type == 'TIMESTAMP':
            return f"(source.{col_name}::text)::timestamp"
        else:
            return f"source.{col_name}"

    def merge_data(self, table_name, data, key_columns, columns=None, template_name=None, columns_for_change_analysis=None):
        """
        Merge (upsert) data into PostgreSQL table
        
        Args:
            table_name (str): Target table name
            data (pd.DataFrame): Data to merge
            key_columns (list): List of key columns for merge
            columns (list): List of column definitions for schema with format:
                [{'name': str, 'dataType': str}, ...]
            template_name (str): Name of the table template to use
            columns_for_change_analysis (list): Список бизнес-колонок для анализа изменений (сброс is_vector)
            
        Returns:
            dict: Merge statistics
        """
        try:
            # Clean all column names first
            cleaned_columns = {}
            seen_cleaned_names = set()
            for col in data.columns:
                cleaned_name = normalize_column_name(col)
                if cleaned_name:
                    # Handle duplicate cleaned names by adding a suffix
                    base_name = cleaned_name
                    counter = 1
                    while cleaned_name in seen_cleaned_names:
                        cleaned_name = f"{base_name}_{counter}"
                        counter += 1
                    seen_cleaned_names.add(cleaned_name)
                    cleaned_columns[col] = cleaned_name
                else:
                    # If cleaning fails, use a safe fallback name
                    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', col.lower())
                    safe_name = re.sub(r'_+', '_', safe_name)
                    safe_name = safe_name.strip('_')
                    # Handle duplicate safe names
                    base_name = safe_name
                    counter = 1
                    while safe_name in seen_cleaned_names:
                        safe_name = f"{base_name}_{counter}"
                        counter += 1
                    seen_cleaned_names.add(safe_name)
                    cleaned_columns[col] = safe_name
            
            # Create mapping of column names to their types
            column_types = {}
            if columns:
                for col in columns:
                    name = normalize_column_name(col['name'])
                    data_type = col['dataType'].upper()
                    column_types[name] = data_type
                self.ensure_table_schema(table_name, columns)
            
            # Create a mapping of cleaned column names to original names
            cleaned_to_original = {cleaned: orig for orig, cleaned in cleaned_columns.items()}
            
            # Find the full column names for the key columns
            full_key_columns = []
            for key in key_columns:
                # Try to find the column by its cleaned name
                cleaned_key = normalize_column_name(key)
                if cleaned_key in cleaned_to_original:
                    full_key_columns.append(cleaned_to_original[cleaned_key])
                else:
                    # Try case-insensitive partial match
                    matching_cols = [col for col in data.columns if key.lower() in col.lower()]
                    if matching_cols:
                        full_key_columns.append(matching_cols[0])
                    else:
                        raise ValueError(f"Could not find column matching key: {key}")
            
            # Update key columns with cleaned names
            key_columns = [cleaned_columns[col] for col in full_key_columns]
            
            # Prepare merge statement with special handling for is_vector
            key_conditions = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])
            
            # Handle different data types with explicit casting
            update_columns = []
            for col in data.columns:
                if col not in key_columns and col != 'is_vector':
                    cleaned_col = cleaned_columns[col]
                    if not cleaned_col:  # Пропускаем пустые имена
                        continue
                    col_type = column_types.get(cleaned_col, 'TEXT')
                    update_columns.append(f"{cleaned_col} = {self._get_column_cast(cleaned_col, col_type)}")
            
            update_set = ", ".join(update_columns)
            
            # Add special handling for is_vector flag and updated_at
            # Используем только бизнес-колонки для distinct_conditions, если columns_for_change_analysis передан
            distinct_conditions = []
            change_cols = columns_for_change_analysis if columns_for_change_analysis is not None else [col for col in data.columns if col not in key_columns and col != 'is_vector']
            for col in change_cols:
                if col not in data.columns:
                    continue
                cleaned_col = cleaned_columns[col]
                if not cleaned_col:
                    continue
                col_type = column_types.get(cleaned_col, 'TEXT')
                pg_type = self._get_column_type(cleaned_col, col_type)
                if pg_type == 'BOOLEAN':
                    distinct_conditions.append(f"target.{cleaned_col} IS DISTINCT FROM {self._get_column_cast(cleaned_col, col_type)}")
                else:
                    distinct_conditions.append(f"target.{cleaned_col} IS DISTINCT FROM source.{cleaned_col}")
            
            # Обновляем is_vector и updated_at только при изменении данных
            if distinct_conditions:
                update_set += f""",
                    is_vector = CASE 
                        WHEN {' OR '.join(distinct_conditions)} THEN FALSE
                        ELSE target.is_vector
                    END,
                    updated_at = CASE 
                        WHEN {' OR '.join(distinct_conditions)} THEN CURRENT_TIMESTAMP
                        ELSE target.updated_at
                    END"""
            else:
                update_set += """,
                    is_vector = target.is_vector,
                    updated_at = target.updated_at"""
            
            # For new records, set is_vector = FALSE by default
            insert_columns = []
            insert_values = []
            seen_insert_columns = set()  # Track columns we've already added
            
            # Detect partners-style merge by key columns (id_1c_* implies partners)
            is_partners_merge = (
                table_name.lower() == 'partners' or
                any(k.lower() in ['id_1c_partner', 'id_1c_contact'] for k in key_columns)
            )

            # Handle different primary key strategies
            if is_partners_merge:
                # Для partners используем partner_uid как PRIMARY KEY
                insert_columns.append('partner_uid')
                insert_values.append('gen_random_uuid()')
                seen_insert_columns.add('partner_uid')
            else:
                # Для других таблиц используем id
                insert_columns.append('id')
                insert_values.append('source.id')
                seen_insert_columns.add('id')
            
            # Then add all other columns
            for col in data.columns:
                if col != 'is_vector':
                    cleaned_col = cleaned_columns[col]
                    if not cleaned_col or cleaned_col in seen_insert_columns:  # Пропускаем пустые имена и дубликаты
                        continue
                    col_type = column_types.get(cleaned_col, 'TEXT')
                    insert_columns.append(cleaned_col)
                    insert_values.append(self._get_column_cast(cleaned_col, col_type))
                    seen_insert_columns.add(cleaned_col)
            
            # Convert DataFrame to list of dictionaries for parameter binding
            data_dicts = []
            for _, row in data.iterrows():
                row_dict = {}
                for col in data.columns:
                    cleaned_col = cleaned_columns[col]
                    if not cleaned_col:  # Пропускаем пустые имена
                        continue
                    val = row[col]
                    if pd.isna(val):
                        row_dict[cleaned_col] = None
                    elif isinstance(val, bool):
                        row_dict[cleaned_col] = str(val).lower()
                    else:
                        row_dict[cleaned_col] = str(val)
                data_dicts.append(row_dict)
            
            # Create a temporary table for the merge using cleaned column names
            temp_table = f"temp_{table_name}"
            
            # Create column definitions for temporary table
            temp_columns = []
            for col in data.columns:
                cleaned_col = cleaned_columns[col]
                if not cleaned_col:  # Пропускаем пустые имена
                    continue
                col_type = column_types.get(cleaned_col, 'TEXT')
                pg_type = self._get_column_type(cleaned_col, col_type)
                temp_columns.append(f"{cleaned_col} {pg_type}")
            
            create_temp_sql = f"""
            CREATE TEMPORARY TABLE {temp_table} (
                {', '.join(temp_columns)}
            ) ON COMMIT DROP;
            """
            
            # Insert data into temporary table using parameterized query
            valid_columns = [col for col in data.columns if cleaned_columns[col]]  # Фильтруем пустые имена
            insert_temp_sql = f"""
            INSERT INTO {temp_table} ({', '.join([cleaned_columns[col] for col in valid_columns])})
            VALUES ({', '.join([f':{cleaned_columns[col]}' for col in valid_columns])});
            """
            
            # Merge from temporary table
            if is_partners_merge:
                # Для partners используем partner_uid
                merge_sql = f"""
                MERGE INTO {table_name} AS target
                USING {temp_table} AS source
                ON {key_conditions}
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT (partner_uid, {','.join(insert_columns[1:])}, is_vector, updated_at)
                    VALUES (gen_random_uuid(), {','.join(insert_values[1:])}, FALSE, CURRENT_TIMESTAMP);
                """
            else:
                # Для других таблиц используем id
                merge_sql = f"""
                MERGE INTO {table_name} AS target
                USING {temp_table} AS source
                ON {key_conditions}
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT (id, {','.join(insert_columns[1:])}, is_vector, updated_at)
                    VALUES (source.id, {','.join(insert_values[1:])}, FALSE, CURRENT_TIMESTAMP);
                """
            
            # Safety patch: if for any reason partners branch didn't apply, rewrite INSERT to use partner_uid
            if table_name.lower() == 'partners' and 'INSERT (id,' in merge_sql:
                merge_sql = merge_sql.replace('INSERT (id,', 'INSERT (partner_uid,')
                merge_sql = merge_sql.replace('VALUES (source.id,', 'VALUES (gen_random_uuid(),')

            # Execute merge
            with self.engine.connect() as conn:
                with conn.begin():  # Start a transaction
                    # Create temporary table
                    conn.execute(text(create_temp_sql))
                    
                    # Insert data into temporary table using parameterized query
                    for row_dict in data_dicts:
                        conn.execute(text(insert_temp_sql), row_dict)
                    
                    # Execute merge
                    result = conn.execute(text(merge_sql))
            
            stats = {
                'updated_rows': result.rowcount
            }
            
            logger.info(f"Successfully merged {result.rowcount} rows into {table_name}")
            return stats
            
        except Exception as e:
            logger.exception(f"Error merging data into PostgreSQL: {str(e)}")
            raise
    
    def execute_query(self, query: str) -> List[Dict]:
        """
        Execute a SQL query and return results
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            List[Dict]: List of dictionaries with query results
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                
                if result.returns_rows:
                    # Convert result to list of dictionaries
                    columns = result.keys()
                    rows = [dict(zip(columns, row)) for row in result.fetchall()]
                    return rows
                else:
                    # For non-SELECT queries, return empty list
                    return []
                    
        except Exception as e:
            logger.exception(f"Error executing query: {str(e)}")
            raise 