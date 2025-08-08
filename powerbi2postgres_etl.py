"""
Power BI to PostgreSQL ETL DAG
-----------------------------
Main ETL DAG that can be configured for different datasets and queries.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from loguru import logger

from oneC_etl.tasks.extract import extract_powerbi_data
from oneC_etl.tasks.load import execute_etl_task
from oneC_etl.config.variables import get_dataset_config
from oneC_etl.config.dax_mappings import get_dax_mapping

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

def process_datasets_task(**context):
    """Process Power BI datasets and prepare ETL tasks"""
    try:
        logger.info("=== Starting process_datasets_task ===")
        
        # Get dataset and mapping configuration from DAG run configuration
        dag_run_conf = context['dag_run'].conf or {}
        dataset_name = dag_run_conf.get('dataset_name')
        mapping_name = dag_run_conf.get('mapping_name')
        
        if not dataset_name or not mapping_name:
            raise ValueError("Both dataset_name and mapping_name must be provided in DAG run configuration")
        
        # Get dataset configuration
        dataset_config = get_dataset_config(dataset_name)
        
        # Get DAX mapping configuration
        dax_mapping = get_dax_mapping(mapping_name)
        
        # Create task configuration
        tasks = [{
            "dataset_id": dataset_config["id"],
            "source_table": dataset_config["source_table"],
            "target_table": dataset_config["target_table"],
            "dax_query": dax_mapping["query"],
            "columns": dax_mapping["columns"],
            "table_template": dax_mapping.get("table_template")
        }]
        
        logger.info(f"Created ETL task for dataset '{dataset_name}' using mapping '{mapping_name}'")
        
        # Push to XCom
        context['ti'].xcom_push(key='etl_tasks', value=tasks)
        return tasks
        
    except Exception as e:
        logger.exception("Error in process_datasets_task")
        raise

def execute_etl_task_wrapper(**context):
    """Wrapper function to handle data extraction and loading"""
    try:
        # Get tasks from XCom
        tasks = context['ti'].xcom_pull(task_ids='etl_tasks.process_datasets', key='etl_tasks')
        
        if not tasks:
            raise ValueError("No ETL tasks found in XCom")
            
        operations = []
        
        for task in tasks:
            try:
                # Extract data
                data = extract_powerbi_data(task)
                
                # Load data
                result = execute_etl_task(data, task)
                operations.append(result)
                
            except Exception as e:
                logger.exception(f"Error processing task {task['source_table']}")
                operations.append({
                    'source': task['source_table'],
                    'target': task['target_table'],
                    'error': str(e),
                    'status': 'failed'
                })
        
        # Push operations to XCom
        context['ti'].xcom_push(key='etl_operations', value=operations)
        return operations
        
    except Exception as e:
        logger.exception("Error in execute_etl_task_wrapper")
        raise

def check_results(**context):
    """Generate summary report of ETL operations"""
    try:
        operations = context['ti'].xcom_pull(
            task_ids='etl_tasks.execute_etl',
            key='etl_operations'
        )
        
        if not operations:
            logger.warning("No operations found in XCom")
            return
        
        # Calculate statistics
        total_ops = len(operations)
        successful_ops = len([op for op in operations if op['status'] == 'success'])
        failed_ops = len([op for op in operations if op['status'] == 'failed'])
        total_records = sum(op.get('records', 0) for op in operations if op['status'] == 'success')
        total_updated = sum(op.get('updated', 0) for op in operations if op['status'] == 'success')
        
        # Generate report
        report = f"""
ETL Operations Summary:
----------------------
Total Operations: {total_ops}
Successful: {successful_ops}
Failed: {failed_ops}
Total Records Processed: {total_records}
Total Records Updated: {total_updated}

Detailed Results:
----------------"""
        
        for op in operations:
            if op['status'] == 'success':
                report += f"\n✓ {op['source']} → {op['target']}: {op.get('records', 0)} records processed, {op.get('updated', 0)} updated"
            else:
                report += f"\n✗ {op['source']} → {op['target']}: FAILED - {op.get('error', 'Unknown error')}"
        
        logger.info(report)
        context['ti'].xcom_push(key='etl_report', value=report)
        
    except Exception as e:
        logger.exception("Error in check_results")
        raise

# Create the DAG
with DAG(
    'Powerbi2PostgresETL_main_template',  # More descriptive name
    default_args=default_args,
    description='ETL from Power BI to PostgreSQL - Main template for different datasets and queries',
    schedule_interval=None,  # Manual triggers only
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    with TaskGroup('etl_tasks') as etl_group:
        process_datasets = PythonOperator(
            task_id='process_datasets',
            python_callable=process_datasets_task,
            provide_context=True
        )
        
        execute_etl = PythonOperator(
            task_id='execute_etl',
            python_callable=execute_etl_task_wrapper,
            provide_context=True
        )
        
        # Set task dependencies within the group
        process_datasets >> execute_etl
    
    check_results = PythonOperator(
        task_id='check_results',
        python_callable=check_results,
        provide_context=True
    )
    
    # Set task group dependency
    etl_group >> check_results 