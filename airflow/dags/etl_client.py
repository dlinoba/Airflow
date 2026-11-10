from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

from services.cliente import (trunc_stage_table,extract_oltp_clients,transform,load,start_execution,finish_execution,fail_execution)


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 9, 1)
}

with DAG(
    dag_id='etl_client',
    schedule='@daily',
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1
) as dag:

    task_start_execution = PythonOperator(
        task_id='start_execution',
        python_callable=start_execution
    )

    task_trunc_stage_table = PythonOperator(
        task_id='trunc_table_st_cliente',
        python_callable=trunc_stage_table
    )

    task_extract_oltp_clients = PythonOperator(
        task_id='get_clients',
        python_callable=extract_oltp_clients
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load_data_to_mysql',
        python_callable=load
    )

    task_finish_execution = PythonOperator(
        task_id='finish_execution',
        python_callable=finish_execution,
        on_failure_callback=fail_execution
    )

    (
        task_start_execution
        >> task_trunc_stage_table
        >> task_extract_oltp_clients
        >> transform_task
        >> load_task
        >> task_finish_execution
    )
