import os
import json
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
import sys
import os

# Add the root directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/.."))

from scripts.ingestdata import fetch_erp_api  # Now it should work!



#from scripts.ingestdata import fetch_erp_api








default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 15),
}

# Define the DAG
with DAG(
    'user_data_to_duckdb',
    default_args=default_args,
    description='Fetch user data from API and store it in DuckDB',
    schedule_interval='@daily', 
    catchup=False,
) as dag:


    fetch_erp_task = PythonOperator(
        task_id='fetch_erp_task',
        python_callable=fetch_erp_api,
       
    )
    # save_sales_task = PythonOperator(
    #     task_id='savesales_data_to_duckdb',
    #     python_callable=savesales_data_to_duckdb,
    #     op_args=["{{ task_instance.xcom_pull(task_ids='fetch_erp_task') }}"],  # Pull the file path from the first task
    # )


    # run_dbt_task = BashOperator(
    #     task_id='run_dbt_transform',
    #     bash_command="""
    #     cd /opt/airflow/dags/dbt_models && \
    #     dbt run --profiles-dir /opt/airflow/dbt/profiles.yml --target dev
    #     """,
    # )

    fetch_erp_task 
    

