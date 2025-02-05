import os
import json
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG
import requests
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import main as main
from tasks.save_sales_order import fetch_erp_sales_api,savesales_data_to_duckdb
from tasks.save_payment import fetch_erp_payment_api,savepayment_data_to_duckdb
from tasks.save_customers import fetch_erp_customer_api,savecustomer_data_to_duckdb


import duckdb 
import json
 
 
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


    fetch_erp_sales = PythonOperator(
        task_id='fetch_erp_sales_api',
        python_callable=fetch_erp_sales_api,
       
    )

    fetch_erp_payment = PythonOperator(
        task_id='fetch_erp_payment_api',
        python_callable=fetch_erp_payment_api,
       
    )

    fetch_erp_customer = PythonOperator(
        task_id='fetch_erp_customer_api',
        python_callable=fetch_erp_customer_api,
       
    )

    save_sales_task = PythonOperator(
        task_id='savesales_data_to_duckdb',
        python_callable=savesales_data_to_duckdb,
        op_args=["{{ task_instance.xcom_pull(task_ids='fetch_erp_sales_api') }}"],  # Pull the file path from the first task
    )

    save_payment_task = PythonOperator(
        task_id='savepayment_data_to_duckdb',
        python_callable=savepayment_data_to_duckdb,
        op_args=["{{ task_instance.xcom_pull(task_ids='fetch_erp_payment_api') }}"],  # Pull the file path from the first task
    )

    save_customer_task = PythonOperator(
        task_id='savecustomer_data_to_duckdb',
        python_callable=savecustomer_data_to_duckdb,
        op_args=["{{ task_instance.xcom_pull(task_ids='fetch_erp_customer_api') }}"],  # Pull the file path from the first task
    )


    # run_dbt_task = BashOperator(  
    #     task_id='run_dbt_transform',
    #     bash_command="""
    #     cd /opt/airflow/dags/dbt_models && \
    #     dbt run --profiles-dir /opt/airflow/dbt/profiles.yml --target dev
    #     """,
    # )

    # fetch_erp_task >>save_sales_task>> run_dbt_task
    fetch_erp_sales >> save_sales_task
    fetch_erp_payment >> save_payment_task
    fetch_erp_customer >> save_customer_task 



