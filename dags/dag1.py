import os, requests
import json
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
import sys
import os

# Add the root directory to sys.path

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/.."))
from scripts.ingestdata import fetch_salesorder , fetch_paymententry, fetch_customerdetails # Now it should work!
from scripts.storerowdata import load_salesorder_to_duckdb, load_payment_to_duckdb, load_customer_to_duckdb


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


    # fetch_sales_order = PythonOperator(
    #     task_id='fetch_sales_order',
    #     python_callable=fetch_salesorder,
       
    # )

    # fetch_payment_entry = PythonOperator(
    #     task_id='fetch_payment_entry',
    #     python_callable=fetch_paymententry,  
       
    # )

    # fetch_customer_details = PythonOperator(
    #     task_id='fetch_customer_details',
    #     python_callable=fetch_customerdetails, 
       
    # )

    store_salesorderinto_duckdb = PythonOperator(
        task_id='store_salesorderinto_duckdb',
        python_callable=load_salesorder_to_duckdb,  
       
    )

    store_paymententryinto_duckdb = PythonOperator(
        task_id='store_paymententryinto_duckdb',
        python_callable=load_payment_to_duckdb ,  
       
    )

    store_customerinto_duckdb = PythonOperator(
        task_id='store_customerinto_duckdb',
        python_callable=load_customer_to_duckdb ,  
       
    )

#     dbt_test = BashOperator(
#     task_id='test_dbt',
#     bash_command="""
#         cd /opt/airflow/etldbt && \
#         echo "1" | dbt init my_project --profiles-dir /opt/airflow/etldbt
#         """, # Change the path if necessary
    
# )

    


    # save_sales_task = PythonOperator(
    #     task_id='savesales_data_to_duckdb',
    #     python_callable=savesales_data_to_duckdb,
    #     op_args=["{{ task_instance.xcom_pull(task_ids='fetch_erp_task') }}"],  # Pull the file path from the first task
    # )


    run_dbt_transform = BashOperator(
        task_id='run_dbt_transform',
        bash_command="""
        cd /opt/airflow/etldbt/my_project && \
        dbt build --profiles-dir /opt/airflow/etldbt/my_project --target dev
        """,
    )

    #fetch_sales_order >> fetch_payment_entry >> fetch_customer_details >> 
    store_salesorderinto_duckdb >> store_paymententryinto_duckdb >> store_customerinto_duckdb >> run_dbt_transform
   
    


