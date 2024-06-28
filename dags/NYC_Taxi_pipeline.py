from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from scripts.Load_Data import *
from scripts.Transfrom_data import *
import logging


default_args = {
    'owner': 'Mahmoud',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'NYC_Taxi_pipeline',
    default_args=default_args,
    description='E-E taxi trip pipeline ',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

def load_credentials():
    return {
        "host": "172.20.0.3",
        "db_name": 'nyc_taxi2',
        "user": 'airflow',
        "password": 'airflow'
    }

def database_preparation():
    try:
        db_credentials = load_credentials()
        logging.info('database_preparation started')
        creat_tables(**db_credentials)
        logging.info('database_preparation done')
    except Exception as E :
        logging.info("Error in database_preparation", E, exc_info=True)

def data_extraction():
    try:
        logging.info('data_extraction started')
        process_Trip_Data()
        logging.info('data_extraction done')
    except Exception as e:
        logging.error("Error in data_extraction", exc_info=True)

def data_delivery():
    try:
        db_credentials = load_credentials()
        logging.info('data_delivery started')
        load_DimLocation(**db_credentials)
        load_DimRate(**db_credentials)
        load_DimPayment(**db_credentials)
        load_dimVendor(**db_credentials)
        Load_DimTrip_type(**db_credentials)
        load_fact(**db_credentials)
        logging.info('data_delivery done')
    except Exception as e:
        logging.error("Error in data_delivery", exc_info=True)

database_preparation_task = PythonOperator(
    task_id='database_preparation',
    python_callable=database_preparation,
    dag=dag,
)

data_extraction_task = PythonOperator(
    task_id='data_extraction',
    python_callable=data_extraction,
    dag=dag,
)

data_delivery_task = PythonOperator(
    task_id='data_delivery',
    python_callable=data_delivery,
    dag=dag,
)

database_preparation_task >> data_extraction_task >> data_delivery_task