import os
from datetime import timedelta, datetime
from pprint import pprint
import time
from airflow import DAG
from airflow.operators.python import PythonOperator


dag_name = os.path.basename(__file__).split('.')[0]

def print_fruit(fruit_name, **kwargs):
    print('=' * 60)
    print('fruit_name:', fruit_name)
    print('=' * 60)
    pprint(kwargs)
    print('=' * 60)
    return 'print complete!!!'

def sleep_seconds(seconds, **kwargs):
    print('=' * 60)
    print('seconds:' + str(seconds))
    print('=' * 60)
    pprint(kwargs)
    print('=' * 60)
    print('sleeping...')
    time.sleep(seconds)
    return 'sleep well!!!'

default_args = {
    'owner': 'devkor',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id=dag_name,
    default_args=default_args,
    description='',
    schedule_interval='@daily',
    start_date=datetime(2023, 9, 26, 00, 00),
    catchup=False,
    tags=['example']
) as dag:
    print_fruit_task = PythonOperator(
        task_id='print_fruit_task',
        python_callable=print_fruit,
        op_kwargs={'fruit_name': 'apple'},
    )
    sleep_seconds_task = PythonOperator(
        task_id='sleep_seconds_task',
        python_callable=sleep_seconds,
        op_kwargs={'seconds': 10},
    )

    print_fruit_task >> sleep_seconds_task