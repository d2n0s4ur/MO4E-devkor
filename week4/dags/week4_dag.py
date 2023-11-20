import os
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# __init__.py가 있어야 모듈로 인식하여 사용가능하다.
from src.model_algo import crawling_companyinfo_data, crawling_stockinfo_data, preprocess_data, model_train, predict_tommorow_price

seoul_time = pendulum.timezone('Asia/Seoul')
dag_name = os.path.basename(__file__).split('.')[0]

default_args = {
    'owner': 'd2n0s4ur',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id=dag_name,
    default_args=default_args,
    description='MO4E Study',
    schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2023, 10, 20, tz=seoul_time), # timezone
    catchup=False,
    tags=['MO4E', 'study']
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    crawling_companyinfo_task = PythonOperator(
        task_id='crawling_companyinfo_task',
        python_callable=crawling_companyinfo_data,
    )

    crawling_stockinfo_task = PythonOperator(
        task_id='crawling_stockinfo_task',
        python_callable=crawling_stockinfo_data,
    )

    preprocess_task = PythonOperator(
        task_id='preprocess_task',
        python_callable=preprocess_data,
    )

    model_train_task = PythonOperator(
        task_id='model_train_task',
        python_callable=model_train,
    )

    # predict_task = PythonOperator(
    #     task_id='predict_task',
    #     python_callable=predict_tommorow_price,
    # )
    
    start >> [crawling_companyinfo_task, crawling_stockinfo_task] >> preprocess_task >> model_train_task >> end