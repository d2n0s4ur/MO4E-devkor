import os
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# __init__.py가 있어야 모듈로 인식하여 사용가능하다.
from src.quant_algo import get_market_fundamental, select_columns, remove_row_fundamental, rank_fundamental, select_stock, print_selected_stock

seoul_time = pendulum.timezone('Asia/Seoul')
dag_name = os.path.basename(__file__).split('.')[0]

default_args = {
    'owner': 'devkor',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

# def get_matket_fundamental():
#     print('get_market_fundamental')

# 클린 파이썬 기준, with 문을 쓸 수 있으면 쓰는게 좋다.
with DAG(
    dag_id=dag_name,
    default_args=default_args,
    description='중간고사 화이팅~.~',
    schedule_interval=timedelta(minutes=10),
    start_date=pendulum.datetime(2023, 10, 9, tz=seoul_time), # timezone 세팅
    catchup=False,
    tags=['quant', 'example']
) as dag:
    get_market_fundamental_task = PythonOperator(
        task_id='get_market_fundamental_task',
        python_callable=get_market_fundamental,
    )
    
    select_columns_task = PythonOperator(
        task_id='select_columns_task',
        python_callable=select_columns,
    )
    
    remove_row_fundamental_task = PythonOperator(
        task_id='remove_row_fundamental_task',
        python_callable=remove_row_fundamental,
    )
    
    rank_fundamental_task = PythonOperator(
        task_id='rank_fundamental_task',
        python_callable=rank_fundamental,
    )
    
    select_stock_task = PythonOperator(
        task_id='select_stock_task',
        python_callable=select_stock,
    )
    
    print_selected_stock_task = PythonOperator(
        task_id='print_selected_stock_task',
        python_callable=print_selected_stock,
    )
    
    get_market_fundamental_task >> select_columns_task >> remove_row_fundamental_task >> rank_fundamental_task >> select_stock_task >> print_selected_stock_task