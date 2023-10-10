import os
from datetime import timedelta, datetime
from pprint import pprint
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

predict_target = '005930' #삼성전자
from_date = '20140101'
to_date = datetime.today().strftime("%Y%m%d")
companyInfo = []
stockInfo = []
data = []
x_data = []
y_data = []
x_train, x_test, y_train, y_test = [], [], [], []

dag_name = os.path.basename(__file__).split('.')[0]

def crawling_data(**kwargs):
    #dependencies
    from pykrx import stock
    global companyInfo, stockInfo

    # set constants
    companyInfo = stock.get_market_fundamental(from_date, to_date, predict_target)
    companyInfo.to_csv('companyInfo.csv')

    stockInfo = stock.get_market_ohlcv(from_date, to_date, predict_target)
    stockInfo.to_csv('stockInfo.csv')

    return 'crawling complete!!!'

def split_train_test(**kwargs):
    #dependencies
    from sklearn.model_selection import train_test_split
    global companyInfo, stockInfo, data, x_data, y_data, x_train, x_test, y_train, y_test

    # merge data
    data = companyInfo[['BPS', 'PER', 'PBR', 'EPS', 'DIV', 'DPS']]
    data['거래량'] = stockInfo[['거래량']]
    data['price'] = stockInfo['종가']
    data['tomorrow_price'] = stockInfo['시가'].shift(-1)
    data = data.dropna()
    
    # split data
    x_data = data[['BPS', 'PER', 'PBR', 'EPS', 'DIV', 'DPS', '거래량', 'price']]
    y_data = data[['tomorrow_price']]

    # train, test set 분리
    x_train, x_test, y_train, y_test = train_test_split(x_data, y_data, test_size=0.2, random_state=0)

    return 'split complete!!!'

def model_training(**kargs):
    #dependencies
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    import datetime
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_squared_error, r2_score
    from pykrx import stock

    # set constants
    global x_train, x_test, y_train, y_test

    # 모델 생성
    lr = LinearRegression()

    # 모델 학습
    lr.fit(x_train, y_train)

    # 예측
    y_pred = lr.predict(x_test)

    # 예측 결과를 데이터 프레임으로 만들어 확인
    df = pd.DataFrame({'actual': y_test['tomorrow_price'].to_numpy().flatten(),
		'predict': y_pred.flatten()})

    # 모델 accuracy 확인
    mse = mean_squared_error(df['actual'], df['predict'])
    rmse = np.sqrt(mse)
    r2 = r2_score(df['actual'], df['predict'])
    print('MSE: ', mse)
    print('RMSE: ', rmse)
    print('R2: ', r2)

    # 모델 저장
    import pickle
    pickle.dump(lr, open('model.pkl', 'wb'))

    # 오늘 종가, 재무현황, 거래량을 통해 내일 주가를 예측
    day = datetime.now().strftime("%Y%m%d")
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")

    # 재무현황
    today_companyInfo = stock.get_market_fundamental(week_ago, day, predict_target).tail(1)

    # 거래량, 종가
    today_stockInfo = stock.get_market_ohlcv(week_ago, day, predict_target).tail(1)

    # feature: 재무현황, 거래량
    today_data = today_companyInfo[['BPS', 'PER', 'PBR', 'EPS', 'DIV', 'DPS']]
    today_data['거래량'] = today_stockInfo[['거래량']]
    today_data['price'] = today_stockInfo['종가']

    today_pred = lr.predict(today_data)
    print("예측값:", today_pred[0][0])

    return 'model training complete!!!'

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
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    crawling_data_task = PythonOperator(
        task_id='crawling_data_task',
        python_callable=crawling_data,
        op_kwargs={'seconds': 10},
    )

    split_train_test_task = PythonOperator(
        task_id='split_train_test_task',
        python_callable=split_train_test,
        op_kwargs={'seconds': 10},
    )

    model_training_task = PythonOperator(
        task_id='model_training_task',
        python_callable=model_training,
        op_kwargs={'seconds': 10},
    )

    start >> crawling_data_task >> split_train_test_task >> model_training_task >> end