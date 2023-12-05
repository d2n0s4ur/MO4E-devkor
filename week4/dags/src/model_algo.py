from datetime import timedelta, datetime
import pandas as pd
from sklearn.linear_model import LinearRegression
import pickle
import boto3
from airflow.models import Variable
import os

from pykrx import stock

def get_today():
    dt_now = str(datetime.now().date())
    print(f'{dt_now} 기준')
    dt_now = ''.join(c for c in dt_now if c not in '-')
    return dt_now

def crawling_companyinfo_data():
    today = get_today()
    from_date = '20140101'
    to_date = today
    predict_target = '005930' #삼성전자
    # set constants
    df = stock.get_market_fundamental(from_date, to_date, predict_target)
    print(df.head())
    df.to_csv(f'./{today}_companyInfo.csv', index=True)

    return df.to_json()

def crawling_stockinfo_data():
    today = get_today()
    from_date = '20140101'
    to_date = today
    predict_target = '005930' #삼성전자
    # set constants
    stockInfo = stock.get_market_ohlcv(from_date, to_date, predict_target)
    print(stockInfo.head())
    stockInfo.to_csv(f'./{today}_stockInfo.csv', index=True)

    return stockInfo.to_json()

def preprocess_data(**context):
    companyInfo = context['task_instance'].xcom_pull(task_ids='crawling_companyinfo_task')
    stockInfo = context['task_instance'].xcom_pull(task_ids='crawling_stockinfo_task')

    companyInfo = pd.read_json(companyInfo)
    stockInfo = pd.read_json(stockInfo)

    # merge data
    data = companyInfo[['BPS', 'PER', 'PBR', 'EPS', 'DIV', 'DPS']]
    data['거래량'] = stockInfo[['거래량']]
    data['price'] = stockInfo['종가']
    data['tomorrow_price'] = stockInfo['시가'].shift(-1)
    data = data.dropna()
    
    # split data
    x_data = data[['BPS', 'PER', 'PBR', 'EPS', 'DIV', 'DPS', '거래량', 'price']]
    y_data = data[['tomorrow_price']]

    return x_data.to_json(), y_data.to_json()

def model_train(**context):
    x_data, y_data = context['task_instance'].xcom_pull(task_ids='preprocess_task')

    x_data = pd.read_json(x_data)
    y_data = pd.read_json(y_data)

    # 모델 생성
    lr = LinearRegression()

    # 모델 학습
    lr.fit(x_data, y_data)

    # 모델 저장
    today = get_today()
    pickle.dump(lr, open(f'./{today}_model.pkl', 'wb'))

    # create s3 client
    Bucket = Variable.get("S3_BUCKET_NAME")
    ClientAccessKey = Variable.get("S3_ACCESS_KEY")
    ClientSecret = Variable.get("S3_SECRET")
    ConnectionUrl = Variable.get("S3_ENDPOINT")
    PublicUrl = Variable.get("S3_PUBLIC_URL")

    s3 = boto3.client(
        service_name ="s3",
        endpoint_url = ConnectionUrl,
        aws_access_key_id = ClientAccessKey,
        aws_secret_access_key = ClientSecret,
        region_name="apac", # Must be one of: wnam, enam, weur, eeur, apac, auto
    )

    # upload model to s3
    s3.upload_file(f'./{today}_model.pkl', Bucket, f'{today}_model.pkl')

    predict_target = '005930' #삼성전자

    # 오늘 종가, 재무현황, 거래량을 통해 내일 주가를 예측
    day = get_today()
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

    return today_pred[0][0]


def predict_tommorow_price(**context):
    lr = context['task_instance'].xcom_pull(task_ids='model_train_task')
    predict_target = '005930' #삼성전자

    # 오늘 종가, 재무현황, 거래량을 통해 내일 주가를 예측
    day = get_today()
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

    return today_pred[0][0]