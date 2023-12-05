from fastapi import APIRouter, Depends
from inference.linear_regression import LinearRegressionModel
from datetime import timedelta, datetime
from pykrx import stock

router = APIRouter(
    prefix='/predict',
    tags=["predict"],
    responses={
        404: { "description": "Not found"}
    }
)

def get_today():
    dt_now = str(datetime.now().date())
    print(f'{dt_now} 기준')
    dt_now = ''.join(c for c in dt_now if c not in '-')
    return dt_now

@router.get('')
async def get_predict():
    LRModel = LinearRegressionModel()
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


    return LRModel.predict(today_data)[0][0]