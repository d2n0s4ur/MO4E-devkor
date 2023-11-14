from datetime import timedelta, datetime
import pandas as pd

from pykrx import stock

def get_today():
    dt_now = str(datetime.now().date())
    print(f'{dt_now} 기준')
    dt_now = ''.join(c for c in dt_now if c not in '-')
    return dt_now

def get_market_fundamental():
    dt_now = get_today()
    df = stock.get_market_fundamental_by_ticker(date=dt_now)
    
    print(df.head())
    df.to_csv(f'./{dt_now}_market_fundamental.csv', index=True)


def select_columns():
    dt_now = get_today()
    df = pd.read_csv(f'./{dt_now}_market_fundamental.csv', index_col=0)
    
    df = df[['PER', 'PBR']]
    df.to_csv(f'./{dt_now}_market_fundamental.csv', index=True)
    
    
def remove_row_fundamental():
    dt_now = get_today()
    df = pd.read_csv(f'./{dt_now}_market_fundamental.csv', index_col=0)
    
    del_index = df[(df['PBR'] <= 0.2) | (df['PER'] <= 0)].index
    df = df.drop(del_index)
    print(df.head())
    df.to_csv(f'./{dt_now}_market_fundamental.csv')

    
def rank_fundamental():
    dt_now = get_today()
    df = pd.read_csv(f'./{dt_now}_market_fundamental.csv', index_col=0)
    
    df.describe(percentiles=[.1,.2,.3,.4,.5,.6,.7,.8,.9,1])
    rank_df = pd.DataFrame(columns=['PER', 'PBR'])

    for col in df.columns:
        rank_df[col] = df[col].apply(lambda x : change_to_rank(df, col, x))
        rank_df['rank_sum'] = rank_df.sum(axis=1)
        rank_df = rank_df.sort_values('rank_sum', ascending=False)
    
    print(rank_df.head())
    rank_df.to_csv(f'./{dt_now}_ranked_market_fundamental.csv', index=True)


def select_stock():
    dt_now = get_today()
    df = pd.read_csv(f'./{dt_now}_market_fundamental.csv', index_col=0)
    rank_df = pd.read_csv(f'./{dt_now}_ranked_market_fundamental.csv', index_col=0)

    num = 50
    selected_stock_df = df.loc[rank_df.iloc[:num].index]
    selected_stock_df.to_csv(f'./{dt_now}_selected_stock.csv', index=True)
    print(selected_stock_df.head())


def print_selected_stock():
    dt_now = get_today()
    selected_stock_df = pd.read_csv(f'./{dt_now}_selected_stock.csv', index_col=0)

    selected_ticker_list = selected_stock_df.index
    print(selected_ticker_list)
    selected_stocks = {}
    for ticker in selected_ticker_list:
        stock_name = stock.get_market_ticker_name(str(ticker).zfill(6))
        selected_stocks[ticker] = stock_name
        print(stock_name)
        
        
def change_to_rank(df, col, val):
    if val <= df[col].quantile(.1):
        return 10
    elif val <= df[col].quantile(.2):
        return 9
    elif val <= df[col].quantile(.3):
        return 8
    elif val <= df[col].quantile(.4):
        return 7
    elif val <= df[col].quantile(.5):
        return 6
    elif val <= df[col].quantile(.6):
        return 5
    elif val <= df[col].quantile(.7):
        return 4
    elif val <= df[col].quantile(.8):
        return 3
    elif val <= df[col].quantile(.9):
        return 2
    else :
        return 1