
#%%

import dask.dataframe as ddf
import pandas as pd
from talib import WILLR
from talib import EMA
import copy 
from sqlalchemy import create_engine
import psycopg2

#%%
TICKER_FOLDER = 'data/tickers/'
PASSWORD = 'user2password' 
ENGINE = create_engine(url = f'postgresql+psycopg2://stocksuser2:{PASSWORD}@localhost/stocksdb')

# def read_all_tickers(folder_path):
#     df = ddf.read_csv(f"{folder_path}/*.csv", assume_missing = True)
#     df = df.compute()
#     return df
#%%
def query_database(query:str)-> pd.DataFrame:
    """
    query: SQL query or table name
    """
    with ENGINE.connect() as connection:
        df = pd.read_sql(sql = query, con = connection) 
        
    return df

def calculate_metrics(df, will_r_timeperiod = 21, ema_timeperiod = 13):
    tickers = df.symbol.unique()
    df = copy.deepcopy(df)
    df.sort_values(by = 'date', ascending= True, inplace = True)
    df.reset_index(inplace = True)
    
    updated_dfs = []
    for ticker in tickers:
        subset_of_interest = df.loc[df['symbol'] == ticker]
        subset_len = len(subset_of_interest)
        if (subset_len > will_r_timeperiod) and (subset_len > ema_timeperiod):
            subset_of_interest.loc[:, 'WillR'] = WILLR(subset_of_interest.high, subset_of_interest.low, subset_of_interest.close, timeperiod = will_r_timeperiod)
            subset_of_interest.loc[:, 'WillR_EMA'] = EMA(subset_of_interest.WillR, timeperiod = ema_timeperiod)
            updated_dfs.append(subset_of_interest)
        else:
            print(f"Unsufficient data for ticker '{ticker}'")
        
    new_df = pd.concat(updated_dfs, ignore_index=True)

    return new_df

def check_gains(df, timeframe = 1):
    tickers = df.symbol.unique()
    df = copy.deepcopy(df)

    df.sort_values(by = ['symbol', 'date'], ascending=[True, True], inplace = True)
    df.reset_index(inplace = True)

    updated_dfs = []
    for ticker in tickers:
        subset_of_interest = df.loc[df['symbol'] == ticker]
        subset_len = len(subset_of_interest)
        if subset_len > timeframe:
            subset_of_interest.loc[:, 'Gains/Losses'] = subset_of_interest.close.pct_change(periods = timeframe)
            
            updated_dfs.append(subset_of_interest)
    
    new_df = pd.concat(updated_dfs, ignore_index=True)
    return new_df

def calculate_AD(df):
    new_df = copy.deepcopy(df)
    grouped = new_df.groupby('symbol')
    new_df['change'] = grouped.close.pct_change().apply(lambda x: 1 if x > 0 else -1)
    daily_NA = new_df.groupby(by = 'date').change.sum()
    a_per_d = daily_NA.rolling(2).sum()
    a_per_d = pd.DataFrame(a_per_d).reset_index()
    a_per_d.columns = ['date', 's&p500']
    return a_per_d

def calculate_AD_EMA(df):
    a_per_d = copy.deepcopy(df)
    for ema in [3,7,10]:
        a_per_d[f'EMA_{ema}'] = EMA(a_per_d['s&p500'], timeperiod = ema)

    return a_per_d

def get_companies_by_industry(industry_name: str, ticker_data: pd.DataFrame, companies_in_dataset: list) -> list:
    companies_of_interest = ticker_data.loc[ticker_data['gics sector'] ==  industry_name].symbol.values
    valid_companies = set(companies_of_interest).intersection(companies_in_dataset)
    return list(valid_companies)

def calculate_industries_ads(data, ticker_data):
    
    unique_industries = ticker_data['gics sector'].unique()
    industries_AD = []
    overall_AD = calculate_AD(data).set_index('date')
    industries_AD.append(overall_AD)
    for industry in unique_industries:
        companies = get_companies_by_industry(industry_name=industry, ticker_data = ticker_data, companies_in_dataset = data.symbol.unique())
        industry_df = calculate_AD(data.loc[data.symbol.isin(companies)])
        industry_df.set_index('date', inplace = True)
        industry_df.rename(columns = {'s&p500': industry}, inplace = True)
        industries_AD.append(industry_df)

    industries_df = pd.concat(industries_AD, axis = 1)
    return industries_df

if __name__ == '__main__':
    pass
