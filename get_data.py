# %%
import os
from re import T
import urllib
from bs4 import BeautifulSoup
import pandas as pd
import yfinance as yf 
import pandas_datareader as dtr
import datetime 
import time 
from tqdm import tqdm
from copy import deepcopy
from talib import WILLR
from talib import EMA

# %%
HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }


# %%
data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')


# %%
summary_table = data[0]
tickers = summary_table['Symbol'].values

# %%
sample_tickers = tickers[:10]

# %%
tickers_df = pd.DataFrame(tickers)


# %%
yf_params = {
    'start':datetime.date.fromisoformat('2021-06-01'),
    'end':datetime.date.fromisoformat('2022-01-01'),
    'pause': .1, 
    'adjust_price':True, 
    'ret_index':False,
    'interval':'d',
    'get_actions': True,
    'adjust_dividends': True
}
TICKER_FOLDER = 'data/tickers/'
# %%
def get_historical_data(tickers,
    previous_data = None,
    params = {},
    timeout = .1):
    print(f"YF parameters: {params}")
    collector = []
    unretrieved_count = 0
    for ticker in tqdm(tickers):
        try:
            df = dtr.get_data_yahoo(symbols = ticker, **params)
            # df.reset_index(inplace = True)
            df['Symbol'] = ticker
            collector.append(df)
            time.sleep(timeout)
        except:
            print(f"Couldn't retieve data for {ticker}")
            unretrieved_count += 1
    print(f"# Unretrieved tickers: {unretrieved_count}")
    collected_data = pd.concat(collector)
    collected_data.reset_index(inplace=True)
    collected_data.Date = collected_data.Date.apply(lambda x: x.date())
    if previous_data is not None:
        if not previous_data.empty:
            updated_data = pd.concat([collected_data, previous_data], axis=0)
            return collected_data, previous_data, updated_data
    else:
        return collected_data

#%%
def get_hist_data(ticker: str, params: dict = None) -> pd.DataFrame:
    df = dtr.get_data_yahoo(symbols = ticker, **params)
    df['Symbol'] = ticker
    return df
    
    
#%%
def collect_tickers(tickers, params: dict = None, timeout:float = 0.1) -> None:
    
    
    if not os.path.exists(TICKER_FOLDER):
        os.mkdir(TICKER_FOLDER)
    
    
    for ticker in tqdm(tickers):
        try:
            ticker_df = get_hist_data(ticker = ticker, params = params)
            ticker_df.to_csv(TICKER_FOLDER + ticker + '.csv', index_label= 'Date')
            time.sleep(timeout)
        except:
            print(f"Couldn't retrieve data for '{ticker}'", flush = True)


def update_tickers(tickers, params, timeout:float = .1):
    """ If ticker data has been previously saved, update it, otherwise get and save it """
    # Yesterday
    params['end'] = datetime.date.today() - datetime.timedelta(days=1) 

    for ticker in tqdm(tickers):
        ticker_path = TICKER_FOLDER + ticker + '.csv'
        if os.path.exists(ticker_path):
            old_df = pd.read_csv(ticker_path)
            new_start = datetime.date.fromisoformat(old_df.Date.max()) + datetime.timedelta(days=1)
            params['start'] = new_start
            
            new_df = get_hist_data(ticker = ticker, params = params)
            new_df.reset_index(inplace=True)
            new_df.Date = new_df.Date.apply(lambda x: x.date())
            updated_df = pd.concat([old_df, new_df], ignore_index = True)
            updated_df.to_csv(ticker_path)
            time.sleep(timeout)
        else:
            ticker_df = get_hist_data(ticker = ticker, params = params)
            ticker_df.to_csv(TICKER_FOLDER + ticker + '.csv', index_label= 'Date')
            time.sleep(timeout)

# THE PARAM PROBLEM could be solved in the main



# %%
# def get_daily_update(tickers,
#                     previous_data = None,
#                     date_string: str = None,
#                     yfparams: dict = None):
#     """ If date_string = None --> get today's """
#     params = deepcopy(yfparams)
    
#     if date_string is None:
#         last_weekday = get_last_weekday(datetime.datetime.today() - datetime.timedelta(days = 1))
#         params['end'] = last_weekday
#         params['start'] = last_weekday
#     else:
#         params['end'] = datetime.date.fromisoformat(date_string)
#         params['start'] = datetime.date.fromisoformat(date_string)


#     # When to update?
#     if params['start'].isoweekday() not in range(1,6):
#         print('The selected day is a weekend...')
#         raise AttributeError
    

#     daily_update = get_historical_data(tickers = tickers, previous_data = None, yfparams = params, timeout=.1)   
#     if previous_data is not None:
#         return pd.concat([previous_data, daily_update])
#     else:
#         return daily_update



# %%

# %%
def update_dataframe(tickers, previous_data: pd.DataFrame, params:dict, timeout = 1):
    """ Given a dataframe, update it to include everything until today """

    start = datetime.datetime.strptime(previous_data.Date.max(), '%Y-%m-%d') + datetime.timedelta(days = 1)
    end = datetime.date.today() - datetime.timedelta(days = 1) # Yesterday
    params['start'] = start
    params['end'] = end
    
    new_data = get_historical_data(tickers = tickers, previous_data = previous_data, params=params, timeout=timeout)
    return new_data




# %%
def calculate_metrics(updated_df, will_r_timeperiod = 21, ema_timeperiod = 13):
    tickers = updated_df.Symbol.unique()

    updated_df.sort_values(by = ['Symbol', 'Date'], ascending=[True, True], inplace = True)
    indexed_df = updated_df.reset_index()
    
    # Set empty columns for the large dataset
    indexed_df[['WillR','WillR_EMA']] = None

    for ticker in tickers:
        subset_of_interest = indexed_df.loc[indexed_df['Symbol'] == ticker]
        subset_of_interest.loc[subset_of_interest.index, 'WillR'] = WILLR(subset_of_interest.High, subset_of_interest.Low, subset_of_interest.Close, timeperiod = will_r_timeperiod)
        subset_of_interest.loc[subset_of_interest.index, 'WillR_EMA'] = EMA(subset_of_interest.WillR, timeperiod = ema_timeperiod)
        
        indexed_df.loc[subset_of_interest.index, 'WillR'] = subset_of_interest['WillR']
        indexed_df.loc[subset_of_interest.index, 'WillR_EMA'] = subset_of_interest['WillR_EMA']
    indexed_df.drop(columns = ['index'], inplace = True)
    return indexed_df

# %%



if __name__ == "__main__":
    data_path = 'data/stocks_data.csv'
    if os.path.exists(data_path):
        data = pd.read_csv(data_path, index_col=[0])
        c, p, u = update_dataframe(tickers, previous_data=data, params = yfparams, timeout=0.3)
        u.to_csv(data_path)
    else:
        data = get_historical_data(tickers = tickers, previous_data=None, params = yfparams)
        data.to_csv(data_path)

#%%

# %%
