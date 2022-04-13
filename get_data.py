#%%
import sys
import os
import json

import pandas as pd
from metrics import PASSWORD
import yfinance as yf 
import pandas_datareader as dtr

import datetime 
import time 
from tqdm import tqdm

from sqlalchemy import create_engine
import psycopg2

#%%

yf_params = {
    "interval":'1d',
    "start" : datetime.date(2019,1,1),
    "end" : datetime.date(2022, 1, 10),
    "group_by":'ticker',
    "auto_adjust":True,
    "actions":True,
    "timeout": 5
}
PASSWORD = 'user2password'
ENGINE = create_engine(url = f'postgresql+psycopg2://stocksuser2:{PASSWORD}@localhost/stocksdb')
TICKER_FOLDER = 'data/tickers/'
#%%
def get_hist_data(ticker: str, params: dict = None) -> pd.DataFrame:
    df = dtr.get_data_yahoo(symbols = ticker, **params)
    df['Symbol'] = ticker
    return df


def collect_tickers(tickers, params: dict = None, timeout:float = 1) -> None:

    print()
    print(f"Downloading data for {len(tickers)} tickers, between {params['start']} and {params['end']}...")
    tickers_obj = yf.Tickers(tickers)
    collected_data = tickers_obj.download(**params)
    
    ticker_dict = tickers_obj.tickers
    
    data_collector = []
    print('Assign symbols...')
    # Assign symbols and drop empty subsets (failed download)
    for ticker in tqdm(list(ticker_dict.keys())):
        data_subset = collected_data[ticker]
        data_subset.reset_index(inplace = True)
        data_subset = data_subset.assign(Symbol = ticker)
        if not data_subset.loc[:, data_subset.columns.difference(['Date', 'Symbol'])].isna().all().all():
            data_collector.append(data_subset)
    
    data = pd.concat(data_collector, axis = 0, ignore_index = True)
    
    data['Updated'] = datetime.datetime.now()
    data = data[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', 'Updated']]
    # Saving to the database; if table exists, overwrite
    table_name = 'daily_stocks_data'
    print()
    print(f"Saving {table_name.replace('_', ' ')} to the database.")
    data.to_sql(name = table_name, con = ENGINE, if_exists = 'replace')
    
    print()
    print('Accesing S&P500 index..')
    snp_obj = yf.Ticker('^GSPC')
    snp_data = snp_obj.history(**yf_params).reset_index()
    snp_data.assign(Symbol = '^GSPC')
    
    table_name = 'daily_index_data'
    snp_data.assign(Symbol = "S&P500")
    snp_data['Updated'] = datetime.datetime.now()
    
    print(f"Saving {table_name.replace('_', ' ')} to the database.")
    snp_data.to_sql(name = table_name, con = ENGINE, if_exists = 'replace')

    

def update_tickers(params, timeout:float = 0):
    """ If ticker data has been previously saved, update it, otherwise get it and save it """
    
    # This assignment might be redundant, however will be left for now, in case the param dict is modified
    params['end'] = datetime.date.today()
    
    # Instead of passing down a ticker param, why not check what's already saved..
    tickers = [s.split('.')[0] for s in os.listdir(TICKER_FOLDER)]
    print(f"Updating {len(tickers)} tickers..")
    for ticker in tqdm(tickers):
        ticker_path = TICKER_FOLDER + ticker + '.csv'
        try:

            old_df = pd.read_csv(ticker_path)
            new_start = datetime.date.fromisoformat(old_df.Date.max())
            params['start'] = new_start
            if params['start'] == params['end']:
                print("The data is up-to-date... Just in case, the last two days will be updated.")
                params['start'] = params['end'] - datetime.timedelta(days=1)

            ticker_obj = yf.Ticker(ticker)
            new_df = ticker_obj.history(**params)
            new_df.reset_index(inplace = True)
            new_df = new_df.assign(Symbol = ticker)

            new_df.Date = new_df.Date.apply(lambda x: x.date())
            old_df.Date = old_df.Date.apply(lambda x: datetime.date.fromisoformat(x))

            updated_df = pd.concat([old_df, new_df], ignore_index = True)
            updated_df = updated_df.loc[updated_df.Date.drop_duplicates(keep = 'last').index]
            updated_df.to_csv(ticker_path, index = False)
            time.sleep(timeout)
        
        except:
            print(f"Couldn't update {ticker}...")

    params["start"] = datetime.date(2019,1,1)
    params['end'] = datetime.date.today()
    print('Accesing S&P index..')
    snp_obj = yf.Ticker('^GSPC')
    snp_data = snp_obj.history(**yf_params).reset_index()
    snp_data.to_csv('data/snp_perf.csv', index = False)
        
#%%
if __name__ == "__main__":
    args = sys.argv
    # with open('data/sp500_tickers.json', 'r') as f:
    #     ticker_dict = json.load(f)
    tickers = pd.read_csv('data/sp500.csv')
    tickers = list(tickers.Symbol)

    if (args[1] == '-collect') or (args[1]== '--c'):
        collect_tickers(tickers, yf_params)
    elif (args[1] == '-update') or (args[1]== '--u'):
        update_tickers(yf_params)
    else: 
        print('Invalid argument..')


# %%
