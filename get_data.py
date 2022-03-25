import sys
import os
import json
import urllib


import pandas as pd
import yfinance as yf 
import pandas_datareader as dtr

import datetime 
import time 
from tqdm import tqdm

# yf.pdr_override()


yf_params = {
    "interval":'1d',
    "start" : datetime.date(2019,1,1),
    "end" : datetime.date.today(),
    "group_by":'ticker',
    "auto_adjust":True,
    "actions":True,
    "timeout": 5
}

TICKER_FOLDER = 'data/tickers/'

def get_hist_data(ticker: str, params: dict = None) -> pd.DataFrame:
    df = dtr.get_data_yahoo(symbols = ticker, **params)
    df['Symbol'] = ticker
    return df

def collect_tickers(tickers, params: dict = None, timeout:float = 1) -> None:
    if not os.path.exists(TICKER_FOLDER):
        os.makedirs(TICKER_FOLDER)
    
    tickers_obj = yf.Tickers(tickers)
    collected_data = tickers_obj.download(**params)
    
    ticker_dict = tickers_obj.tickers.keys()
    
    print()
    print('Saving tickers to their corresponding files...')
    
    for ticker in tqdm(list(ticker_dict)):
        ticker_path = TICKER_FOLDER + ticker + '.csv'
        data_subset = collected_data[ticker]
        data_subset.reset_index(inplace = True)
        data_subset = data_subset.assign(Symbol = ticker)
        data_subset.to_csv(ticker_path, index = False)
    

def update_tickers(params, timeout:float = 1):
    """ If ticker data has been previously saved, update it, otherwise get it and save it """
    
    params['end'] = datetime.date.today()
    
    # Instead of passing down a ticker param, why not check what's already saved..
    tickers = [s.split('.')[0] for s in os.listdir('data/tickers')]
    
    for ticker in tqdm(tickers):
        ticker_path = TICKER_FOLDER + ticker + '.csv'
        
        try:
            old_df = pd.read_csv(ticker_path)
            new_start = datetime.date.fromisoformat(old_df.Date.max())
            params['start'] = new_start

            if params['start'] == params['end']:
                params['start'] = params['end'] - datetime.timedelta(days=1)
            
            new_df = get_hist_data(ticker = ticker, params = params)
            new_df.reset_index(inplace=True)
            new_df.Date = new_df.Date.apply(lambda x: x.date())
            old_df.Date = old_df.Date.apply(lambda x: datetime.date.fromisoformat(x))

            updated_df = pd.concat([old_df, new_df], ignore_index = True)
            updated_df = updated_df.loc[updated_df.Date.drop_duplicates(keep = 'last').index]
            updated_df.to_csv(ticker_path, index = False)
            time.sleep(timeout)
        except:
            print(f"Couldn't update {ticker}")
        

if __name__ == "__main__":
    args = sys.argv
    with open('data/sp500_tickers.json', 'r') as f:
        ticker_dict = json.load(f)

    tickers = list(ticker_dict.values())[:20]
   
    if (args[1] == '-collect') or (args[1]== '--c'):
        collect_tickers(tickers, yf_params)
    elif (args[1] == '-update') or (args[1]== '--u'):
        update_tickers(yf_params)
    else: 
        print('Invalid argument..')
    