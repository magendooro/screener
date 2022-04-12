#%%
import sys
import os
import json

import pandas as pd
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
    "end" : datetime.date.today(),
    "group_by":'ticker',
    "auto_adjust":True,
    "actions":True,
    "timeout": 5
}

TICKER_FOLDER = 'data/tickers/'
#%%


def collect_tickers(stock_tickers, index_tickers, params: dict = None, timeout:float = 1) -> None:
    password = 'user2password'
    engine = create_engine(url = f'postgresql+psycopg2://stocksuser2:{password}@localhost/stocksdb1')
    
    # Add S&P ticker
    if stock_tickers:
        tickers_obj = yf.Tickers(stock_tickers)
        collected_data = tickers_obj.download(**params)
        
        ticker_dict = tickers_obj.tickers
        
        data_collector = []
        for ticker in tqdm(list(ticker_dict.keys())):
            data_subset = collected_data[ticker]
            data_subset.reset_index(inplace = True)
            data_subset = data_subset.assign(Symbol = ticker)
            data_collector.append(data_subset)
        
        data = pd.concat(data_collector, axis = 0, ignore_index = True)
        
        data['Updated'] = datetime.datetime.now()
        data = data[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', 'Updated']]
        # Saving to the database; if table exists, overwrite
        table_name = 'daily_stocks_data'
        data.to_sql(name = table_name, con = engine, if_exists = 'replace')

    print('Accesing index data..')

    if index_tickers:
        tickers_obj = yf.Tickers(stock_tickers)
        collected_data = tickers_obj.download(**params)
        
        ticker_dict = tickers_obj.tickers
        
        data_collector = []
        for ticker in tqdm(list(ticker_dict.keys())):
            data_subset = collected_data[ticker]
            data_subset.reset_index(inplace = True)
            data_subset = data_subset.assign(Symbol = ticker)
            data_collector.append(data_subset)
        
        data = pd.concat(data_collector, axis = 0, ignore_index = True)
        
        data['Updated'] = datetime.datetime.now()
        data = data[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', 'Updated']]
        
        # Saving to the database; if table exists, overwrite
        
        table_name = 'daily_index_data'
        data.to_sql(name = table_name, con = engine, if_exists = 'replace')

    

def update_tickers(params, timeout:float = 0):
    """ If ticker data has been previously saved, update it, otherwise get it and save it """
    
    password = 'user2password'
    engine = create_engine(url = f'postgresql+psycopg2://stocksuser2:{password}@localhost/stocksdb1')

    # This assignment might be redundant, however will be left for now, in case the param dict is modified
    params['end'] = datetime.date.today()
    
    # Query unique tickers
    tickers = ...

    # Query last date for symbol
    groups = ... # Group date, ticker list 
    # Group by date
    for group in groups:
        group_date, group_tickers = group
        params['start'] = group_date
        tickers_obj = yf.Tickers(group_tickers)
        collected_data = tickers_obj.download(**params)
    
        ticker_dict = tickers_obj.tickers
    
        data_collector = []
        for ticker in tqdm(list(ticker_dict.keys())):
            data_subset = collected_data[ticker]
            data_subset.reset_index(inplace = True)
            data_subset = data_subset.assign(Symbol = ticker)
            data_collector.append(data_subset)
    
        data = pd.concat(data_collector, axis = 0, ignore_index = True)
    
        data['Updated'] = datetime.datetime.now()
        data = data[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', 'Updated']]
        # Saving to the database; if table exists, append
        table_name = 'daily_stocks_data'
        data.to_sql(name = table_name, con = engine, if_exists = 'append')

    
    # Query last date for indexes
    indexes_groups = ...

    for group in indexes_groups:
        group_date, group_tickers = group
        params['start'] = group_date
        tickers_obj = yf.Tivkers(group_tickers)
        collected_data = tickers_obj.download(**params)
    
        ticker_dict = tickers_obj.tickers
    
        data_collector = []
        for ticker in tqdm(list(ticker_dict.keys())):
            data_subset = collected_data[ticker]
            data_subset.reset_index(inplace = True)
            data_subset = data_subset.assign(Symbol = ticker)
            data_collector.append(data_subset)
    
        data = pd.concat(data_collector, axis = 0, ignore_index = True)
    
        data['Updated'] = datetime.datetime.now()
        data = data[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', 'Updated']]
    
        table_name = 'daily_index_data'
        data.to_sql(name = table_name, con = engine, if_exists = 'append')
        
#%%
if __name__ == "__main__":
    args = sys.argv
    # with open('data/sp500_tickers.json', 'r') as f:
    #     ticker_dict = json.load(f)
    tickers = pd.read_csv('data/sp500.csv') # TODO REPLACE w/ db query
    tickers = list(tickers.Symbol)

    if (args[1] == '-collect') or (args[1]== '--c'):
        collect_tickers(tickers, yf_params)
    elif (args[1] == '-update') or (args[1]== '--u'):
        update_tickers(yf_params)
    else: 
        print('Invalid argument..')

#%%
tickers

# %%
