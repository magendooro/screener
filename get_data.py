
import sys
import os
import json

import pandas as pd
from metrics import PASSWORD
import yfinance as yf 


import datetime 
import time 
from tqdm import tqdm

from sqlalchemy import create_engine
import psycopg2



yf_params = {
    "interval":'1d',
    "start" : datetime.date(2019,1,1),
    "end" : datetime.date.today(),
    "group_by":'ticker',
    "auto_adjust":True,
    "actions":True,
    "timeout": 5
}

PASSWORD = 'user2password'
ENGINE = create_engine(url = f'postgresql+psycopg2://stocksuser2:{PASSWORD}@localhost/stocksdb')
TICKER_FOLDER = 'data/tickers/'


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
    
    table_name = 'daily_index_data'
    snp_data['Symbol'] = '^GSPC'
    snp_data['Updated'] = datetime.datetime.now()
    
    print(f"Saving {table_name.replace('_', ' ')} to the database.")
    snp_data.to_sql(name = table_name, con = ENGINE, if_exists = 'replace')

    

def update_tickers(params, timeout:float = 0):
    """ If ticker data has been previously saved, update it, otherwise get it and save it """
    
    # This assignment might be redundant, however will be left for now, in case the param dict is modified
    params['end'] = datetime.date.today()
    
    
    latest_entries_dates = pd.read_sql(sql = """
                SELECT DISTINCT ON ("Symbol")
                    "Symbol", "Date"
                FROM 
                    daily_stocks_data
                ORDER BY "Symbol", "Date" DESC
    """, con = ENGINE)
    unique_dates = latest_entries_dates.Date.unique()
    
    # STOCKS
    for date_value in unique_dates:
        tickers_list = latest_entries_dates.loc[latest_entries_dates.Date == date_value].Symbol.to_list()
        date_value = pd.to_datetime(date_value).date()
        params['start'] = date_value
        
        if params['start'] == params['end']:
                print(f"The data is up-to-date for {len(tickers_list)} tickers... Just in case, the last two days will be updated.")
                params['start'] = params['end'] - datetime.timedelta(days=1)
        
        print(f"Downloading data for {len(tickers)} tickers, between {params['start']} and {params['end']}...")
        tickers_obj = yf.Tickers(tickers_list)
        collected_data = tickers_obj.download(**params)
    
        ticker_dict = tickers_obj.tickers
    
        data_collector = []

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
        print(f"Saving {table_name.replace('_', ' ')} for {len(tickers_list)} tickers to the database.")
        data.to_sql(name = table_name, con = ENGINE, if_exists = 'append')

    # indices
    index_max_dates = pd.read_sql(sql = """
                SELECT DISTINCT ON ("Symbol")
                    "Symbol", "Date"
                FROM 
                    daily_index_data
                ORDER BY "Symbol", "Date" DESC
    """, con = ENGINE)
    
    max_date = index_max_dates.Date.unique()[0]
    date_value = pd.to_datetime(max_date).date()
    params['start'] = date_value
    
    if params['start'] == params['end']:
        print(f"The index is up-to-date... Just in case, the last two days will be updated.")
        print(f"Date range {params['start'], params['end']}")
        params['start'] = params['end'] - datetime.timedelta(days=1)
    # For now single index case
    print(f"Updating S&P500 index from {params['start']} to {params['end']}")
    snp_obj = yf.Ticker('^GSPC')
    snp_data = snp_obj.history(**params).reset_index()
    
    table_name = 'daily_index_data'
    snp_data['Symbol'] = '^GSPC'
    snp_data['Updated'] = datetime.datetime.now()
    
    print(f"Saving {table_name.replace('_', ' ')} to the database...")
    snp_data.to_sql(name = table_name, con = ENGINE, if_exists = 'append')
    
    
   

if __name__ == "__main__":
    args = sys.argv
    # with open('data/sp500_tickers.json', 'r') as f:
    #     ticker_dict = json.load(f)
    try:
        tickers = pd.read_sql(sql = 'SELECT "Symbol" FROM ticker_data', con = ENGINE)['Symbol'].to_list()
    except:
        print("No ticker could be retrieved from the database.")

    if (args[1] == '-collect') or (args[1]== '--c'):
        collect_tickers(tickers, yf_params)
    elif (args[1] == '-update') or (args[1]== '--u'):
        update_tickers(yf_params)
    else: 
        print('Invalid argument..')

