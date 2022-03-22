from bs4 import BeautifulSoup
import pandas as pd
import pandas_datareader as dtr
import datetime 
import time 
from copy import deepcopy
from talib import WILLR
from talib import EMA

HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }
data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
summary_table = data[0]
tickers = summary_table['Symbol'].values

# %%
sample_tickers = tickers[:10]

# %%
tickers_df = pd.DataFrame(tickers)
print(tickers_df)
tickers_df.to_json('data/sp500.json')