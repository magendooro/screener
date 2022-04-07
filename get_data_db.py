#%%
from genericpath import exists
import pandas as pd
# %%
import sys
import os
import json

import datetime 
import time 
from tqdm import tqdm
from metrics import read_all_tickers
from sqlalchemy import create_engine
import psycopg2
# %%
data = read_all_tickers('data/tickers')
# %%
password = 'user2password'
engine = create_engine(url = f'postgresql+psycopg2://stocksuser2:{password}@localhost/stocksdb1')
# %%

#%%
data['Updated'] = datetime.datetime.now()
# %%
data = data[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', 'Updated']]
data
# %%
data.to_sql(name = 'Stocks Database', con = engine, if_exists = 'replace' )
# %%
table_name = 'Stocks Database'
# %%
new_data = pd.read_sql(sql = 'daily_stocks_data', con = engine)
# %%
new_data
# %%
new_data
# %%
#%%
import sys

# %%
