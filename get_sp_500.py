
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

PASSWORD  = 'user2password'
ENGINE = create_engine(url = f'postgresql+psycopg2://stocksuser2:{password}@localhost/stocksdb1')

if __name__ == '__main__':
    data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    summary_table = data[0]
    tickers = summary_table[['Symbol', 'GICS Sector']]
    tickers.set_index('Symbol').to_sql('data/sp500.csv', con = ENGINE, if_exists='replace')




# %%
