
import pandas as pd

HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }

if __name__ == '__main__':
    data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    summary_table = data[0]
    tickers = summary_table[['Symbol', 'GICS Sector']]
    tickers.set_index('Symbol').to_csv('data/sp500.csv')




