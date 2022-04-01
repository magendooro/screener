
#%%
import dask.dataframe as ddf
import pandas as pd
from talib import WILLR
from talib import EMA
import copy
#%%

TICKER_FOLDER = 'data/tickers/'
#%%
def read_all_tickers(folder_path):
    df = ddf.read_csv(f"{folder_path}/*.csv", assume_missing = True)
    df = df.compute()
    return df
#%%
def calculate_metrics(df, will_r_timeperiod = 21, ema_timeperiod = 13):
    tickers = df.Symbol.unique()
    df = copy.deepcopy(df)
    df.sort_values(by = ['Symbol', 'Date'], ascending=[True, True], inplace = True)
    df.reset_index(inplace = True)
    
    updated_dfs = []
    for ticker in tickers:
        subset_of_interest = df.loc[df['Symbol'] == ticker]
        subset_len = len(subset_of_interest)
        if (subset_len > will_r_timeperiod) and (subset_len > ema_timeperiod):
            subset_of_interest.loc[:, 'WillR'] = WILLR(subset_of_interest.High, subset_of_interest.Low, subset_of_interest.Close, timeperiod = will_r_timeperiod)
            subset_of_interest.loc[:, 'WillR_EMA'] = EMA(subset_of_interest.WillR, timeperiod = ema_timeperiod)
            updated_dfs.append(subset_of_interest)
        else:
            print(f"Unsufficient data for ticker '{ticker}'")
        
    new_df = pd.concat(updated_dfs, ignore_index=True)

    return new_df
#%%
def check_gains(df, timeframe = 1):
    tickers = df.Symbol.unique()
    df = copy.deepcopy(df)

    df.sort_values(by = ['Symbol', 'Date'], ascending=[True, True], inplace = True)
    df.reset_index(inplace = True)

    updated_dfs = []
    for ticker in tickers:
        subset_of_interest = df.loc[df['Symbol'] == ticker]
        subset_len = len(subset_of_interest)
        if subset_len > timeframe:
            subset_of_interest.loc[:, 'Gains/Losses'] = subset_of_interest.Close.pct_change(periods = timeframe)
            
            updated_dfs.append(subset_of_interest)
    
    new_df = pd.concat(updated_dfs, ignore_index=True)
    return new_df


if __name__ == '__main__':
    pass

#%%
data = read_all_tickers('data/tickers')

# %%
data
# %%
tickers = data.Symbol.unique()
data.sort_values(by = ['Symbol', 'Date'], ascending=[True, True], inplace = True)
updated_dfs = []
for ticker in tickers:
    subset_of_interest = data.loc[data['Symbol'] == ticker]
    subset_len = len(subset_of_interest)
    subset_of_interest.assign(Daily_change = subset_of_interest.Close.pct_change(periods = 1), inplace = True)
    net_advance = subset_of_interest['Daily_change'].apply(lambda x: 1 if x > 0 else -1)

    updated_dfs.append(subset_of_interest)

new_df = pd.concat(updated_dfs, ignore_index = True)
    

# %%
subset = data.loc[:['MSFT', 'AAPL', 'FB']]
 # %%
subset
# %%
subset['Close_change'] = subset.Close.pct_change()

# %%
subset.Close_change.apply(lambda x: 1 if x > 0 else -1) 
# %%
