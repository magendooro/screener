
#%%
import dask.dataframe as ddf
import pandas as pd
from talib import WILLR
from talib import EMA
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
# data = read_all_tickers('data/tickers')


# # %%
# g = check_gains(data)
# # %%
# gdf = g.loc[g.Date == g.Date.max()].loc[:, ['Symbol', 'Gains/Losses']].sort_values(by = 'Gains/Losses', ascending = True)
# top_decrease = gdf.iloc[:10]
# top_increase = gdf.iloc[-10:]
# # %%
# top_increase
# # %%

# for i in range(len(top_increase)):
#     print(top_increase.iloc[i]['Symbol'])
# # %%
# top_decrease.style.format({'Gains/Losses': '{:,.2%}'})
# %%
