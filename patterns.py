#%%
from metrics import calculate_metrics, read_all_tickers
import pandas as pd 
from scipy.signal import find_peaks
import matplotlib.pyplot as plt
import datetime
import pandas_datareader as dtr
# %%
data = read_all_tickers('tickers')
#%%
# %%
subset = data.loc[data.Symbol == 'MSFT']
# %%
peaks, peak_properties = find_peaks(subset.Close)
bottoms, bottom_properties =  find_peaks(subset.Close * - 1)
#%%
plt.figure(figsize=(15,5))
plt.plot(subset.index, subset.Close)
plt.scatter(x = peaks, y = subset.Close.iloc[peaks], alpha = .3, color = 'r')
plt.scatter(x = bottoms, y = subset.Close.iloc[bottoms], alpha = .3, color = 'g')



# %%
peaks
# %%
bottoms
# %%
f = [*peaks, *bottoms]
# %%
sorted(f)
# %%
f_price = subset.iloc[sorted(f)].Close
# %%
window_size = 5
db_count = 0
double_bottoms = []
for i in range(len(f_price) - window_size + 1):
    x = f_price[i:i+window_size]
    xl = x.to_list()
    
    if xl[0] > xl[1] and xl[1] < xl[2] and xl[2] > xl[3] and xl[1] < xl[3] and xl[3] < xl[4] and xl[2] < xl[4]:
        double_bottoms.append(x)
        db_count += 1
db_count


# %%
mod_db = [ db*.97 for db in double_bottoms] 

# %%
plt.figure(figsize=(50,5))
plt.plot(subset.index, subset.Close)
plt.scatter(x = peaks, y = subset.Close.iloc[peaks], alpha = .9, color = 'g')
plt.scatter(x = bottoms, y = subset.Close.iloc[bottoms], alpha = .9, color = 'r')
i = 0
for db in mod_db:
    plt.plot(db.index, db.values, color = 'purple',linestyle = '-', alpha = .5)
    plt.axvspan(db.index[0], db.index[-1], color = 'lightgray', alpha = .5) 
    plt.text(x = db.index[0], y = db.values[0] * .98, s = str(i))
    i+=1
 # %%


































yf_params = {
    'start':datetime.date.today() - datetime.timedelta(days = 1),
    'end':datetime.date.today(),
    'pause': .2, 
    'adjust_price':True, 
    'ret_index':False,
    'interval':'d',
    'get_actions': True,
    'adjust_dividends': True
}
tickers = [s.split('.')[0] for s in os.listdir('data/tickers')]
#%%

# %%
x = dtr.get_data_yahoo(symbols = tickers, **yf_params)
# %%
x.T
# %%
x.columns
# %%
 