#%%
from curses import window
from metrics import calculate_metrics, read_all_tickers
import pandas as pd 
from scipy.signal import find_peaks, find
import matplotlib.pyplot as plt
# %%
data = read_all_tickers('tickers copy')
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
mod_db
# %%
plt.figure(figsize=(50,5))
plt.plot(subset.index, subset.Close)
plt.scatter(x = peaks, y = subset.Close.iloc[peaks], alpha = .3, color = 'r')
plt.scatter(x = bottoms, y = subset.Close.iloc[bottoms], alpha = .3, color = 'g')
for db in mod_db:
    plt.plot(db.index, db.values, color = 'purple', alpha = .5)
    plt.axvspan(db.index[0], db.index[-1], color = 'pink', alpha = .5)
# %%
