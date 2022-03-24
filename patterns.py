#%%
from metrics import calculate_metrics, read_all_tickers
import pandas as pd 
import scipy as sp
import seaborn as sns
# %%
data = read_all_tickers('data/tickers')
# %%
data.loc[data.duplicated().any()]
# %%

# %%
