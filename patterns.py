#%%
import pandas as pd
from bokeh.plotting import figure, show
from bokeh.models import Label
from bokeh.models import Span

from scipy.signal import find_peaks
import numpy as np
#%%
def find_w_pattern(subset, column_of_interest = "close"):
    subset['Gradient'] = np.gradient(subset[column_of_interest].rolling(center=False,window=4).mean())
    subset.reset_index(inplace = True, drop= True)
    peaks, peak_properties = find_peaks(subset[column_of_interest], plateau_size = 1)
    bottoms, bottom_properties =  find_peaks(subset[column_of_interest] * - 1, plateau_size=1)
    local_extrema = [*peaks, *bottoms] # merging the two lists
    f_price = subset.iloc[sorted(local_extrema)][column_of_interest]

    window_size = 5
    db_count = 0
    double_bottoms = []
    for i in range(len(f_price) - window_size + 1):
        x = f_price[i:i+window_size]
        
        xl = x.to_list()
        
        if xl[0] > xl[1] and xl[1] < xl[2] and xl[2] > xl[3] and xl[1] < xl[3] and xl[3] < xl[4] and xl[2] < xl[4]:
            if xl[0] * .99 > xl[1]: # exclude minor movements
                if subset['Gradient'].iloc[x.index[1]:x.index[2]].is_monotonic and subset['Gradient'].iloc[x.index[2]:x.index[3]].is_monotonic:
                    double_bottoms.append(x)
                    db_count += 1

    
    mod_db = [ db*.9 for db in double_bottoms] 

    p = figure(title = f'W patterns in {column_of_interest}',
        x_axis_label = 'Date',
        y_axis_label = 'W%R',
        x_axis_type ='datetime',
        plot_width = 1000,
        plot_height = 200,
        tools = 'wheel_zoom, pan, reset')
    
    
    p.line(x = subset['date'], y = subset[column_of_interest], line_width = 1)
    p.circle(x = subset['date'].iloc[peaks], y = subset[column_of_interest].iloc[peaks], size = 5, color = 'green', alpha = .5)
    p.circle(x = subset['date'].iloc[bottoms], y = subset[column_of_interest].iloc[bottoms], size = 5, color = 'red', alpha = .5)
    idx = len(mod_db)
    for db in mod_db:
        p.line(x = subset['date'].iloc[db.index], y = db.values, color = 'magenta')
        w_begin = Span(location = subset['date'].iloc[db.index[0]], dimension = 'width', line_color='#009E73',
                              line_dash='dashed', line_width=3 )
        w_end = Span(location = subset['date'].iloc[db.index[-1]], dimension = 'width', line_color='#009E73',
                              line_dash='dashed', line_width=3)
        p.add_layout(w_begin)
        p.add_layout(w_end)
        w_idx = Label(x = subset['date'].iloc[db.index[0]], y = db.values[0], text = str(idx))
        p.triangle(x = subset['date'].iloc[db.index[0]], y = db.values[0], color = 'pink', size = 10)
        p.add_layout(w_idx)
        idx -= 1
    
    return p

#%%
 
if __name__ == '__main__':
    pass