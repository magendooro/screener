from operator import index
import streamlit as st
import pandas as pd
from bokeh.plotting import figure


import datetime
from metrics import calculate_metrics, read_all_tickers
import seaborn as sns
import matplotlib.pyplot as plt
from pandas.core.common import SettingWithCopyWarning
from metrics import read_all_tickers
from metrics import check_gains

from patterns import find_w_pattern
import warnings
import copy

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

day_mark_90 = datetime.date.today() - datetime.timedelta(days = 90)

@st.cache
def read_data():
    data = read_all_tickers('data/tickers/')
    data.Date = data.Date.apply(lambda x: datetime.date.fromisoformat(x))
    return data


@st.cache
def update_gains(data, timeframe = 1):
    return check_gains(data, timeframe)


@st.cache
def update_data(data, williams_choice, ema_choice):
    return calculate_metrics(data,
        will_r_timeperiod=williams_choice,
        ema_timeperiod=ema_choice)



plt.style.use('dark_background')
data = copy.deepcopy(read_data())
snp_data = pd.read_csv('data/snp_perf.csv')
snp_data.Date = snp_data.Date.apply(lambda x: datetime.date.fromisoformat(x))

intro = st.container()
intro.title("S&P500 Screener")
    
# Num tickers/timeframe 
dataset_info = st.container()
with dataset_info:
    st.markdown(f"The dataset contains daily financial data for **{len(data.Symbol.unique())}** unique tickers.")
    st.markdown(f"The included tickers span from **{data.Date.min()}** to **{data.Date.max()}**")
    
    st.markdown('S&P500 index:')
    x = snp_data.loc[snp_data.Date > day_mark_90].Date
    y = snp_data.loc[snp_data.Date > day_mark_90].Close
    p = figure(title = 'S&P Performance',
        x_axis_label = 'Date',
        y_axis_label = 'Close',
        x_axis_type ='datetime',
        
        plot_width = 800,
        plot_height = 200,
        tools = 'wheel_zoom, pan, reset')
    
    p.line(x, y,  line_width=2)
    p.toolbar.active_scroll = "auto"
    
    st.bokeh_chart(p, use_container_width=True)
gains_section = st.container()

with gains_section:
    st.header('Gains and losses')
    st.caption('Largest gains and losses in a given timeframe.')
    gains_columns = st.columns(2)
    with gains_columns[0]:
        gains_selection = st.select_slider(label = 'Timeframe', options = ['daily', 'weekly', 'monthly'])
    
    if gains_selection == 'daily':
        gains_df = update_gains(data, 1)
    elif gains_selection == 'weekly':
        gains_df = update_gains(data, 7)
    elif gains_selection == 'monthly':
        gains_df = update_gains(data, 1)
    gdf = gains_df.loc[gains_df.Date == gains_df.Date.max()].loc[:, ['Symbol', 'Close', 'Gains/Losses' ]].sort_values(by = 'Gains/Losses', ascending = False)


    top_decrease = gdf.iloc[-5:]
    top_increase = gdf.iloc[:5]
    loss_columns = st.columns(len(top_decrease))
    gains_columns = st.columns(len(top_increase))


    for i in reversed(range(len(top_increase))):
        loss_columns[i].metric(label = top_increase.iloc[i]['Symbol'], value = round(top_increase.iloc[i]['Close'],4), delta = str(round(top_increase.iloc[i]['Gains/Losses'], 3) * 100) + '%')

    for i in reversed(range(len(top_decrease))):
        gains_columns[i].metric(label = top_decrease.iloc[i]['Symbol'], value = round(top_decrease.iloc[i]['Close'], 4), delta = str(round(top_decrease.iloc[i]['Gains/Losses'], 3) * 100) + '%')

# User input and display tickers that satisfy the condition
parameter_selection = st.container()
with parameter_selection:
    st.header('**Select parameters**')
    williams_choice = st.slider(label = 'Williams R period', min_value=5, max_value = 100, value = 21)
    ema_choice = st.slider(label = 'EMA for Williams', min_value=3, max_value = 100, value = 13)
    st.header("Ranges of interest")
    st.caption("""
    Williams %R moves between 0 and -100. 
    Generally, a reading above -20 is overbought,
    and a reading below -80 is oversold. However, these can be picked on a use-case basis as well.
    """)
    filter_indep = st.checkbox(label = 'Filter independently')
    
    updated_data = update_data(data, williams_choice, ema_choice)
    col1, col2 = st.columns(2)
    # col1.subheader('Williams %R')
    # col2.subheader('EMA')
    
    will_lower_thresh, will_upper_thresh = col1.select_slider(label = 'Williams %R range', options = range(-100, 1, 1), value = (-80, -20))
    
    ema_lower_thresh, ema_upper_thresh = col2.select_slider(label = 'EMA range', options = range(-100, 1, 1), value = (-80, -20))
   
   



# Filter
filtered = st.container()
with filtered:
    st.header('Tickers of interest')
    col1_t, col2_t = st.columns(2)
    with col1_t:
        date_selection = st.date_input(
            label = 'Consider Williams %R on this day',
            value = data.Date.max(),
            min_value = data.Date.min(),
            max_value = data.Date.max())
    date_filtered_data = updated_data.loc[updated_data.Date == date_selection]

# Filtering results

if not filter_indep:
    thresh_filtered = date_filtered_data.loc[
            ((date_filtered_data.WillR > will_lower_thresh) & (date_filtered_data.WillR < will_upper_thresh)) &
            ((date_filtered_data.WillR_EMA > ema_lower_thresh) & (date_filtered_data.WillR_EMA < ema_upper_thresh))
            ]
    
else:
    thresh_filtered = date_filtered_data.loc[
            ((date_filtered_data.WillR > will_lower_thresh) & (date_filtered_data.WillR < will_upper_thresh)) |
            ((date_filtered_data.WillR_EMA > ema_lower_thresh) & (date_filtered_data.WillR_EMA < ema_upper_thresh))
            ]
    

with filtered:
    with col2_t:
        st.markdown(f"The number of tickers that satisfy the above condition is **{len(thresh_filtered)}**")



st.caption(f"The table below can be sorted by clicking on the column header.")
st.dataframe(thresh_filtered[['Date', 'Symbol', 'WillR', 'WillR_EMA', 'Close']])




# Select filtered tickers
# Detailed exploration and plots

# Where ema < W%R
exploration = st.container()
with exploration:
    exploration_choice = st.selectbox(label = "Inspect the following ticker:", options = thresh_filtered.Symbol.unique() )
    date_col1, date_col2 = st.columns(2)
    with date_col1:
        date_lower = st.date_input(
            label = 'From',
            value = day_mark_90,
            min_value = updated_data.Date.min(),
            max_value = updated_data.Date.max())
    with date_col2:
        date_upper = st.date_input(
            label = 'To',
            value = updated_data.Date.max(),
            min_value = updated_data.Date.min(),
            max_value = updated_data.Date.max())

    subset = updated_data.loc[(updated_data.Symbol == exploration_choice) & (updated_data.Date > date_lower) & ( updated_data.Date < date_upper)]


    # major_ticks = pd.date_range(start = date_lower, end = date_upper, periods = 10)
    # minor_ticks = pd.date_range(start = date_lower, end = date_upper, periods = 20)

    # fig, axs = plt.subplots(nrows = 2, ncols = 1,figsize = (15, 5), sharex = True)
    
    # axs[0].plot(subset.Date, subset.WillR, label = 'W%R')
    # axs[0].plot(subset.Date, subset.WillR_EMA, label = 'EMA')
    # axs[0].set_title('Williams %R and EMA of Williams %R')
    # # axs[0].grid(which='minor', alpha=0.2)
    # axs[0].grid(which='major', alpha=0.8)
    # axs[0].set_xticks(major_ticks)
    # # axs[0].set_xticks(minor_ticks, minor=True)

    # axs[1].plot(subset.Date, subset.Close, label = 'Close')
    # axs[1].set_title('Closing price')
    # # axs[1].grid(which='minor', alpha=0.2)
    # axs[1].grid(which='major', alpha=0.8)
    # axs[1].set_xticks(major_ticks)
    # # axs[1].set_xticks(minor_ticks, minor=True)
    
    
    # plt.legend()
    
    # plt.xticks(rotation = 45)
    # st.pyplot(fig)

    close_p = figure(title = f"Closing price data for ${exploration_choice}",
        x_axis_label = 'Date',
        y_axis_label = 'Close',
        x_axis_type ='datetime',
        plot_width = 1000,
        plot_height = 200,
        tools = 'wheel_zoom, pan, reset')
    close_p.line(x = subset.Date, y = subset['Close'], line_width = 1)

    will_ema_p = figure(title = f"Williams %R data for ${exploration_choice}",
        x_axis_label = 'Date',
        y_axis_label = 'Williams %R',
        x_axis_type ='datetime',
        plot_width = 1000,
        plot_height = 200,
        tools = 'wheel_zoom, pan, reset')
    will_ema_p.line(x = subset.Date, y = subset['WillR'], line_width = 1, color = 'coral', legend_label = 'Williams %R')
    will_ema_p.line(x = subset.Date, y = subset['WillR_EMA'], line_width = 1, color = 'black', legend_label = f"EMA {ema_choice}", line_dash = 'dashed')
    will_ema_p.legend.location = 'top_left'
    st.bokeh_chart(close_p,  use_container_width = True)
    st.bokeh_chart(will_ema_p,  use_container_width = True)

    pattern_close = find_w_pattern(subset, column_of_interest='Close')
    pattern_will = find_w_pattern(subset, column_of_interest='WillR')

    st.bokeh_chart(pattern_close, use_container_width = True)

    st.bokeh_chart(pattern_will, use_container_width = True)






