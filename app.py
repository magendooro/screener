#%%
from numpy import insert
import streamlit as st
import pandas as pd
from bokeh.plotting import figure
from bokeh.palettes import Turbo256, Category20, Spectral
from bokeh.palettes import brewer

import numpy as np
from sklearn.preprocessing import MinMaxScaler

import datetime
from metrics import calculate_metrics
import seaborn as sns
import matplotlib.pyplot as plt
from pandas.core.common import SettingWithCopyWarning

from metrics import query_database
# from metrics import read_all_tickers
from metrics import check_gains
from metrics import calculate_AD_EMA
from metrics import calculate_AD
from metrics import calculate_industries_ads

from patterns import find_w_pattern
import warnings
import copy

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

day_mark_90 = datetime.date.today() - datetime.timedelta(days = 150)

@st.cache 
def get_industries():
    ticker_data = pd.read_csv('data/sp500.csv') # TODO replace w/ database query
    return ticker_data



@st.cache
def calculate_industries_AD(data, ticker_data):
    return calculate_industries_ads(data, ticker_data)
    

@st.cache
# def read_data():
#     data = read_all_tickers('data/tickers/') # TODO Replace w/ database query
#     data.date = data.date.apply(lambda x: datetime.date.fromisoformat(x))
#     return data


@st.cache 
def read_data2(source: str):
    data = query_database(source)
    data['date'] = data['date'].apply(lambda x: pd.to_datetime(x).date())
    return data

@st.cache 
def read_data3(source: str):
    data = query_database(source)
    return data

@st.cache
def update_gains(data, timeframe = 1):
    return check_gains(data, timeframe)


@st.cache
def update_data(data, williams_choice, ema_choice):
    return calculate_metrics(data,
        will_r_timeperiod=williams_choice,
        ema_timeperiod=ema_choice)

@st.cache
def get_AD(data):
    return calculate_AD(data)

#%%
data = read_data2('daily_stocks_data')
snp_data = read_data2('daily_index_data')
ticker_data = read_data3('ticker_data')
# snp_data['date'] = snp_data['date'].apply(lambda x: datetime.date.fromisoformat(x))


snp_AD = get_AD(data)
industry_ads = calculate_industries_AD(data, ticker_data)
intro = st.container()
intro.title("S&P500")
    
# Num tickers/timeframe 
dataset_info = st.container()
with dataset_info:
    st.markdown(f"The dataset contains daily financial data for **{len(data.symbol.unique())}** unique tickers.")
    st.markdown(f"The included tickers span from **{data['date'].min()}** to **{data['date'].max()}**")
    
    st.markdown('S&P500 index:')
    snp_date_lower, snp_date_upper = st.date_input(label = 'Select the date range of interest.', value = (snp_data['date'].max() - datetime.timedelta(days = 180), snp_data['date'].max()), min_value =  snp_data['date'].min(), max_value = snp_data['date'].max())
    
    date_mask = (snp_data['date'] > snp_date_lower) & (snp_data['date'] <= snp_date_upper)

    x = snp_data.loc[date_mask]['date']
    y = snp_data.loc[date_mask].close
    industry_ads = industry_ads.loc[(industry_ads.index < snp_date_upper) & (industry_ads.index > snp_date_lower)]
    p = figure(title = 'S&P Performance',
        x_axis_label = 'date',
        y_axis_label = 'close',
        x_axis_type ='datetime',
        
        plot_width = 800,
        plot_height = 200,
        tools = 'wheel_zoom, pan, reset')
    
    p.line(x, y,  line_width=2)
    p.toolbar.active_scroll = "auto"
    
    st.bokeh_chart(p, use_container_width=True)

    AD_plus_EMA = calculate_AD_EMA(snp_AD)
    x = AD_plus_EMA.loc[date_mask]['date']
    y = AD_plus_EMA.loc[date_mask]['s&p500']
    p = figure(title = 'A/D line',
        x_axis_label = 'date',
        y_axis_label = 'A/D value',
        x_axis_type ='datetime',
        
        plot_width = 800,
        plot_height = 200,
        tools = 'wheel_zoom, pan, reset')
    
    p.line(x, y,  line_width=1, color = 'coral')

    y = AD_plus_EMA.loc[date_mask]['EMA_3']
    p.line(x, y,  line_width=2, color = 'green', alpha = 1, legend_label = 'EMA(3)')
    y = AD_plus_EMA.loc[date_mask]['EMA_7']
    p.line(x, y,  line_width=2, color = 'cyan', alpha = 1, legend_label = 'EMA(7)',)
    y = AD_plus_EMA.loc[date_mask]['EMA_10']
    p.line(x, y,  line_width=2, color = 'purple', alpha = 1, legend_label = 'EMA(10)')
    p.legend.location = 'top_left'
    p.toolbar.active_scroll = "auto"
    st.bokeh_chart(p, use_container_width=True)
    st.caption(body = 'The A/D line can be interpreted as an indicator that shows the trend for a majority of stocks.')

    

    industries_selection = st.multiselect(label = 'Industries of interest: ', options = industry_ads.columns, default=industry_ads.columns.values[:5])
    p = figure(title = 'A/D by industry scaled',
        x_axis_label = 'date',
        y_axis_label = 'A/D value',
        x_axis_type ='datetime',
        plot_width = 800,
        plot_height = 250,
        tools = 'wheel_zoom, pan, reset')
    p.toolbar.active_scroll = "auto"
    x = industry_ads.index
    scaler = MinMaxScaler()
    color_idx = 0 
    color_range = np.linspace(0, 200, len(industries_selection), dtype=np.int64)
    colors = [Turbo256[idx] for idx in color_range]
    
    for column in industry_ads.columns:
        if column in industries_selection:
            y = industry_ads[[column]]
            y = scaler.fit_transform(y)
            if column == 's&p500':
                p.line(x, np.squeeze(y), line_width=2, alpha = 1, color = 'black', legend_label = 's&p500', line_dash = 'dashed')
            else:
                p.line(x, np.squeeze(y), line_width=2, alpha = .50, color = colors[color_idx], legend_label = column)
        color_idx += 1
    
    p.legend.location = 'top_left'
   
    st.bokeh_chart(p, use_container_width=True)
    iads = industry_ads.drop(columns = 's&p500')
    # st.area_chart(iads, use_container_width = True)

    p = figure(title = 'A/D by industry stacked',
        x_axis_label = 'date',
        y_axis_label = 'A/D value',
        x_axis_type ='datetime',
        plot_width = 800,
        plot_height = 400,
        tools = 'wheel_zoom, pan, reset')
    
    p.varea_stack(stackers=industries_selection, x='date', color = colors, legend_label=industries_selection, source=iads)
    p.legend.location = 'top_left'
    p.legend.orientation = 'horizontal'
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
    gdf = gains_df.loc[gains_df['date'] == gains_df['date'].max()].loc[:, ['symbol', 'close', 'Gains/Losses' ]].sort_values(by = 'Gains/Losses', ascending = False)


    top_decrease = gdf.iloc[-5:]
    top_increase = gdf.iloc[:5]
    loss_columns = st.columns(len(top_decrease))
    gains_columns = st.columns(len(top_increase))


    for i in reversed(range(len(top_increase))):
        loss_columns[i].metric(label = top_increase.iloc[i]['symbol'], value = round(top_increase.iloc[i]['close'],4), delta = str(round(top_increase.iloc[i]['Gains/Losses'], 3) * 100) + '%')

    for i in reversed(range(len(top_decrease))):
        gains_columns[i].metric(label = top_decrease.iloc[i]['symbol'], value = round(top_decrease.iloc[i]['close'], 4), delta = str(round(top_decrease.iloc[i]['Gains/Losses'], 3) * 100) + '%')

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
    
    ema_lower_thresh, ema_upper_thresh = col2.select_slider(label = 'EMA range', options = range(-100, 1, 1), value = (-100, -70))
   



# Filter
filtered = st.container()
with filtered:
    st.header('Tickers of interest')
    col1_t, col2_t = st.columns(2)
    with col1_t:
            date_selection = st.date_input(
            label = 'Consider Williams %R on this day',
            value = data['date'].max(),
            min_value = data['date'].min(),
            max_value = data['date'].max())
    date_filtered_data = updated_data.loc[updated_data['date'] == date_selection]

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




# Select filtered tickers
# Detailed exploration and plots

# Where ema < W%R
exploration = st.container()

with exploration:
    if thresh_filtered.symbol.unique().any():
        st.dataframe(thresh_filtered[['date', 'symbol', 'WillR', 'WillR_EMA', 'close']])

        exploration_choice = st.selectbox(label = "Inspect the following ticker:", options = thresh_filtered.symbol.unique(), index = 0)
        date_col1, date_col2 = st.columns(2)
        with date_col1:
            date_lower = st.date_input(
                label = 'From',
                value = day_mark_90,
                min_value = updated_data['date'].min(),
                max_value = updated_data['date'].max())
        with date_col2:
            date_upper = st.date_input(
                label = 'To',
                value = updated_data['date'].max(),
                min_value = updated_data['date'].min(),
                max_value = updated_data['date'].max())

        subset = updated_data.loc[(updated_data.symbol == exploration_choice) & (updated_data['date'] > date_lower) & ( updated_data['date'] <= date_upper)]
        
        close_p = figure(title = f"Closing price data for ${exploration_choice}",
            x_axis_label = 'date',
            y_axis_label = 'close',
            x_axis_type ='datetime',
            plot_width = 1000,
            plot_height = 200,
            tools = 'wheel_zoom, pan, reset')
        close_p.line(x = subset['date'], y = subset['close'], line_width = 1)

        will_ema_p = figure(title = f"Williams %R data for ${exploration_choice}",
            x_axis_label = 'date',
            y_axis_label = 'Williams %R',
            x_axis_type ='datetime',
            plot_width = 1000,
            plot_height = 200,
            tools = 'wheel_zoom, pan, reset')
        will_ema_p.line(x = subset['date'], y = subset['WillR'], line_width = 1, color = 'coral', legend_label = 'Williams %R')
        will_ema_p.line(x = subset['date'], y = subset['WillR_EMA'], line_width = 1, color = 'black', legend_label = f"EMA {ema_choice}", line_dash = 'dashed')
        will_ema_p.legend.location = 'top_left'
        st.bokeh_chart(close_p,  use_container_width = True)
        st.bokeh_chart(will_ema_p,  use_container_width = True)

        pattern_close = find_w_pattern(subset, column_of_interest='close')
        pattern_will = find_w_pattern(subset, column_of_interest='WillR')

        st.bokeh_chart(pattern_close, use_container_width = True)
        st.bokeh_chart(pattern_will, use_container_width = True)
    else:
        st.warning('No stock satisfies the range constraint of W%R or EMA...')


# %%
