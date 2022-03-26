from operator import index
import streamlit as st
import pandas as pd
from datetime import date
from metrics import calculate_metrics, read_all_tickers
import seaborn as sns
import matplotlib.pyplot as plt
from pandas.core.common import SettingWithCopyWarning
from metrics import read_all_tickers
from metrics import check_gains
import warnings

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

plt.style.use('dark_background')

data = read_all_tickers('data/tickers/')
data.Date = data.Date.apply(lambda x: date.fromisoformat(x))

@st.cache
def update_gains(data, timeframe = 1):
    return check_gains(data, timeframe)


@st.cache
def update_data(data, williams_choice, ema_choice):
    return calculate_metrics(data,
        will_r_timeperiod=williams_choice,
        ema_timeperiod=ema_choice)

intro = st.container()
intro.title("S&P500 metrics")
    
# Num tickers/timeframe 
dataset_info = st.container()
with dataset_info:
    st.markdown(f"The dataset contains daily financial data for **{len(data.Symbol.unique())}** unique tickers.")
    st.markdown(f"The included tickers span from **{data.Date.min()}** to **{data.Date.max()}**")
    st.text(f"Sample from the dataset:")
    st.write(data)

gains_section = st.container()
with gains_section:
    gains_df = update_gains(data)
    gdf = gains_df.loc[gains_df.Date == gains_df.Date.max()].loc[:, ['Symbol', 'Close', 'Gains/Losses', ]].sort_values(by = 'Gains/Losses', ascending = True)
    gdf.style.format(
        {'Gains/Losses': '{:,.2%}'})
    
    
    top_decrease = gdf.iloc[:5]
    top_increase = gdf.iloc[-5:]
    loss_columns = st.columns(len(top_decrease))
    st.write(len(loss_columns))
    for i in range(len(top_increase)):
        loss_columns[i].metric(label = top_increase.iloc[i]['Symbol'], value = top_increase.iloc[i]['Close'], delta = top_increase.iloc[i]['Gains/Losses'])


# User input and display tickers that satisfy the condition
parameter_selection = st.container()
with parameter_selection:
    st.header('**Select parameters**')
    williams_choice = st.slider(label = 'Williams R period', min_value=5, max_value = 100, value = 21)
    ema_choice = st.slider(label = 'EMA for Williams', min_value=3, max_value = 100, value = 13)
    st.header("Ranges of interest")
    st.text("""
    Williams %R moves between 0 and -100. 
    Generally, a reading above -20 is overbought,
    and a reading below -80 is oversold. However, these can be picked on a use-case basis as well.
    """)
    filter_indep = st.checkbox(label = 'Filter independently')
    st.write(filter_indep)
    updated_data = update_data(data, williams_choice, ema_choice)
    col1, col2 = st.columns(2)
    # col1.subheader('Williams %R')
    # col2.subheader('EMA')
    
    will_lower_thresh = col1.number_input(label = "Williams %R lower bound", value = -80, min_value = -100, max_value = 0)
    will_upper_thresh = col1.number_input(label = "Williams %R upper bound", value = -20, min_value = -100, max_value = 0)
    
    ema_lower_thresh = col2.number_input(label = "EMA lower bound", value = -80, min_value = -100, max_value = 0)
    ema_upper_thresh = col2.number_input(label = "EMA upper bound", value = -20, min_value = -100, max_value = 0)


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
st.write(thresh_filtered[['Date', 'Symbol', 'WillR', 'WillR_EMA', 'Close']])




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
            value = updated_data.Date.min(),
            min_value = updated_data.Date.min(),
            max_value = updated_data.Date.max())
    with date_col2:
        date_upper = st.date_input(
            label = 'To',
            value = updated_data.Date.max(),
            min_value = updated_data.Date.min(),
            max_value = updated_data.Date.max())

    major_ticks = pd.date_range(start = date_lower, end = date_upper, periods = 10)
    minor_ticks = pd.date_range(start = date_lower, end = date_upper, periods = 20)

    fig, axs = plt.subplots(nrows = 2, ncols = 1,figsize = (15, 5), sharex = True)
    subset = updated_data.loc[(updated_data.Symbol == exploration_choice) & (updated_data.Date > date_lower) & ( updated_data.Date < date_upper)]
    
    axs[0].plot(subset.Date, subset.WillR, label = 'W%R')
    axs[0].plot(subset.Date, subset.WillR_EMA, label = 'EMA')
    axs[0].set_title('Williams %R and EMA of Williams %R')
    # axs[0].grid(which='minor', alpha=0.2)
    axs[0].grid(which='major', alpha=0.8)
    axs[0].set_xticks(major_ticks)
    # axs[0].set_xticks(minor_ticks, minor=True)

    axs[1].plot(subset.Date, subset.Close, label = 'Close')
    axs[1].set_title('Closing price')
    # axs[1].grid(which='minor', alpha=0.2)
    axs[1].grid(which='major', alpha=0.8)
    axs[1].set_xticks(major_ticks)
    # axs[1].set_xticks(minor_ticks, minor=True)
    
    
    plt.legend()
    
    plt.xticks(rotation = 45)
    st.pyplot(fig)







