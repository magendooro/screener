from operator import index
import streamlit as st
import pandas as pd
from datetime import date
from get_data import calculate_metrics
import seaborn as sns
import matplotlib.pyplot as plt

from metrics import read_all_tickers

plt.style.use('dark_background')

data = read_all_tickers('data/tickers/')
data.Date = data.Date.apply(lambda x: date.fromisoformat(x))

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
    st.text(f"The dataset contains daily financial data for {len(data.Symbol.unique())} unique tickers for the timeframe: {data.Date.min()} — {data.Date.max()}")
    st.text(f"Sample from the dataset:")
    st.write(data.head(3))


# User input and display tickers that satisfy the condition
parameter_selection = st.container()
with parameter_selection:
    st.header('**Select parameters**')
    williams_choice = st.slider(label = 'Williams R period', min_value=5, max_value = 100, value = 14)
    ema_choice = st.slider(label = 'EMA for Williams', min_value=3, max_value = 100, value = 21)
    st.header("Williams %R params")
    st.text("""
    Williams %R moves between 0 and -100. 
    Generally, a reading above -20 is overbought,
    and a reading below -80 is oversold. However, these can be picked on a use-case basis as well.
    """)
    updated_data = update_data(data, williams_choice, ema_choice)

    col1, col2 = st.columns(2)
    lower_thresh = col1.number_input(label = "Oversold threshold", value = -80, min_value = -100, max_value = 0)
    upper_thresh = col2.number_input(label = 'Overbought threshold', value = -20, min_value = -100, max_value = 0)

# Filter
filtered = st.container()
with filtered:
    selection_choices = ['below the oversold threshold', 'above the overbought threshold' ]
    st.header('Tickers of interest')
    filter_selection = st.selectbox(label = 'Only show tickers:', options=selection_choices)
    date_selection = st.date_input(
            label = 'Consider Williams %R on this day',
            value = data.Date.max(),
            min_value = data.Date.min(),
            max_value = data.Date.max())


date_filtered_data = updated_data.loc[updated_data.Date == date_selection]

# Filtering results
if 'below' in filter_selection:
    # Filter by date
    thresh_filtered = date_filtered_data.loc[date_filtered_data.WillR < lower_thresh]
    

else:
    # Same as above
    thresh_filtered = date_filtered_data.loc[date_filtered_data.WillR > upper_thresh]

st.write(thresh_filtered[['Date', 'Symbol', 'WillR', 'WillR_EMA', 'Close']])




# Select filtered tickers
# Detailed exploration and plots
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
    fig, ax = plt.subplots(figsize = (15, 5))
    subset = updated_data.loc[(updated_data.Symbol == exploration_choice) & (updated_data.Date > date_lower) & ( updated_data.Date < date_upper)]
    ax.plot(subset.Date, subset.WillR, label = 'W%R')
    ax.plot(subset.Date, subset.WillR_EMA, label = 'EMA' )
    ax.set_title('Williams %R and EMA of Williams %R')
    plt.legend()
    plt.xticks(rotation = 45)
    st.pyplot(fig)
        






