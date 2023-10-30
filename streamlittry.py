import streamlit as st
import plotly.express as px
import pandas as pd
import os 
import warnings
from datetime import datetime
import sqlite3
import numpy as np
warnings.filterwarnings('ignore')

st.set_page_config(page_title = "Jupyter Log Analysis", page_icon = ":bar_chart:" , layout = "wide")

st.title(" :bar_chart: Önlab 1: Monitoring Jupyter Logs")
st.markdown('<style>div.block-container{padding-top:1rem;}</style>',unsafe_allow_html = True)

#C:\Users\Administrator\.ipython\profile_default\history.sqlite'
location_of_local_db = r'history.sqlite'

#get the different sessions
sessions_conn = sqlite3.connect(location_of_local_db)
sessions_cursor = sessions_conn.cursor()
sessions_cursor.execute("SELECT * FROM sessions;")
data = sessions_cursor.fetchall()
sessions_df = pd.read_sql_query("SELECT * FROM sessions", sessions_conn)

##get the history of the logs
history_conn = sqlite3.connect(location_of_local_db)
history_cursor = history_conn.cursor()
history_cursor.execute("SELECT * FROM history;")
data = history_cursor.fetchall()
history_df = pd.read_sql_query("SELECT * FROM history", history_conn)

#Join the two df-s together
df = history_df.merge(sessions_df, on='session', how='inner')
df['source_length'] = df['source'].apply(len)
df['source_raw_length'] = df['source_raw'].apply(len)

# Convert session_start and session_end columns to datetime
df['start'] = pd.to_datetime(df['start']).dt.date
df['end'] = pd.to_datetime(df['end']).dt.date
df["Session_and_start_date"] = 'ID: ' + df['session'].astype(str) + ', Start Date: ' + df['start'].astype(str)

# Define the conditions and corresponding values for the new_column
conditions = [
    df['num_cmds'].isna(),
    df['num_cmds'] == 0,
    (df['num_cmds'] > 0) & (df['num_cmds'] <= 5),
    (df['num_cmds'] > 5) & (df['num_cmds'] <= 20),
    (df['num_cmds'] > 20) & (df['num_cmds'] <= 50),
    (df['num_cmds'] > 50) & (df['num_cmds'] <= 100),
    df['num_cmds'] > 100
]

values = ['NA', '0', '<= 5', '(5, 20]', '(20, 50]','(50, 100]', '> 50']
# Use numpy.select to create the new_column based on conditions
df['command_number_category'] = np.select(conditions, values, default='Unknown')

col1, col2 = st.columns((2))
##getting the min and max date of the start date 
startDate = df['start'].min()
endDate = df['start'].max()

with col1: 
    date1 = (st.date_input("Start Date", startDate))

with col2: 
    date2 = (st.date_input("End Date", endDate))

df = df[(df['start'] >= date1) & (df['start'] <= date2)].copy()

#Filter dow to specific sessions based on the ID
st.sidebar.header("Filter based o SessionID ans Start Date: ")
sessions_id = st.sidebar.multiselect("Pick your Session", df["Session_and_start_date"].unique())
if not sessions_id:
    df2 = df.copy()
else: 
    df2 = df[df["Session_and_start_date"].isin(sessions_id)]


#Filter dow to specific sessions based on the number of commands in the sessions
st.sidebar.header("CFilter based on the Number of Commands: ")
command_len = st.sidebar.multiselect("Filter down to Sessions based on the number of commands", df["command_number_category"].unique())
if not command_len:
    df3 = df2.copy()
else: 
    df3 = df2[df2["command_number_category"].isin(command_len)]

#Apply the specific filter combinations
if not sessions_id and not command_len:
    filtered_df = df
elif not sessions_id:
    filtered_df = df[df['command_number_category'].isin(command_len)]
elif not command_len:
    filtered_df = df[df['Session_and_start_date'].isin(sessions_id)]
elif sessions_id and command_len: 
    filtered_df = df3[df["command_number_category"].isin(command_len) & df3["Session_and_start_date"].isin(sessions_id)]


agg_df = filtered_df.groupby('session').agg({
    'line': 'count',
    'start': 'min',
    'end': 'max',
    'num_cmds': 'sum',
    'source_length': 'sum',
    'source_raw_length': 'sum'
}).reset_index()

# Rename columns 
agg_df = agg_df.rename(columns={
    'line': 'line_count',
    'start': 'session_start',
    'end': 'session_end',
    'num_cmds': 'total_num_cmds',
    'source_length': 'total_source_length',
    'source_raw_length': 'total_source_raw_length'
})

# Convert session_start and session_end columns to datetime
agg_df['session_start'] = pd.to_datetime(agg_df['session_start'])
agg_df['session_end'] = pd.to_datetime(agg_df['session_end'])

# Define the conditions and corresponding values for the line_count_category
conditions = [
    (agg_df['line_count'].isna()) | (agg_df['line_count'] == 0),
    (agg_df['line_count'] > 0) & (agg_df['line_count'] <= 5),
    (agg_df['line_count'] > 5) & (agg_df['line_count'] <= 20),
    (agg_df['line_count'] > 20) & (agg_df['line_count'] <= 50),
    (agg_df['line_count'] > 50) & (agg_df['line_count'] <= 100),
    agg_df['line_count'] > 100
]

values = ['0', '<= 5', '(5, 20]', '(20, 50]','(50, 100]', '> 50']
# Use numpy.select to create the new_column based on conditions
agg_df['line_count_category'] = np.select(conditions, values, default='Unknown')

conditions = [
    (agg_df['total_source_length'].isna()) | (agg_df['total_source_length'] == 0),
    (agg_df['total_source_length'] > 0) & (agg_df['total_source_length'] <= 200),
    (agg_df['total_source_length'] > 200) & (agg_df['total_source_length'] <= 800),
    (agg_df['total_source_length'] > 800) & (agg_df['total_source_length'] <= 2000),
    (agg_df['total_source_length'] > 2000) & (agg_df['total_source_length'] <= 5000),
    agg_df['total_source_length'] > 5000
]

values = ['0', '<= 200', '(200, 800]', '(800, 2000]','(2000, 5000]', '> 5000']
# Use numpy.select to create the new_column based on conditions
agg_df['char_sum_category'] = np.select(conditions, values, default='Unknown')

#Group by the same way, but with the previosly created category for count of lines
piechartdf2 = agg_df.groupby('line_count_category').agg({'line_count': 'count'}).reset_index()
#Group by the same way, but with the previosly created category for count of lines
charsumdf = agg_df.groupby('char_sum_category').agg({'total_source_length': 'count'}).reset_index()

piechartdf = filtered_df.groupby('command_number_category').agg({   'num_cmds': 'sum'}).reset_index()
piechartdf = piechartdf.rename(columns={'num_cmds': 'total_num_cmds'})

##get the latest 10 sessions in a seperate df
fresh_df = filtered_df.sort_values(by='start', ascending=False)
fresh_df = filtered_df.head(20).copy()

#Create variables for overview
startDate = str(agg_df['session_start'].min())[:10]
endDate = str(agg_df['session_end'].min())[:10]
uniqueSessions = str(agg_df['line_count'].count())
commandNumber = int(agg_df['total_num_cmds'].sum())
lineCount = agg_df['line_count'].sum()
charSum = agg_df['total_source_length'].sum()

# Overview on top of the page
st.markdown('### Overview')
col1, col2,col3, col4, col5, col6 = st.columns(6)
# Define inline styles for metrics
metric_style = f"""
    background-color: rgb(239, 138, 96);
    border-radius: 10px; /* Adds rounded corners */
    text-align: center;
    vertical-align: middle;
    font-weight: bold;
    padding: 10px;
"""

# Create each metric element with the specified inline styles
import streamlit as st

# Create variables for overview
startDate = str(agg_df['session_start'].min())[:10]
endDate = str(agg_df['session_end'].min())[:10]
uniqueSessions = str(agg_df['line_count'].count())
commandNumber = int(agg_df['total_num_cmds'].sum())
lineCount = agg_df['line_count'].sum()
charSum = agg_df['total_source_length'].sum()

# Create each metric element with styles
with col1:
    st.markdown(f'<div style="{metric_style}"><span style="font-weight: bold; ">Session Start (min)</span><br>{startDate}</div>', unsafe_allow_html=True)

with col2:
    st.markdown(f'<div style="{metric_style}"><span style="font-weight: bold;">Session End (max)</span><br>{endDate}</div>', unsafe_allow_html=True)

with col3:
    st.markdown(f'<div style="{metric_style}"><span style="font-weight: bold;">Number of Sessions</span><br>{uniqueSessions}</div>', unsafe_allow_html=True)

with col4:
    st.markdown(f'<div style="{metric_style}"><span style="font-weight: bold;">Number of Commands</span><br>{commandNumber}</div>', unsafe_allow_html=True)

with col5:
    st.markdown(f'<div style="{metric_style}"><span style="font-weight: bold;">Count of Lines</span><br>{lineCount}</div>', unsafe_allow_html=True)

with col6:
    st.markdown(f'<div style="{metric_style}"><span style="font-weight: bold;">Number of characters</span><br>{charSum}</div>', unsafe_allow_html=True)


#Show data based on filters 
st.subheader("View logs based on currently applied filters:")
with st.expander("View the Data based on current filters:"):
    st.write(filtered_df.style.background_gradient(cmap="Oranges"))
    csv = filtered_df.to_csv(index= False, sep = '|').encode('utf-8')
    st.download_button("Download Data", file_name= "Jupyter_logs.csv",  data = csv,mime = "text/csv", help = 'Click here to download data as a Csv file')

with st.expander("View the 20 latest activity based on current filters:"):
    st.write(fresh_df.style.background_gradient(cmap="Oranges"))
    csv = fresh_df.to_csv(index= False, sep = '|').encode('utf-8')
    st.download_button("Download Data", file_name= "New_Jupyter_logs.csv",  data = csv,mime = "text/csv", help = 'Click here to download data as a Csv file')

with st.expander("View the aggregated data for each sessions:"):
    st.write(agg_df.style.background_gradient(cmap="Oranges"))
    csv = agg_df.to_csv(index= False, sep = '|').encode('utf-8')
    st.download_button("Download Data", file_name= "Aggregated_logs.csv",  data = csv,mime = "text/csv", help = 'Click here to download data as a Csv file')

colbar1, colbar2 = st.columns((2))

with colbar1:
    st.subheader("Sessions by the count of code lines")
    #Only take into consideration the sessions where the line number is not null 
    agg_df_line_count = agg_df[pd.notna(agg_df['line_count']) & (agg_df['line_count'] > 0)]
    agg_df_line_count = agg_df_line_count.sort_values(by='session_start', ascending=True)
    # Create a Plotly figure for each visualization
    fig1 = px.bar(
        agg_df_line_count,
        y='line_count',
        x='session',
        #orientation='h',
        text='line_count',  # Tooltip for line_count
        custom_data=['session_start', 'session_end'],  # Tooltip for session_start and session_end
        title=f"Sessions by Line Count",
        labels={'line_count': 'Line Count'},
        color='line_count',  # Use color scale based on line_count
        color_continuous_scale='matter',  # Choose a color scale
    )
    all_session_ids_fig1 = agg_df_line_count['session'].unique()
    fig1.update_xaxes(type='category', categoryorder='array', categoryarray=all_session_ids_fig1)
    fig1.update_layout(xaxis_title="Session", yaxis_title="Line Count")
    fig1.update_traces( hovertemplate='<b>Session Start:</b> %{customdata[0]}<br><b>Session End:</b> %{customdata[1]}<br><b>Value:</b> %{text}')
    st.plotly_chart(fig1, use_container_width= True)

with colbar2:
    st.subheader("Number Of Commands per Session")
    aggdf_total_num_cmds = agg_df[pd.notna(agg_df['total_num_cmds']) & (agg_df['total_num_cmds'] > 0)]
    aggdf_total_num_cmds = aggdf_total_num_cmds.sort_values(by='session_start', ascending=True)
    fig2 = px.bar(
        aggdf_total_num_cmds,
        y='total_num_cmds',
        x='session',
        text='total_num_cmds',  # Tooltip for total_num_cmds
        custom_data=['session_start', 'session_end'],  # Tooltip for session_start and session_end
        title=f"Total Number Of Commands per Session",
        labels={'total_num_cmds': 'Total num. of commands'},
        color='total_num_cmds',  # Use color scale based on total_num_cmds
        color_continuous_scale='matter',  # Choose a color scale
    )
    all_session_ids_fig2 = aggdf_total_num_cmds['session'].unique()
    fig2.update_xaxes(type='category', categoryorder='array', categoryarray=all_session_ids_fig2)
    fig2.update_layout(xaxis_title="Session", yaxis_title="Total number of commands")
    fig2.update_traces( hovertemplate='<b>Session Start:</b> %{customdata[0]}<br><b>Session End:</b> %{customdata[1]}<br><b>Value:</b> %{text}')
    st.plotly_chart(fig2, use_container_width= True)


st.subheader("Pie Charts based on Number of Commands and Count of Lines")
piecol1, piecol2 = st.columns((2))

with piecol1:
# Create a pie chart with the same color scheme
    fig_pie = px.pie(piechartdf, names='command_number_category', values='total_num_cmds', title="Sessions by Total Number of Commands")
    fig_pie.update_traces(marker=dict(colors=px.colors.qualitative.Dark2))
    fig_pie.update_traces(textinfo='percent+label')
    st.plotly_chart(fig_pie, use_container_width=True)

with piecol2: 
    fig_pie2 = px.pie(piechartdf2, names='line_count_category', values='line_count', title="Sessions by Count of Lines")
    fig_pie2.update_traces(marker=dict(colors=px.colors.qualitative.Dark2))
    fig_pie2.update_traces(textinfo='percent+label')
    st.plotly_chart(fig_pie2, use_container_width=True)

#Show data for the piecharts
datacol1, datacol2, datacol3 = st.columns((3))
with datacol1: 
    with st.expander("View aggregated data for Number of Commands:"):
        st.write(piechartdf.style.background_gradient(cmap="Oranges"))
        csv = piechartdf.to_csv(index= False, sep = '|').encode('utf-8')
        st.download_button("Download Data", file_name= "Count_of_lines.csv", data = csv, mime = "text/csv", help = 'Click here to download data as a Csv file')

with datacol2: 
    with st.expander("View aggregated data for Count Of Lines:"):
        st.write(piechartdf2.style.background_gradient(cmap="Oranges"))
        csv = piechartdf2.to_csv(index= False, sep = '|').encode('utf-8')
        st.download_button("Download Data", file_name= "Number_of_commands.csv",  data = csv,mime = "text/csv", help = 'Click here to download data as a Csv file')

with datacol3: 
    with st.expander("View aggregated data for the Sum of Chars in Code:"):
        st.write(charsumdf.style.background_gradient(cmap="Oranges"))
        csv = charsumdf.to_csv(index= False, sep = '|').encode('utf-8')
        st.download_button("Download Data", file_name= "Sum_of_chars_in_code.csv",  data = csv,mime = "text/csv", help = 'Click here to download data as a Csv file')

col3, col4 = st.columns((2))

with col3:
    st.subheader("Duration of different Sessions (which have ended)")
    # Convert session_start and session_end columns to datetime
    agg_df['session_start'] = pd.to_datetime(agg_df['session_start'])
    agg_df['session_end'] = pd.to_datetime(agg_df['session_end'])
    # Calculate session_duration in hours
    agg_df['session_duration'] = round((agg_df['session_end'] - agg_df['session_start']).dt.total_seconds() / 3600,4)  # Hours

    agg_df_session_duration_notna = agg_df[pd.notna(agg_df['session_end'])]
    agg_df_session_duration_notna = agg_df_session_duration_notna.sort_values(by='session_start', ascending=True)
    fig4 = px.bar(
        agg_df_session_duration_notna,
        y='session_duration',
        x='session',
        text='session_duration',  # Tooltip for session_duration
        custom_data=['session_start', 'session_end'],  # Tooltip for session_start and session_end
        title=f"Session Durations (in hours)",
        labels={'session_duration': 'Session Duration (hours)'},
        color='session_duration',  # Use color scale based on session_duration
        color_continuous_scale='matter',  # Choose a color scale
    )
    all_session_ids_fig4 = agg_df_session_duration_notna['session'].unique()
    fig4.update_xaxes(type='category', categoryorder='array', categoryarray=all_session_ids_fig4)
    fig4.update_layout(xaxis_title="Session", yaxis_title="Session duration (hours)")
    fig4.update_traces( hovertemplate='<b>Session Start:</b> %{customdata[0]}<br><b>Session End:</b> %{customdata[1]}<br><b>Value:</b> %{text}')
    st.plotly_chart(fig4, use_container_width= True)

with col4:
    st.subheader("Duration of currently Open Sessions")
    #Only work with sessions which are currently open
    agg_df_session_duration_na = agg_df[pd.isna(agg_df['session_end'])]
    agg_df_session_duration_na = agg_df_session_duration_na.sort_values(by='session_start', ascending=True)
    current_time = datetime.now()
    # Calculate session_duration in hours
    agg_df_session_duration_na['session_duration'] = round((current_time - agg_df_session_duration_na['session_start']).dt.total_seconds() / 3600,2)  # Hours

    fig5 = px.bar(
        agg_df_session_duration_na,
        y='session_duration',
        x='session',
        text='session_duration',  # Tooltip for session_duration
        custom_data=['session_start', 'session_end'],  # Tooltip for session_start and session_end
        title=f"Sessions Currently Open duration (in hours)",
        labels={'session_duration': 'Session Duration (hours)'},
        color='session_duration',  # Use color scale based on session_duration
        color_continuous_scale='matter',  # Choose a color scale
    )
    all_session_ids_fig5 = agg_df_session_duration_na['session'].unique()
    fig5.update_xaxes(type='category', categoryorder='array', categoryarray=all_session_ids_fig5)
    fig5.update_layout(xaxis_title="Session", yaxis_title="Open Session Duration (hours)")
    fig5.update_traces( hovertemplate='<b>Session Start:</b> %{customdata[0]}<br><b>Session End:</b> %{customdata[1]}<br><b>Value:</b> %{text}')
    st.plotly_chart(fig5, use_container_width= True)


    # Create a treemap chart
figtree = px.treemap(
    aggdf_total_num_cmds,
    path=['session'],
    values='total_num_cmds',  # Size of the treemaps
    color='line_count',       # Color based on line count
    hover_data=['session', 'session_start', 'session_end', 'total_num_cmds'],  # Tooltip data
    title='Treemap Chart of Sessions',
    labels={'total_num_cmds': 'Total Num Commands', 'line_count': 'Line Count'}, 
    color_continuous_scale='matter',  # Choose a color scale
)

# Customize the tooltip format
st.subheader("Analyzing sessions which have ended: Based on: -Box Size: Number of Commands, -Color: Number of Lines")
figtree.update_traces(
    hovertemplate='<b>Session:</b> %{customdata[0]}<br><b>Start Date:</b> %{customdata[1]}<br><b>End Date:</b> %{customdata[2]}<br>'
                  '<b>Total Num Commands:</b> %{customdata[3]}<br><b>Line Count:</b> %{color}',
)
st.plotly_chart(figtree, use_container_width= True)

# Create a calendar heatmap
st.subheader("Calendar heatmap of Sessions. Color based on Total number of Commands")
figcalendar = px.scatter(
    agg_df,
    x='session_start',
    y='session_end',
    color='total_num_cmds',
    color_continuous_scale='viridis',
    title='Calendar Heatmap of Sessions',
    labels={'total_num_cmds': 'Total Num Commands'},
    size_max=10,# Adjust the size of the markers
    range_x=[agg_df['session_start'].min() - pd.DateOffset(days=1), agg_df['session_end'].max() + pd.DateOffset(days=1)],
    range_y=[agg_df['session_start'].min() - pd.DateOffset(days=1), agg_df['session_end'].max() + pd.DateOffset(days=1)]
)

figcalendar.update_xaxes(
    dtick='D1',  # Show a tick for every day
    showgrid=True
)

figcalendar.update_yaxes(
    dtick='D1',  # Show a tick for every day
    showgrid=True
)
st.plotly_chart(figcalendar, use_container_width= True)

    



