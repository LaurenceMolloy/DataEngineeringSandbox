import pandas as pd                    # a python module for data manipulation and analysis
import sqlite3                         # a python module for interacting with sqlite databases 
import matplotlib.pyplot as plt        # graph plotting module
import matplotlib.dates as mdates      # date manipulation routines for use with the graph plotting module
import numpy as np
import string
import re
from datetime import datetime

# script configuration settings
config = {
    # request url construction parameters
    'url_base': "https://api.coronavirus.data.gov.uk",
    'url_api_version': "v2",
    'url_area_type': "region",
    'url_metric': "newCasesBySpecimenDateAgeDemographics",
    'url_format': "csv",
    # database settings
    'db_filename': "example.db",
    'db_tablename': "cases_by_specdate",
    # timeframe for the analysis
    'from_dt': '2022-08-01',
    'to_dt': '2023-02-14'
}

# UK Covid Dashboard REST request endpoint url
endpoint = (f"{config['url_base']}/{config['url_api_version']}/data?"
            f"areaType={config['url_area_type']}&"
            f"metric={config['url_metric']}&"
            f"format={config['url_format']}")

# plot transformed data, stratified by by age range
def plot(fig, ax, dataframe, x, y, region, min=None, max=None, groupby=None, folded=False):
    if max is None: max = dataframe[y].max() 
    if min is None: min = dataframe[y].min() 
    ax = configure_plot(ax, min, max, region)
    plot_bg(dataframe[x], ax, min, max)
    if groupby == None:
        subplot(dataframe, ax, x, y)
    else:
        for group_name, group_df in dataframe.groupby(groupby):
            if folded is True: label = f"{group_name}-{int(group_name)+9}"
            else: label = group_name
            subplot(group_df, ax, x, y, label=re.sub(r"_", " - ", label))
        ax.legend(loc='upper left', bbox_to_anchor=(0.05, 0.99), title='Age Range', title_fontsize=8, prop={'size': 6})
    fig.autofmt_xdate()

def plot_bg(x_vals, ax, min, max):
    xs = pd.to_datetime(x_vals, format='%Y-%m-%d').reset_index(drop=True)
    max_array = np.full(len(x_vals), max)
    plus5_array = np.full(len(x_vals), 5)
    minus5_array = np.full(len(x_vals), -5)
    min_array = np.full(len(x_vals), min)
    ax.fill_between(xs, max_array, y2=plus5_array, interpolate=True, color='#FFE1E1', alpha=0.5)
    ax.fill_between(xs, plus5_array, interpolate=True, color='#FFFFE1', alpha=0.5)
    ax.fill_between(xs, min_array, interpolate=True, color='#E1FFE1', alpha=0.5)
    ax.fill_between(xs[-7:], min_array[-7:], y2=max_array[-7:], interpolate=True, color='#DDDDDD', alpha=0.5)
    ax.annotate('14 day doubling', xy=(mdates.date2num(xs[int(xs.size/2)]), 5), ha='center', va='bottom', alpha=0.5)
    ax.annotate('7 day doubling', xy=(mdates.date2num(xs[int(xs.size/2)]), 10), ha='center', va='bottom', alpha=0.5)
    ax.annotate('5 day doubling', xy=(mdates.date2num(xs[int(xs.size/2)]), 15), ha='center', va='bottom', alpha=0.5)
    ax.annotate('14 day halving', xy=(mdates.date2num(xs[int(xs.size/2)]), -5), ha='center', va='bottom', alpha=0.5)
    ax.annotate('7 day halving', xy=(mdates.date2num(xs[int(xs.size/2)]), -10), ha='center', va='bottom', alpha=0.5)

def subplot(dataframe, ax, x, y, label=None):
    dataframe[x] = pd.to_datetime(dataframe[x], format='%Y-%m-%d')
    dataframe = dataframe.sort_values(by=x, ascending=True)
    zeros = np.zeros(len(dataframe[x]))
    ax.plot(dataframe[x], dataframe[y], label=label)
    ax.plot(dataframe[x], zeros, color='Black', lw=0.5, alpha=0.75)

def configure_plot(ax, min, max, region):
    # set location of major and minor plot ticks
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1)) # major ticks every 1 month
    #ax.xaxis.set_major_locator(mdates.DayLocator(interval=7)) # minor ticks every 7 days
    ax.tick_params(axis='both', which='major', labelsize=8)
    #Chart title and axis labels
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('% Daily Growth', fontsize=12)
    ax.set_title(f'{region}', fontsize=12)
    ax.grid(axis='y', alpha=0.7)
    ax.set_ylim(min, max)
    # formatting the tick labels as '01/03/2020'
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d / %m / %y'))
    return ax

def db_create(db_file, db_table):
    db_connection = sqlite3.connect(db_file)
    cur = db_connection.cursor()
    # If it already exists, drop the table
    cur.execute(f"DROP TABLE IF EXISTS {db_table}")
    db_connection.commit()
    # Create a table for daily and 7 day moving average deaths, stratified by age range
    cur.execute(f'''   CREATE TABLE IF NOT EXISTS {db_table}
                       ( date            text, 
                         age             text, 
                         areaName       text,
                         cases          integer, 
                         cases_ma_7day  real    )   ''')
    db_connection.commit()
    db_connection.close()

def db_batch_insert(df, db_file, db_table):
    db_connection = sqlite3.connect(db_file)
    df.to_sql(db_table, db_connection, if_exists='append', index=False)
    db_connection.close()

def db_select(db_file, db_table):
    db_connection = sqlite3.connect(db_file)
    df = pd.read_sql_query(f"SELECT * FROM {db_table}", db_connection)
    db_connection.close()
    return df

def add_14_days(date_str):
    new_date = pd.to_datetime(date_str, format='%Y-%m-%d') + pd.Timedelta(days=14)
    return new_date.strftime('%Y-%m-%d')


### EXTRACT STEP 1: read 'raw' data from open data web source ###
df = pd.read_csv(endpoint)

# Map all under 20s to a single '00_19' age grouping
df['00_19'] = df['age'].isin(['00_04', '05_09', '10_14', '15_19'])
df.loc[df['00_19'] == True, 'age'] = '00_19'

# Map all 20-59 year olds to a single '20_59' age grouping
df['20_59'] = df['age'].isin(['20_24', '25_29', '30_34', '35_39','40_44', '45_49', '50_54', '55_59'])
df.loc[df['20_59'] == True, 'age'] = '20_59'

# drop all data points classified as 60+, as this is an aggregate grouping
df = df[(df['age'] == "60+") | (df['age'] == "20_59") | (df['age'] == "00_19")] # & (df['age'] != "unassigned")]

# (optional) filter by date
df = df[(df['date'] > config['from_dt']) & (df['date'] < config['to_dt'])]

# (optional) fold data into age ranges of 10yrs (e.g. 10-14 and 15-19 => 10-19)
# when uncommenting this code, also change to folded=True in plot() function below
#df = df.assign(age=lambda x: (x['age'].astype(str).str[0].astype(int) * 10).astype(str))

### TRANSFORM STEP 1: sum deaths over all regions ###
df = df.groupby(['age','areaName','date'], as_index=False)['cases'].sum().reset_index(drop=True)

### TRANSFORM STEP 2: daily rate of change (roc) ###
df['roc_cases_ma'] = (df['cases'].rolling(7).sum().pct_change(1)*100).reset_index(drop=True)

### TRANSFORM STEP 3: 7 day moving average of daily roc ###
df['cases_ma_7day'] = df['roc_cases_ma'].rolling(7,win_type='exponential').mean(tau=5).reset_index(drop=True)

### LOAD STEP: create a SQLite database and table, curate the dataframe's fields and insert data directly into the db ###
db_create(config['db_filename'], config['db_tablename'])

# one-liner - descriptive statistics for the data that I'm about to use
#print(df[(df['date'] > add_14_days(config['from_dt']))][['date', 'age', 'areaName', 'cases','cases_ma_7day']].describe()) 

min = df[(df['date'] > add_14_days(config['from_dt']))]['cases_ma_7day'].min()
max = df[(df['date'] > add_14_days(config['from_dt']))]['cases_ma_7day'].max()

# select relevant columns for SQLite batch insert and perform that insertion
df = df[['date', 'age', 'areaName', 'cases','cases_ma_7day']] 
db_batch_insert(df, config['db_filename'], config['db_tablename'])

### EXTRACT STEP 2: read 'transformed' data back from the SQLite db
df_sql = db_select(config['db_filename'], config['db_tablename'])

# from/to dates set as 'None' imply no upper/lower date limit respectively
def dt_filter(df, area=None, from_dt=None, to_dt=None):
    if from_dt is None: from_dt = add_14_days(df['date'].min())
    if to_dt is None: to_dt = df['date'].max()
    return (df['date']>=from_dt) & (df['date']<=to_dt) & (df['areaName']==area)

# define a layout for a plot grid using single-character ASCII art
# plots will be positioned, sized and semantically referenced using these letters
# NWest   Yorks&Humber  NEast
# WMids   EMids         East
# SWest   London        SEast
layout = """
    ABC
    DEF
    GHI
    """

# set up the figure and provide it with details of our defined layout
# IMPORTANT: subplot_mosaic() requires matplotlib V3.3. Have you upgraded yet?
# (See the cell near the top of this , notebook)
fig = plt.figure(figsize=(18,9), constrained_layout=False)
ax_dict = fig.subplot_mosaic(layout)

source_text = f'Data Source: UK Covid Dashboard\n{endpoint}'
plt.figtext(0.125, 0.08, source_text, ha='left', fontsize=8)

ts = datetime.utcnow()
ts_date = ts.strftime("%d-%m-%Y")
ts_time = ts.strftime("%H:%M:%S")
copyright_text  = f'DataViz design by Laurence Molloy (2022)\n'
copyright_text += f'generated on {ts_date} at {ts_time}'

plt.figtext(0.9, 0.08, copyright_text, ha='right', fontsize=8)

title  = (  "Daily Growth (%) for Trailing 7 Day +VE Covid Test Counts\n"
            "7 Day Exponential Moving Average "
            "(Tau=5), by Specimen Date, Stratified by Age Range"    )
fig.suptitle(title, fontsize=14, y=0.975)

positions = string.ascii_uppercase[0:9]
regions = [ 'North West',       'Yorkshire and The Humber', 'North East',
            'West Midlands',    'East Midlands',            'East of England',
            'South West',       'London',                   'South East']
plots = zip(positions, regions)

# Plot each region's data, stratified by age (position determined by letter)
for position, region in plots:
    plot(fig, ax_dict[position], 
        df_sql[dt_filter(df_sql, region)], 
        'date', 'cases_ma_7day', region, 
        min=min, max=max, groupby='age', folded=False)

plt.savefig('../docs/images/UCD_Growth_By_Region_+_Age.png')