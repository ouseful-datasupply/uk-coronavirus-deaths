# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Covid-19 Daily Deaths - UK
#
# Via: https://www.england.nhs.uk/statistics/statistical-work-areas/covid-19-daily-deaths/
#
# At the moment, each time this script runs it downloads all the daily datafiles and builds the db from scratch. We need to optimise things so that only new daily files are parsed and added, incrementally, to the database.

# +
import sqlite_utils
# #!rm nhs_dailies.db
DB = sqlite_utils.Database("nhs_dailies.db")
processed = DB['processed']
# Start on a mechanism for only downloading things we haven't already grabbed
# Need a better way to handle query onto table if it doesn't exist yet
try:
    already_processed = pd.read_sql("SELECT * FROM processed", DB.conn)['reference'].to_list()
except:
    already_processed = []
    
already_processed  
# -

# Daily reports are published as an Excel spreadhseet linked from the following page:

# Reporting page
url = 'https://www.england.nhs.uk/statistics/statistical-work-areas/covid-19-daily-deaths/'

# Load the page:

# +
import requests

page = requests.get(url)
# -

# Get the HTML page data into a form we can scrape it:

# +
from bs4 import BeautifulSoup, SoupStrainer

soup = BeautifulSoup(page.text)
# -

# Get the relevant links to the daily spreadseets:

links = {}
for link in soup.find("article", {"class": "rich-text"}).find_all('a'):
    if link.text.startswith('COVID 19 daily announced deaths'):
        if link.text not in links:
            links[link.text] = link.get('href')
    elif link.text.startswith('COVID 19 total announced deaths') and link.text.endswith('weekly tables'):
        weekly_totals_link =  link.get('href')
    elif link.text.startswith('COVID 19 total announced deaths'):
        totals_link =  link.get('href')
links

import numpy as np
import pandas as pd

# Start to sketch out how we can parse the data out of one of the spreadsheets. The following has been arrivied though a little bit of iteration an previewing of the data:

# + tags=["active-ipynb"]
# sheets = pd.read_excel(links['COVID 19 daily announced deaths 9 April 2020'],
#                            sheet_name=None)
#
# # What sheets are available in the spreadsheet
# sheet_names = sheets.keys()
# sheet_names
# -

# The spreadsheet contains the following sheets:
#
# - `COVID19 daily deaths by region`
# - `COVID19 daily deaths by age`
# - `COVID19 daily deaths by trust`
#

# ## Cleaning the Sheets
#
# Clean the sheets to get the actual data:

# + tags=["active-ipynb"]
# sheet = 'COVID19 daily deaths by region'
# sheets[sheet].head(15)
# -

# We don't necessarily know how much metadata there is at the start of the sheet so we need to emply heuristics. If *NHS England Region* is used consistently as a column heading, we can use that as a crib:

# + tags=["active-ipynb"]
# rows, cols = np.where(sheets[sheet] == 'NHS England Region')
# rows, cols

# + tags=["active-ipynb"]
# colnames = sheets[sheet].iloc[12]
# sheets[sheet] = sheets[sheet].iloc[15:]
# sheets[sheet].columns = colnames
# sheets[sheet].dropna(axis=1, how='all', inplace=True)
# sheets[sheet].dropna(axis=0, how='all', inplace=True)
# #sheets[sheet].dropna(axis=0, subset=[sheets[sheet].columns[0]], inplace=True)
# sheets[sheet].head()
# -

# The ages data is structured differently, but we can perhaps use *Age Group* as a crib?

# + tags=["active-ipynb"]
# sheet = 'COVID19 daily deaths by age'
# sheets[sheet].head(25)
# -

# We can extract the published date to provide an additional metadata column:

# + tags=["active-ipynb"]
# rows, cols = np.where(sheets[sheet] == 'Published:')
# published_date = sheets[sheet].iat[rows[0], cols[0]+1]
# published_date
# -

# Try the crib:

# + tags=["active-ipynb"]
# rows, cols = np.where(sheets[sheet] == 'Age group')
# rows, cols
# -

# Does the same cleaning pattern work?

# + tags=["active-ipynb"]
# colnames = sheets[sheet].iloc[13]
# sheets[sheet] = sheets[sheet].iloc[16:]
# sheets[sheet].columns = colnames
# sheets[sheet].dropna(axis=0, how='all', inplace=True)
# sheets[sheet].dropna(axis=1, how='all', inplace=True)
# #sheets[sheet].dropna(axis=0, subset=[sheets[sheet].columns[0]], inplace=True)
# sheets[sheet].head()
# -

# Again, *NHS England Region* may be a handy crib in the following sheet:

# + tags=["active-ipynb"]
# sheet = 'COVID19 daily deaths by trust'
# sheets[sheet].head(15)
# -

# The same cleaning pattern we used before seems to work fine:

# + tags=["active-ipynb"]
# colnames = sheets[sheet].iloc[12]
# sheets[sheet] = sheets[sheet].iloc[15:]
# sheets[sheet].columns = colnames
# sheets[sheet].dropna(axis=1, how='all', inplace=True)
# sheets[sheet].dropna(axis=0, how='all', inplace=True)
# #sheets[sheet].dropna(axis=0, subset=[sheets[sheet].columns[0]], inplace=True)
# sheets[sheet].head()
# -

# Sheet names keep changing, so create a lookup of aliases that we can normalise into.
#
# Some of the `ignore` sheets should be treated as "ignore for now" - there is data we can scrape but it may not be in a form currently handled.

sheet_aliases = {
    'COVID19 daily deaths by age': 'deaths by age',
    'COVID19 daily deaths by region': 'deaths by region',
    'COVID19 daily deaths by trust': 'deaths by trust',
    # TO DO - could the totals as well as daily sheet move to this convention?
    'Tab1 Deaths by region': 'deaths by region', 
    'Tab2 Deaths - no pos test': 'ignore', 
    'Tab3 Deaths by age': 'deaths by age',
    'Tab4 Deaths by trust': 'deaths by trust',
    'Contents': 'ignore',
    'Fig1 Daily deaths': 'ignore',
    'COVID19 daily deaths chart': 'ignore',
    'Deaths by region- no pos test ':  'ignore',
    'Deaths by region-negative test ': 'ignore',
    'Deaths by region - no pos test ':  'ignore',
    'COVID19 total deaths chart': 'ignore',
    'COVID19 total deaths by trust': 'deaths by trust',
    'COVID19 total deaths by region': 'deaths by region',
    'COVID19 total deaths by age': 'deaths by age',
    'COVID19 all deaths by ethnicity': 'deaths by ethnicity',
    'COVID19 all deaths by gender': 'deaths by gender',
    'COVID19 all deaths by condition': 'ignore',
    'Tab1 Deaths by ethnicity': 'deaths by ethnicity',
    'Tab2 Deaths by gender': 'deaths by gender', 
    'Tab3 Deaths by condition': 'ignore',
    'Tab4 Deaths by cond (detail)': 'deaths by condition'
}


# The following tries to clean things automatically - we drop the national aggregate values:

# +
# Should work for:
#COVID19 total deaths by trust
#COVID19 total deaths by region
#COVID19 total deaths by age
#COVID19 all deaths by ethnicity
#COVID19 all deaths by gender

# Currently excludes:
#COVID19 total deaths chart
#Deaths by region - no pos test
#COVID19 all deaths by condition

def cleaner(sheets):
    for sheet in sheets:
        #if 'chart' in sheet or 'no pos' in sheet or 'condition' in sheet:
        #    continue
        if sheet not in sheet_aliases or sheet_aliases[sheet]=='ignore':
            continue
        rows, cols = np.where(sheets[sheet] == 'Published:')
        published_date = sheets[sheet].iat[rows[0], cols[0]+1]
        print('1',sheet_aliases[sheet])
        if 'age' in sheet or 'gender' in sheet_aliases[sheet]:
            rows, cols = np.where(sheets[sheet] == 'Age group')
            #print((rows, cols))
            _ix= rows[0]
        elif 'ethnicity' in sheet_aliases[sheet]:
            rows, cols = np.where(sheets[sheet] == 'Ethnic group')
            #print((rows, cols))
            _ix= rows[0]
        elif 'condition' in sheet_aliases[sheet]:
            rows, cols = np.where(sheets[sheet] == 'Date introduced')
            _ix= rows[0]
        else:
            rows, cols = np.where(sheets[sheet] == 'NHS England Region')
            #print((sheet, rows, cols))
            _ix= rows[0] #ix[sheet][0]

        colnames = sheets[sheet].iloc[_ix]
        sheets[sheet] = sheets[sheet].iloc[_ix+3:]
        sheets[sheet].columns = colnames
        sheets[sheet].dropna(axis=1, how='all', inplace=True)
        sheets[sheet].dropna(axis=0, how='all', inplace=True)
        sheets[sheet] = sheets[sheet].loc[:, sheets[sheet].columns.notnull()]
        #display(f'Checking: {sheet}')
        sheets[sheet]['Published'] = published_date
        sheets[sheet].reset_index(inplace=True, drop=True)
        
        # Drop lines after Notes
        rows, cols = np.where(sheets[sheet] == 'Date introduced')
        if rows:
            sheets[sheet].drop(sheets[sheet].index[rows[0]:], inplace=True)
         #sheets[sheet].dropna(axis=0, subset=[sheets[sheet].columns[0]], inplace=True)
    return sheets


# -

# Grab all the daily reports:

# +
data = {}

tabs = []
for link in links:
    if link in already_processed:
        continue
    try:
        print(link)
        sheets = pd.read_excel(links[link], sheet_name=None)
        for k in sheets.keys():
            if k not in tabs:
                tabs.append(k)
        sheets = cleaner(sheets)
        data[link] = sheets
        processed.insert({"reference": link})
    except:
        print("Broke with sheets:", sheets.keys())
        exit(-1)


# + tags=["active-ipynb"]
# tabs

# + tags=["active-ipynb"]
# data.keys()
# -

# Just as an aside, we can informally extract the publication date of a spreadheet from the associated link text on the original web page (trusting that the link does refer to the correctly linked document):

# +
from parse import parse
import dateparser

def getLinkDate(link):
    """Get date from link text."""
    _date = parse('COVID 19 daily announced deaths {date}', link)['date']
    return dateparser.parse(_date)


# + tags=["active-ipynb"]
# #Test the date extractor
# getLinkDate('COVID 19 daily announced deaths 15 April 2020')

# + tags=["active-ipynb"]
# data['COVID 19 daily announced deaths 9 April 2020'].keys()
# -

# Preview what sort of data we've got:

# + tags=["active-ipynb"]
# df = data['COVID 19 daily announced deaths 9 April 2020']['COVID19 daily deaths by trust']
# df
# -

# Preview a specific area, albeit with quite an informal search term:

# + tags=["active-ipynb"]
# df = data['COVID 19 daily announced deaths 4 April 2020']['COVID19 daily deaths by trust']
# df[df['Name'].str.contains('WIGHT')]
# -

# Grab the totals:

totals_xl = pd.read_excel(totals_link, sheet_name=None)
totals_xl.keys()

weekly_totals_xl =  pd.read_excel(weekly_totals_link, sheet_name=None)
weekly_totals_xl.keys()

# +
#totals_xl['Tab4 Deaths by cond (detail)']
# -

totals_xl = cleaner(totals_xl)
totals_xl.keys()

weekly_totals_xl = cleaner(weekly_totals_xl)
weekly_totals_xl.keys()

totals_xl

# + tags=["active-ipynb"]
# #dfs = totals_xl['COVID19 total deaths by trust']
# #dfs[dfs['Name'].str.contains('WIGHT')]
# -

# ## Adding NHS Daily Data to a Database
#
# The data is perhaps most easily managed in a long form. We could normalise the data properly across several tables, or for mow we can just grab perhaps slightly denormalised tables for the dates and separate tables for totals and result awaiting verification:

# + tags=["active-ipynb"]
# df_dailies = df.drop(columns=['Awaiting verification', 'Total'])
# tmp = df_dailies.melt(id_vars=['NHS England Region','Code','Name', 'Published'],
#                       var_name='Date',
#                       value_name='value')
# tmp.head()
# -

# Find the days lag between published and strike date:

# + tags=["active-ipynb"]
# tmp['Date'] = pd.to_datetime(tmp['Date'])
# tmp['lag'] = tmp['Published'] - tmp['Date']
# tmp.head()
# -

# Create a simple SQLite database:

# Add the daily data to the db:

# + tags=["active-ipynb"]
# #df_long.head()

# +
idx = {'trust': ['NHS England Region','Code','Name', 'Published'],
       'age': ['Age group', 'Published'],
       'region': ['NHS England Region', 'Published'] }

for daily in data.keys():
    #print(daily)
    #linkDate = getLinkDate(daily)
    # TO DO - get data from excluded sheets
    if daily in already_processed:
        continue
    for sheet in data[daily].keys():
        if sheet not in sheet_aliases or sheet_aliases[sheet]=='ignore':
            continue
        #print(sheet)
        table = parse('deaths by {table}', sheet_aliases[sheet])['table']
        #print(f'Using table {table}')
        df_dailies = data[daily][sheet].drop(columns=['Awaiting verification', 'Total'])
        #df_dailies['Link_date'] = linkDate
        idx_cols = idx[table]#+['Link_date']
        df_long = df_dailies.melt(id_vars=idx_cols,
                                  var_name='Date',
                                  value_name='value')
        df_long['Date'] = pd.to_datetime(df_long['Date'])
        if df_long['Published'].dtype == 'O':
            df_long['Published'] = df_long['Published'].apply(dateparser.parse)
        df_long['lag'] = (df_long['Published'] - df_long['Date']).dt.days

        _table = f'nhs_dailies_{table}'
        df_long.to_sql(_table, DB.conn, index=False, if_exists='append')
        
        cols = idx[table] + ['Awaiting verification', 'Total']
        data[daily][sheet][cols].to_sql(f'{_table}_summary',
                                        DB.conn, index=False, if_exists='append')
        
    processed.insert({"reference": daily})
# -

# Dummy query on `age` sheet:

# + tags=["active-ipynb"]
# pd.read_sql("SELECT * FROM nhs_dailies_age LIMIT 25", DB.conn)
# -

# Dummy query on `age_summary` sheet:

# + tags=["active-ipynb"]
# pd.read_sql("SELECT * FROM nhs_dailies_age_summary LIMIT 5", DB.conn)
# -

# Dummy query on `trust` sheet:

# + tags=["active-ipynb"]
# pd.read_sql("SELECT * FROM nhs_dailies_trust LIMIT 5", DB.conn)
# -

# Dummy query on `trust_summary` sheet:

# + tags=["active-ipynb"]
# pd.read_sql("SELECT * FROM nhs_dailies_trust_summary LIMIT 5", DB.conn)
# -

# Dummy query on `region` sheet:

# + tags=["active-ipynb"]
# pd.read_sql("SELECT * FROM nhs_dailies_region LIMIT 25", DB.conn)
# -

# Dummy query on `region_summary` sheet:

# + tags=["active-ipynb"]
# pd.read_sql("SELECT * FROM nhs_dailies_region_summary LIMIT 25", DB.conn)
# -

# ### Adding NHS Totals Data to Database

# + tags=["active-ipynb"]
# totals_xl.keys()
# -

for sheet in totals_xl.keys():
    if sheet not in sheet_aliases or sheet_aliases[sheet]=='ignore':
            continue
    table = parse('deaths by {table}', sheet_aliases[sheet])['table']
    _table = f'nhs_totals_{table}'
    if 'ethnicity' not in table and 'gender' not in table and 'condition' not in table:
        df_totals = totals_xl[sheet].drop(columns=['Awaiting verification', 'Total', 'Up to 01-Mar-20'])
        idx_cols = idx[table]
        df_long = df_totals.melt(id_vars=idx_cols,
                                  var_name='Date',
                                  value_name='value')
        df_long['Date'] = pd.to_datetime(df_long['Date'])
        if df_long['Published'].dtype == 'O':
            df_long['Published'] = df_long['Published'].apply(dateparser.parse)
        df_long['lag'] = (df_long['Published'] - df_long['Date']).dt.days

        df_long.to_sql(_table, DB.conn, index=False, if_exists='append')

        cols = idx_cols + ['Up to 01-Mar-20', 'Awaiting verification', 'Total']
        totals_xl[sheet][cols].to_sql(f'{_table}_summary',
                                        DB.conn, index=False, if_exists='append')
    else:
        totals_xl[sheet].to_sql(f'{_table}', DB.conn, index=False, if_exists='append')

for sheet in weekly_totals_xl.keys():
    if sheet not in sheet_aliases or sheet_aliases[sheet]=='ignore':
            continue
    table = parse('deaths by {table}', sheet_aliases[sheet])['table']
    _table = f'nhs_weekly_totals_{table}'
    if 'ethnicity' not in table and 'gender' not in table and 'condition' not in table:
        df_totals = weekly_totals_xl[sheet].drop(columns=['Awaiting verification', 'Total', 'Up to 01-Mar-20'])
        idx_cols = idx[table]
        df_long = df_totals.melt(id_vars=idx_cols,
                                  var_name='Date',
                                  value_name='value')
        df_long['Date'] = pd.to_datetime(df_long['Date'])
        if df_long['Published'].dtype == 'O':
            df_long['Published'] = df_long['Published'].apply(dateparser.parse)
        df_long['lag'] = (df_long['Published'] - df_long['Date']).dt.days

        df_long.to_sql(_table, DB.conn, index=False, if_exists='append')

        cols = idx_cols + ['Up to 01-Mar-20', 'Awaiting verification', 'Total']
        weekly_totals_xl[sheet][cols].to_sql(f'{_table}_summary',
                                        DB.conn, index=False, if_exists='append')
    else:
        weekly_totals_xl[sheet].to_sql(f'{_table}', DB.conn, index=False, if_exists='append')

# + tags=["active-ipynb"]
# DB.table_names()

# + tags=["active-ipynb"]
# #pd.read_sql("SELECT * FROM nhs_totals_region_summary LIMIT 25", DB.conn)
# -

# ## Basic Charts
#
# Let's try some basic charts. For example, 

# + tags=["active-ipynb"]
# zz = pd.read_sql("SELECT * FROM nhs_totals_region WHERE `NHS England Region`='London' and Date=DATETIME('2020-04-09')", DB.conn)
# zz
# -

# How long does it take for a particular hospital to report deaths (i.e. what's the lag distribution between the publication date and the strike date?)?
#
# The following chart sums the number of deaths reported relative to the delay in reporting them:

# + tags=["active-ipynb"]
# pd.read_sql("SELECT value, lag FROM nhs_totals_trust WHERE Name='WEST HERTFORDSHIRE HOSPITALS NHS TRUST'", DB.conn).groupby(['lag']).sum().plot(kind='bar')
# -

# ## Public Health England
#
# Data published by Public Health England:
#     
# - [Cases](https://coronavirus.data.gov.uk/downloads/csv/coronavirus-cases_latest.csv)
# - [Deaths](https://coronavirus.data.gov.uk/downloads/csv/coronavirus-deaths_latest.csv)

# +
#via https://stackoverflow.com/questions/61415090/python-pandas-handling-of-308-request
import requests
import io

def get_308_csv(url):
    datastr = requests.get(url, allow_redirects=True).text
    data_file = io.StringIO(datastr)
    _df = pd.read_csv(data_file)
    _df['Specimen date'] =  pd.to_datetime(_df['Specimen date'])
    return _df


# +
phe_cases_url = 'https://coronavirus.data.gov.uk/downloads/csv/coronavirus-cases_latest.csv'
phe_cases_df = get_308_csv(phe_cases_url)

_table = f'phe_cases'
phe_cases_df.to_sql(_table, DB.conn, index=False, if_exists='replace')
    
phe_cases_df.head()

# + tags=["active-ipynb"]
# pd.read_sql("SELECT * FROM phe_cases LIMIT 3", DB.conn)

# +
phe_deaths_url = 'https://coronavirus.data.gov.uk/downloads/csv/coronavirus-deaths_latest.csv'
phe_deaths_df = get_308_csv(phe_cases_url)

_table = f'phe_deaths'
phe_cases_df.to_sql(_table, DB.conn, index=False, if_exists='replace')

phe_deaths_df.head()

# + tags=["active-ipynb"]
# pd.read_sql("SELECT * FROM phe_deaths LIMIT 3", DB.conn)
# -

# ### NHS - A&E
#
# Monthly data:
# https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/ae-attendances-and-emergency-admissions-2019-20/
#
# Hospital Episode Statistics:
# https://digital.nhs.uk/data-and-information/publications/statistical/hospital-episode-statistics-for-admitted-patient-care-outpatient-and-accident-and-emergency-data

# ## ONS
#
# Death registrations, 2020: https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/causesofdeath/datasets/deathregistrationsandoccurrencesbylocalauthorityandhealthboard
#
# Weekly Death registrations (provisional):
# https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/datasets/weeklyprovisionalfiguresondeathsregisteredinenglandandwales
#

# ### Weekly deaths, ONS:

base='https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/datasets/weeklyprovisionalfiguresondeathsregisteredinenglandandwales'
page = requests.get(base, allow_redirects=True)
soup = BeautifulSoup(page.text, 'lxml')
links = {}
lahtable_link = ''
for link in soup.find_all('a'):
    if 'Download Deaths registered weekly' in link.text:
        lahtable_link = link.get('href')
        break
weeklytable_file = lahtable_link#.split('/')[-1]
weeklytable_file

ons_weekly_url = f'https://www.ons.gov.uk{weeklytable_file}'
print(ons_weekly_url)


# +
r = requests.get(ons_weekly_url, allow_redirects=True)

fn = ons_weekly_url.split('/')[-1]
 
with open(fn, 'wb') as f:
    f.write(r.content)

try:
    ons_sheets = pd.read_excel(fn, sheet_name=None)
except:
    with open(fn) as f:
        print(f.read())
# What sheets are available in the spreadsheet
ons_sheet_names = ons_sheets.keys()
ons_sheet_names
# -

ons_weekly_reg = ons_sheets['Covid-19 - Weekly registrations']
ons_weekly_reg.head()

ons_weekly_occ = ons_sheets['Covid-19 - Weekly occurrences']
ons_weekly_occ.head()


def ons_weeklies(ons_weekly, typ):
    ons_weekly_long = {}
    rows, cols = np.where(ons_weekly == 'Week ended')
    colnames = ons_weekly.iloc[rows[0]].tolist()
    colnames[1] = 'Age'

    rows, cols = np.where(ons_weekly == 'Deaths by age group')
    _rows, _ = np.where(ons_weekly == '90+')
    _ix = rows[0]

    tables = []


    #Get the first three tables - for Persons, Males and Females
    for r, c in zip(rows, cols):
        tables.append(ons_weekly.iloc[r-1, c].split()[0])

    for r, _r, t in zip(rows, _rows, tables):
        ons_weekly_long[t] = ons_weekly.iloc[r+1: _r+1]
        ons_weekly_long[t].columns = colnames
        ons_weekly_long[t].dropna(axis=1, how='all', inplace=True)
        dropper = [c for c in ons_weekly_long[t].columns if 'to date' in str(c)]
        dropper = dropper + [c for c in ons_weekly_long[t].columns if '1 to' in str(c)]
        if dropper:
            ons_weekly_long[t].drop(columns=dropper, inplace=True)
        ons_weekly_long[t] = ons_weekly_long[t].melt(id_vars=['Age'], var_name='Date', value_name='value')
        ons_weekly_long[t]['measure'] = typ
        display(ons_weekly_long[t])
        ons_weekly_long[t]['Date'] = pd.to_datetime(ons_weekly_long[t]['Date'])

    ons_weekly_long['Any'] = pd.DataFrame()
    for t in tables:
        ons_weekly_long[t]['Group'] = t
        ons_weekly_long['Any'] = pd.concat([ons_weekly_long['Any'], ons_weekly_long[t]])
    
    ons_weekly_long['Any'].reset_index(inplace=True, drop=True)
    
    return ons_weekly_long

ons_weekly_reg_long = ons_weeklies(ons_weekly_reg, 'Weekly registrations')
ons_weekly_reg_long['Females']

# + tags=["active-ipynb"]
# ons_weekly_reg_long['Any']
# -

ons_weekly_occ

ons_weekly_occ_long = ons_weeklies(ons_weekly_occ, 'Weekly occurrences')
ons_weekly_occ_long['Males']

ons_weekly_all = ons_sheets['Weekly figures 2020']
ons_weekly_all.head()

ons_weekly_all_long = ons_weeklies(ons_weekly_all, 'Weekly all mortality')
ons_weekly_all_long['Males']

# Add to database...

# +
_table = 'ons_deaths'

ons_weekly_occ_long['Any'].to_sql(_table, DB.conn, index=False, if_exists='append')
ons_weekly_reg_long['Any'].to_sql(_table, DB.conn, index=False, if_exists='append')
ons_weekly_all_long['Any'].to_sql(_table, DB.conn, index=False, if_exists='append')
# -

# ### ONS Death Registrations, 2020
#
# https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/causesofdeath/datasets/deathregistrationsandoccurrencesbylocalauthorityandhealthboard

base='https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/causesofdeath/datasets/deathregistrationsandoccurrencesbylocalauthorityandhealthboard'
page = requests.get(base, allow_redirects=True)
soup = BeautifulSoup(page.text, 'lxml')
links = {}
lahtable_link = ''
for link in soup.find_all('a'):
    if 'Download Death registrations and occurrences' in link.text:
        lahtable_link = link.get('href')
        break
lahtable_file = lahtable_link#.split('/')[-1]
lahtable_file

ons_death_reg_url = f'https://www.ons.gov.uk{lahtable_file}'
ons_death_reg_url

# +
r = requests.get(ons_death_reg_url, allow_redirects=True)

fn = ons_death_reg_url.split('/')[-1]
 
with open(fn, 'wb') as f:
    f.write(r.content)

ons_reg_sheets = pd.read_excel(fn, sheet_name=None)

# What sheets are available in the spreadsheet
ons_reg_sheet_names = ons_reg_sheets.keys()
ons_reg_sheet_names
# -

ons_death_reg = ons_reg_sheets['Registrations - All data']
ons_death_reg_metadata = ons_death_reg.iloc[0, 0]
ons_death_reg_metadata

# +
from parse import parse
import dateparser

upto = parse('Deaths (numbers) by local authority and cause of death, registered up to the {date}, England and Wales',
             ons_death_reg_metadata)['date']
upto = dateparser.parse(upto)

rows, cols = np.where(ons_death_reg == 'Area code')
colnames = ons_death_reg.iloc[rows[0]].tolist()
    
ons_death_reg = ons_death_reg.iloc[rows[0]+1:].reset_index(drop=True)
ons_death_reg.columns = colnames


ons_death_reg['Registered up to'] = upto
ons_death_reg
# -

ons_death_occ_metadata

# +
ons_death_occ = ons_reg_sheets['Occurrences - All data']
ons_death_occ_metadata = ons_death_occ.iloc[0, 0]
ons_death_occ_metadata

uptos = parse('Deaths (numbers) by local authority and cause of death, for deaths that occurred up to {date_occ} but were registered up to {date_reg}, England and Wales',
             ons_death_occ_metadata)

upto_occ = uptos['date_occ']
if '2020' not in upto_occ: upto_occ = f'{upto_occ} 2020'
    
upto_reg = uptos['date_reg']
if '2020' not in upto_occ: upto_occ = f'{upto_reg} 2020'

upto_occ = dateparser.parse(upto_occ)
upto_reg = dateparser.parse(upto_reg)

rows, cols = np.where(ons_death_occ == 'Area code')
colnames = ons_death_occ.iloc[rows[0]].tolist()
    
ons_death_occ = ons_death_occ.iloc[rows[0]+1:].reset_index(drop=True)
ons_death_occ.columns = colnames


ons_death_occ['Occurred up to'] = upto_occ
ons_death_occ['Registered up to'] = upto_reg
ons_death_occ

# +
_table = 'ons_deaths_reg'
ons_death_reg.to_sql(_table, DB.conn, index=False, if_exists='replace')

_table = 'ons_deaths_reg_occ'
ons_death_occ.to_sql(_table, DB.conn, index=False, if_exists='replace')
# -

# ## Deployment via datasette
#
# `datasette publish fly nhs_dailies.db --app="nhs-orgs"`

# ## Simple Chat

# + tags=["active-ipynb"]
# # It takes tiny amounts of code to post s/thing from a notebook to an API and display a result
#
#
# # Create some magic to call and API
# from IPython.core.magic import register_cell_magic, register_line_magic
# import requests
# import pandas as pd
#
# from urllib.parse import urlencode
#     
# _datasette_url = 'https://nhs-orgs.fly.dev/nhs_dailies/phe_cases.csv?{}'
#
# @register_line_magic
# def phe_cases(line):
#     "Query datasette."
#     payload = {'_sort': 'rowid',
#                'Area name__contains': line,
#                '_size': 'max'}
#     _url =  _datasette_url.format(urlencode(payload))
#     return pd.read_csv( _url)

# + tags=["active-ipynb"]
# # Pass a string to the API via some magic and display the result
#
# %phe_cases isle of wight
# -

# ## Run as file

# !python3 uk_daily_deaths_nhs.py

# ## Looking Inside Downloaded Zip Files

# + tags=["active-ipynb"]
# #https://techoverflow.net/2018/01/16/downloading-reading-a-zip-file-in-memory-using-python/
# import zipfile
#
# def download_extract_xml(url):
#     """
#     Download a ZIP file and extract its contents in memory
#     yields (filename, file-like object) pairs
#     """
#     response = requests.get(url)
#     with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
#         for zipinfo in thezip.infolist():
#             with thezip.open(zipinfo) as thefile:
#                 yield zipinfo.filename, thefile
#                 
# r = download_extract_xml(ons_weekly_url)
# for f in r:
#     print(f)
