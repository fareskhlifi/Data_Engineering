import requests
from bs4 import BeautifulSoup
import pandas as pd 
import sqlite3
import numpy as np 
from datetime import datetime


# Code for ETL operations on Country-GDP data

# Importing the required libraries
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'




def extract(url, table_attribs):
    page = requests.get(url).text 
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)

    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    count = 0

    for row in rows:
        if count > 2:
            tds = row.find_all('td')
            if tds[0].contents[-1] is not None and '—' not in tds[2]:
                data_dict = {
                    table_attribs[0]: tds[0].contents[-1].contents[0],
                    table_attribs[1]: tds[2].contents[0]  
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)       
                count += 1
        else:
            count += 1
    return df

def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float(''.join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x*10**(-3), 2) for x in GDP_list]
    df["GDP_USD_billions"] = GDP_list
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

log_progress('ETL has begun!')
df = extract(url, table_attribs)
log_progress('extract is done!')
df = transform(df)
log_progress('transform is done!')

load_to_csv(df, csv_path)
log_progress('data saved to csv file!')

sql_connection = sqlite3.connect(db_name)
log_progress('sql connection initialized!')

load_to_db(df, sql_connection, table_name)
log_progress('data loaded to db!')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Congrats, ETL pipeline completed!')
sql_connection.close()