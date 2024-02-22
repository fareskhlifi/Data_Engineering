import requests
import numpy as np 
import pandas as pd 
from bs4 import BeautifulSoup
from datetime import datetime 
import sqlite3


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
#table_attribs = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
table_attribs = ['Rank', 'Bank_name', 'MC_USD_Billion']
output_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = '/home/project/exchange_rate.csv'


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    page = requests.get(url).text

    data = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)

    tables = data.find_all('tbody')

    rows = tables[0].find_all('tr')


    for row in rows:
        tds = row.find_all('td')
        if not len(tds):
            continue
        Rank = tds[0].contents[0]
        bank_name = tds[1].contents[-2].contents[0]
        market = tds[-1].contents[0]
        
        data_dict = {
            'Rank': Rank[0:len(Rank)-1],
            'Bank_name': bank_name,
            'MC_USD_Billion': market[0:len(market)-1]
        }
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df, df1], ignore_index=True) 
    
    return df 


def transform(df, csv_path):
    df1 = pd.read_csv(csv_path)
    exchange_rate = df1.set_index('Currency')['Rate'].to_dict() 
    df['MC_GBP_Billion'] = [np.round(float(x)*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(float(x)*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(float(x)*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    
    return df 


def load_to_csv(df, output_path):
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)





log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_path)

log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, output_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL connection initiated')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as a table, Executing queries')

query_statement = f"SELECT * FROM Largest_banks"

run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"

run_query(query_statement, sql_connection)

query_statement = f"SELECT Bank_name from Largest_banks LIMIT 5"

run_query(query_statement, sql_connection)

log_progress('Process Complete')

sql_connection.close()