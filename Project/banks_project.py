import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
import numpy as np
import requests
import datetime


def log_progress(message):
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"{timestamp} : {message}\n"
    with open("code_log.txt", "a") as log_file:
        log_file.write(log_message)

def extract(url, table_attribs):

    df = pd.DataFrame(columns = table_attribs)
    
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    tables = data.find_all('tbody')[0]
    rows = tables.find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            ancher_data = col[1].find_all('a')[1]
            if ancher_data is not None:
                data_dict = {
                    'Name' :ancher_data.contents[0],
                    'MC_USD_Billion': col[2].contents[0]
                }
                df1 = pd.DataFrame(data_dict, index = [0])
                df = pd.concat([df,df1],ignore_index = True)
    
    USD_ls = list(df['MC_USD_Billion'])
    USD_ls = [float(''.join(x.split('\n'))) for x in USD_ls]
    df['MC_USD_Billion'] = USD_ls
    

    return df

def transform(df, exchange_rate_path):

    exchange_csv = pd.read_csv(exchange_rate_path)
    dict = exchange_csv.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * dict['GBP'], 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * dict['EUR'], 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * dict['INR'], 2)
    print(df)


    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)
    

def run_query(query_statements, sql_connection):
    for query in query_statements:
        print(query)
        print(pd.read_sql(query, sql_connection), '\n')
    




''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate_path = 'exchange_rate.csv'
table_attribs = ["Name","MC_USD_Billion"]
output_path ='Largest_banks_data.csv'
conn_Bank_db = sqlite3.connect('Banks.db')
table_name = 'Largest_banks'
log_file = 'code_log.txt'
query_statements = [
        'SELECT * FROM Largest_banks',
        'SELECT AVG(MC_GBP_Billion) FROM Largest_banks',
        'SELECT Name from Largest_banks LIMIT 5'
    ]



log_progress("Preliminaries complete. Initiating ETL process")
df = extract(url, table_attribs)

log_progress("Data extraction complete. Initiating Transformation process")
df = transform(df, exchange_rate_path)

log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(df,output_path)

log_progress('Data saved to CSV file')
log_progress('SQL Connection initiated')
load_to_db(df, conn_Bank_db, table_name)

log_progress('Data loaded to Database as table. Running the query.')
run_query(query_statements, conn_Bank_db)
conn_Bank_db.close()
log_progress('Process Complete.')