# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime 
from bs4 import BeautifulSoup

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 
''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if (col != []):
                name = col[1].text.rstrip()
                market_cap = col[2].text.strip()
                df = df.append({"Name": name, "MC_USD_Billion": market_cap}, ignore_index=True)
    return df

def extract_csv(exchange_rate_csv_path):
    df_conversion = pd.read_csv(exchange_rate_csv_path)

    return df_conversion

def transform(df, df_conversion):
    ''' This function converts the market cap from USD to EUR, GBP, and INDR, and rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    currencies = ['GBP', 'EUR', 'INR']
    for currency in currencies:
        conversion_rate = df_conversion.loc[df_conversion['Currency'] == currency, 'Rate'].values[0]
        new_column_name = f'MC_{currency}_Billion'
        df[new_column_name] = df['MC_USD_Billion'].apply(lambda x: round(float(x.replace('[^\d.]', '')) * conversion_rate, 2))  


    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

conn = sqlite3.connect('Banks.db')
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'append', index =False)

def run_queries(query_statement1, query_statement2, query_statement3, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement1)
    query_output1 = pd.read_sql(query_statement1, sql_connection)
    print(query_output1)

    print(query_statement2)
    query_output2 = pd.read_sql(query_statement2, sql_connection)
    print(query_output2)

    print(query_statement3)
    query_output3 = pd.read_sql(query_statement3, sql_connection)
    print(query_output3)





# Log the initialization of the ETL process 
log_file = "code_log.txt"
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
table_name = 'Largest_banks'
exchange_rate_csv_path = 'exchange_rate.csv'
attribute_list = ['Name', "MC_USD_Billion"]
csv_path = './Largest_banks_data.csv'

log_progress("Preliminaries complete. Initiating ETL process") 

# Log the beginning of the Extraction process 
extracted_data = extract(url, attribute_list) 
print("Data extraction complete. Initiating Transformation process") 
print(extracted_data) 

# Log the beginning of the Extraction process 
extracted_conversion_data = extract_csv(exchange_rate_csv_path) 
print(extracted_conversion_data) 

# Log the beginning of the Transformation process 
transformed_data = transform(extracted_data, extracted_conversion_data) 
print("Data transformation complete. Initiating Loading process") 
print(transformed_data) 

# Log the beginning of the load csv process 
loaded_csv_data = load_to_csv(transformed_data, csv_path)
log_progress("Data saved to CSV file") 

# Log SQL connection initiated
conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated") 

# Log the beginning of the load to database process 
loaded_db_data = load_to_db(transformed_data, conn, table_name) 
log_progress("Data loaded to Database as a table, Executing queries") 

# Log the beginning of the running of query process 
query_statement1 = f"SELECT * FROM {table_name}"

query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"

query_statement3 = f"SELECT * FROM {table_name} LIMIT 5"

read_query_data = run_queries(query_statement1, query_statement2, query_statement3, conn)
log_progress("Process Complete") 

conn.close()
log_progress("Server Connection closed") 
