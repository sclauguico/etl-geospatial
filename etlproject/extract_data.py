import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

def extract(url, table_attributes):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attributes)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if (col != []):
            if len(col) > 1:
                province = col[0].text.rstrip()
                population = col[1].text
                df = df.append({"Province": province, "Population": population}, ignore_index=True)
    return df

url = 'https://en.wikipedia.org/wiki/List_of_Philippine_provinces_by_population'
attribute_list = ["Province"]    
extracted_data = extract(url, attribute_list) 
print("Data extraction complete. Initiating Transformation process") 
print(extracted_data) 