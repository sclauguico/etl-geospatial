import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import geocoder

# Function for extracting the provinces and their population from the web
def extract_from_web(url, table_attributes):
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

# Function for extracting the coordinates of the provinces from an API
def extract_lat_lng(location):
    g = geocoder.arcgis('{}, Philippines'.format(location))
    return g.latlng
