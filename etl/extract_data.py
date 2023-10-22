import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import geocoder
import os
import mysql.connector

# Function for extracting the provinces and their population from the web
def extract_provinces(url, table_attributes):
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

# Function for extracting the coordinates of the provinces from an API/library
def extract_lat_lng(location):
    g = geocoder.arcgis('{}, Philippines'.format(location))
    return g.latlng


# Function for extracting the province geometry coordinates from an open-source repo / GitHub
def extract_geojson(geo_url, filename):
    try:
        response = requests.get(geo_url)
        if response.status_code == 200:
            with open(filename, 'wb') as file:
                file.write(response.content)
            print(f'GeoJSON file "{filename}" downloaded!')
        else:
            print(f'Failed to download GeoJSON. Status code: {response.status_code}')
    except Exception as e:
        print(f'An error occurred: {e}')


# Function for extracting the venues from the FOURSQUARE API
def extract_venues(coordinates, province_names):
    
    category_names = []
    names = []
    latitudes = []
    longitudes = []
    locations = []

    for (lat, lng), name in zip(coordinates, province_names):
        url = "https://api.foursquare.com/v3/places/search"

        params = {
            "query": "venue",
            "ll": f"{lat},{lng}",
            # "open_now": "true",
            "sort": "DISTANCE",
        }

        headers = {
            "Accept": "application/json",
            "Authorization": os.environ.get("API_KEY"),
        }

        response = requests.get(url, params=params, headers=headers)

        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
                
        for place in data['results']:
            categories = place.get('categories')
            if categories:
                first_category = categories[0]
                category_name = first_category.get('name')
                category_names.append(category_name)

            names.append(place['name'])
            latitudes.append(place['geocodes']['main']['latitude'])
            longitudes.append(place['geocodes']['main']['longitude'])
            locations.append(place['location']['formatted_address'])


        df = pd.DataFrame({ 
            'Name': names,
            'Latitude': latitudes,
            'Longitude': longitudes,
            'Location': locations
        })

        df_category = pd.DataFrame({ 
        'Category' : category_names,
        })

    return df, df_category

# Function for extracting the tourism data from a database
def extract_tourism():
    # Establish a connection
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123",
        database="tourism"
    )

    # Create a cursor
    cursor = conn.cursor()

    # Execute a query
    cursor.execute("SELECT * FROM tourism")

    # Fetch all rows
    data = cursor.fetchall()

    df = pd.DataFrame(data, columns=['Location', "No. of Foreign Tourists"])

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return df