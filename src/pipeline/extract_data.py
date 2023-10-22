import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import geocoder
import os
import mysql.connector

# Function for extracting the provinces and their population from the web
def extract_provinces(url, table_attributes, file_path_name):
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
    
    df.to_csv(file_path_name, index=False)
    
    return df

# Function for extracting the coordinates of the provinces from an API/library
def extract_coordinates(provinces_list, file_path_name):
    coordinates = []

    for province in provinces_list:
        g = geocoder.arcgis('{}, Philippines'.format(province))
        latlng = g.latlng
        coordinates.append(latlng)

    df = pd.DataFrame(coordinates, columns=['Latitude', 'Longitude'])
    df.to_csv(file_path_name, index=False)

    return df


# Function for extracting the province geometry coordinates from an open-source repo / GitHub
def extract_geo(geo_url, file_path_name):
    try:
        response = requests.get(geo_url)
        if response.status_code == 200:
            with open(file_path_name, 'wb') as file:
                file.write(response.content)
            print(f'GeoJSON file "{file_path_name}" downloaded!')
        else:
            print(f'Failed to download GeoJSON. Status code: {response.status_code}')
    except Exception as e:
        print(f'An error occurred: {e}')


# Function for extracting the venues from the FOURSQUARE API
def extract_venues(coordinates, province_names, file_path_name_venues, file_path_name_venue_categories):
    
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


        df_venues = pd.DataFrame({ 
            'Name': names,
            'Latitude': latitudes,
            'Longitude': longitudes,
            'Location': locations
        })

        df_venue_categories = pd.DataFrame({ 
        'Category' : category_names,
        })

    df_venues.to_csv(file_path_name_venues, index=False)
    df_venue_categories.to_csv(file_path_name_venue_categories, index=False)
    return df_venues, df_venue_categories

# Function for extracting the tourism data from a database
def extract_tourism(file_path_name):
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

    df.to_csv(file_path_name, index=False)
    
    return df