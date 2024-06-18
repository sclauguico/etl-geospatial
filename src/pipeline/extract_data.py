import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import geocoder
import os
import mysql.connector

# Function for extracting the provinces and their population from the web
def extract_provinces(url, file_path_name):    
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if (col != []):
            if len(col) > 1:
                province = col[0].text.rstrip()
                population = col[1].text
                    
    df_provinces = pd.concat([pd.DataFrame([{"Province": col[0].text.strip().replace('[a]', ''),
                               "Population": col[1].text}])
                for row in rows if (col := row.find_all('td')) and col != [] and len(col) > 1],
               ignore_index=True)
    
    df_provinces.to_csv(file_path_name, index=False)
    
    return df_provinces

# Function for extracting the coordinates of the provinces from an API/library
def extract_coordinates(provinces_list, file_path_name):
    coordinates = []

    for province in provinces_list:
        g = geocoder.arcgis('{}, Philippines'.format(province))
        latlng = g.latlng
        coordinates.append(latlng)

    df_coordinates = pd.DataFrame(coordinates, columns=['Latitude', 'Longitude'])
    df_coordinates.to_csv(file_path_name, index=False)

    return df_coordinates


# Function for extracting the venues from the FOURSQUARE API
def extract_venues(coordinates, province_names, file_path_name):
    
    # Create empty lists to store data
    province_data = []
    venue_data = []

    # Loop through the JSON responses
    for (lat, lng), province_name in zip(coordinates, province_names):
        url = "https://api.foursquare.com/v3/places/search"
        params = {
            "query": "venue",
            "ll": f"{lat},{lng}"
        }
        headers = {
            "Accept": "application/json",
            "Authorization": os.environ.get("API_KEY"),
        }
        
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        except requests.RequestException as e:
            print(f"Error in Foursquare API request: {e}")
            continue  # Skip to the next iteration

        data = response.json()
            
        # Extract venue information
        for venue in data.get('results', []):
            venue_info = {
                'Province': province_name,
                'Venue Name': venue['name'],
                'Venue Category': venue.get('categories', [])[0].get('name'),
                'Venue Lat': venue['geocodes']['main']['latitude'],
                'Venue Long': venue['geocodes']['main']['longitude'],
                'Address': venue['location']['formatted_address']
            }
            venue_data.append(venue_info)

    # Create DataFrame
    df_venues = pd.DataFrame(venue_data)
    
    df_venues.to_csv(file_path_name, index=False)

    return df_venues

# Function for extracting the tourism data from a database
def extract_tourists(file_path_name):
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

    df_tourists = pd.DataFrame(data, columns=["Location", "No. of Foreign Tourists"])

    # Close the cursor and connection
    cursor.close()
    conn.close()

    df_tourists.to_csv(file_path_name, index=False)
    
    return df_tourists

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
