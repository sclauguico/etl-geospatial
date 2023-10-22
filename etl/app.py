### Import libraries and packages
import pandas as pd
from extract_data import extract_provinces, extract_lat_lng, extract_geojson, extract_venues, extract_tourism
from log import log_progress


### Define the wikiperdial url and the attributes to be extracted
wiki_url = 'https://en.wikipedia.org/wiki/List_of_Philippine_provinces_by_population'
attribute_list = ["Province", "Population"]    

# Scrape province and population data
extracted_wiki_data = extract_provinces(wiki_url, attribute_list) 

# Log the progress and print extracted Wiki data
log_progress("Web scraping complete.") 
print(extracted_wiki_data) 


### Define the provinces that require the coordinates data
provinces_list = extracted_wiki_data["Province"].tolist()
coordinates = []

# Extract coordinates of provinces
for province in provinces_list:
    coordinates.append(extract_lat_lng(province))

# Convert list of coordinates to a DataFrame
extracted_coordinate_data = pd.DataFrame(coordinates, columns=['Latitude', 'Longitude'])

# Log the progress and print extracted coordinate data
log_progress("Coordinates extraction complete.") 
print(extracted_coordinate_data)


### Define the geo URL and the filename for the GeoJSON data
geo_url = "https://raw.githubusercontent.com/macoymejia/geojsonph/master/Province/Provinces.json"
filename = "geo_provinces.json"

# Extract the GeoJSON data
extracted_geojson_data = extract_geojson(geo_url, filename)

# Log the progress and print extracted GeoJson data
log_progress("GeoJSON extraction complete") 
print(extracted_geojson_data)


### Define the provinces and their coordinates to obtain the venues avaible
province_names = extracted_wiki_data['Province']
coordinates = [(lat, lng) for lat, lng in zip(extracted_coordinate_data['Latitude'], extracted_coordinate_data['Longitude'])]

# Extract the venues data
extracted_venues_data, extracted_venue_categories_data = extract_venues(coordinates, province_names)

# Log the progress and print extracted coordinate data
log_progress("Places and venue data extraction complete.") 
print(extracted_venues_data, extracted_venue_categories_data)


### Extract the tourism data
extracted_tourism_data = extract_tourism()

# Log the tourism data
log_progress("Tourismm data extraction complete. Initiating Transformation process...") 
print(extracted_tourism_data)