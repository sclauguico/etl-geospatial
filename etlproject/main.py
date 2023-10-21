import pandas as pd
from extract_data import extract_from_web, extract_lat_lng, extract_geo_json
from log import log_progress


### Define the wikiperdial url and the attributes to be extracted
wiki_url = 'https://en.wikipedia.org/wiki/List_of_Philippine_provinces_by_population'
attribute_list = ["Province", "Population"]    

# Scrape province and population data
extracted_wiki_data = extract_from_web(wiki_url, attribute_list) 

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
log_progress("Coordinates extraction complete. Initiating Transformation process") 
print(extracted_coordinate_data)


### Define the geo URL and the filename for the GeoJSON data
geo_url = "https://raw.githubusercontent.com/macoymejia/geojsonph/master/Province/Provinces.json"
filename = "geo_provinces.json"

# Extract the GeoJSON data
extract_geo_json(geo_url, filename)
