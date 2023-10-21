import pandas as pd
from extract_data import extract_from_web, extract_lat_lng
from log import log_progress

# Scrape province and population data
url = 'https://en.wikipedia.org/wiki/List_of_Philippine_provinces_by_population'
attribute_list = ["Province", "Population"]    
extracted_data = extract_from_web(url, attribute_list) 
log_progress("Web scraping complete.") 
print(extracted_data) 

# Extract coordinates of provinces
provinces_list = extracted_data["Province"].tolist()
coordinates = []
for province in provinces_list:
    coordinates.append(extract_lat_lng(province))

extracted_coordinates = pd.DataFrame(coordinates, columns=['Latitude', 'Longitude'])

log_progress("Coordinates extraction complete. Initiating Transformation process") 
print(extracted_coordinates)