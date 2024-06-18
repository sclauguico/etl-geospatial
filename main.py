import pandas as pd
import os
from src.pipeline.extract_data import extract_provinces, extract_coordinates, extract_geo, extract_venues, extract_tourists
from src.pipeline.transform_data import transform_provinces, transform_coordinates, transform_venues, transform_tourists
from src.logger import log_progress

def main():
    # Scrape province and population data
    extracted_prov_pop_data = extract_province_population()
    log_progress("Web scraping complete.")
    print(extracted_prov_pop_data)

    # Extract coordinates of provinces
    extracted_coordinates_data = extract_province_coordinates_data(extracted_prov_pop_data)
    log_progress("Coordinates extraction complete.")
    print(extracted_coordinates_data)

    # Extract venues data
    extracted_venues_data = extract_venues_data(extracted_prov_pop_data, extracted_coordinates_data)
    log_progress("Places and venue data extraction complete.")
    print(extracted_venues_data)

    # Extract tourist data
    extracted_tourist_data = extract_tourist_data()
    log_progress("Tourism data extraction complete. Initiating Transformation process...")
    print(extracted_tourist_data)
    
    # Extract GeoJSON data
    extracted_geojson_data = extract_geojson_data()
    log_progress("GeoJSON extraction complete")
    print(extracted_geojson_data)

    # Transform data
    transform_data()
    log_progress("Data transformation complete.")

def extract_province_population():
    wiki_url = "https://en.wikipedia.org/wiki/List_of_Philippine_provinces_by_population"
    return extract_provinces(wiki_url, "extracted_data/province_population.csv")

def extract_province_coordinates_data(extracted_wiki_data):
    provinces_list = extracted_wiki_data["Province"].tolist()
    return extract_coordinates(provinces_list, "extracted_data/province_coordinates.csv")

def extract_venues_data(extracted_prov_pop_data, extracted_coordinates_data):
    province_names = extracted_prov_pop_data["Province"]
    coordinates = [(lat, lng) for lat, lng in zip(extracted_coordinates_data["Latitude"], extracted_coordinates_data['Longitude'])]
    return extract_venues(coordinates, province_names, "extracted_data/province_venues.csv")

def extract_tourist_data():
    return extract_tourists("extracted_data/province_tourists.csv")

def extract_geojson_data():
    geo_url = "https://raw.githubusercontent.com/macoymejia/geojsonph/master/Province/Provinces.json"
    extract_geo(geo_url, "extracted_data/province_geometry.json")

def transform_data():
    # Transform province population data
    transformed_prov_pop_data = transform_provinces("extracted_data/province_population.csv", "transformed_data/province_population.csv")
    log_progress("Province population data transformation complete.")
    print(transformed_prov_pop_data)

    # Transform coordinates data
    transformed_coordinates_data = transform_coordinates(transformed_prov_pop_data, "extracted_data/province_coordinates.csv", "transformed_data/province_coordinates.csv")
    log_progress("Coordinates data transformation complete.")
    print(transformed_coordinates_data)

    # Transform venues data
    transformed_venues_data = transform_venues("extracted_data/province_venues.csv", "transformed_data/province_venues.csv")
    log_progress("Venues data transformation complete.")
    print(transformed_venues_data)

    # Transform tourist data
    transformed_tourist_data = transform_tourists("extracted_data/province_tourists.csv", "transformed_data/province_tourists.csv")
    log_progress("Tourist data transformation complete.")
    print(transformed_tourist_data)

if __name__ == "__main__":
    if not os.path.exists("extracted_data"):
        os.makedirs("extracted_data")
    if not os.path.exists("transformed_data"):
        os.makedirs("transformed_data")
    main()
