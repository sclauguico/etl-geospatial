import pandas as pd
from src.pipeline.extract_data import extract_provinces, extract_coordinates, extract_geo, extract_venues, extract_tourism
from src.logger import log_progress

def main():
    # Scrape province and population data
    extracted_wiki_data = extract_province_population()
    log_progress("Web scraping complete.")
    print(extracted_wiki_data)

    # Extract coordinates of provinces
    extracted_coordinate_data = extract_province_coordinates_data(extracted_wiki_data)
    log_progress("Coordinates extraction complete.")
    print(extracted_coordinate_data)

    # Extract GeoJSON data
    extracted_geojson_data = extract_geojson_data()
    log_progress("GeoJSON extraction complete")
    print(extracted_geojson_data)

    # Extract venues data
    extracted_venues_data, extracted_venue_categories_data = extract_venues_data(extracted_wiki_data, extracted_coordinate_data)
    log_progress("Places and venue data extraction complete.")
    print(extracted_venues_data, extracted_venue_categories_data)

    # Extract tourism data
    extracted_tourism_data = extract_tourism_data()
    log_progress("Tourism data extraction complete. Initiating Transformation process...")
    print(extracted_tourism_data)

def extract_province_population():
    wiki_url = "https://en.wikipedia.org/wiki/List_of_Philippine_provinces_by_population"
    attribute_list = ["Province", "Population"]
    return extract_provinces(wiki_url, attribute_list, "extracted_data/provinces.csv")

def extract_province_coordinates_data(extracted_wiki_data):
    provinces_list = extracted_wiki_data["Province"].tolist()
    return extract_coordinates(provinces_list, "extracted_data/province_coordinates.csv")

def extract_geojson_data():
    geo_url = "https://raw.githubusercontent.com/macoymejia/geojsonph/master/Province/Provinces.json"
    return extract_geo(geo_url, "extracted_data/province_geometry.json")

def extract_venues_data(extracted_wiki_data, extracted_coordinate_data):
    province_names = extracted_wiki_data["Province"]
    coordinates = [(lat, lng) for lat, lng in zip(extracted_coordinate_data["Latitude"], extracted_coordinate_data['Longitude'])]
    return extract_venues(coordinates, province_names, "extracted_data/province_venues.csv", "extracted_data/venue_categories.csv")

def extract_tourism_data():
    return extract_tourism("extracted_data/province_tourism.csv")

if __name__ == "__main__":
    main()
