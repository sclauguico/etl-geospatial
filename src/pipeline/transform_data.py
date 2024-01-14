import glob
import pandas as pd
import os


def read_csv_files(data_directory):
    dataframes = {}

    csv_files = glob.glob(f"{data_directory}/*.csv")
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        file_name = csv_file.replace(data_directory + "\\", "").replace(".csv", "")  # Extract the file name
        dataframes[file_name] = df

    return dataframes

def read_json_file(file_to_process):
    dataframe = pd.read_json(data_directory + file_to_process)
    return dataframe


data_directory = "extracted_data"
dataframes = read_csv_files(data_directory)

for key, dataframe in dataframes.items():
    globals()[f'df_{key}'] = dataframe

print(df_provinces.head())
print(df_coordinates.head())
print(df_venues.head())
print(df_tourists.head())

df_province_geometry = read_json_file("/province_geometry.json")
print(df_province_geometry.head())