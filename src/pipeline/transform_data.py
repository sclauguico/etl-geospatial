import glob
import pandas as pd
import os


def transform_provinces(read_file_path_name, write_file_path_name):
    df_prov_pop = pd.read_csv(read_file_path_name)
    
    df_prov_pop = df_prov_pop[df_prov_pop.Province != "Philippines"]
    
    df_prov_pop["Population"] = df_prov_pop["Population"].str.replace(',','')
    df_prov_pop["Population"] = df_prov_pop["Population"].astype("int")
    
    df_prov_pop.columns = [x.lower() for x in df_prov_pop.columns]
    df_prov_pop.columns = df_prov_pop.columns.str.replace("[ ]", "_", regex=True)
    
    df_prov_pop.to_csv(write_file_path_name, index=False)
    
    return df_prov_pop
    
def transform_coordinates(df_prov_pop, read_file_path_name, write_file_path_name):
    df_coordinates = pd.read_csv(read_file_path_name)
    
    df_prov_coordinates = pd.DataFrame(columns=["province", "latitude", "longitude"])
    
    df_prov_coordinates["province"] = df_prov_pop.province
    df_prov_coordinates["latitude"] = df_coordinates.latitude
    df_prov_coordinates["longitude"] = df_coordinates.longitude
    
    df_coordinates.to_csv(write_file_path_name, index=False)
    
    return df_coordinates

def transform_venues(read_file_path_name, write_file_path_name):
    df_venues = pd.read_csv(read_file_path_name)
    df_venues.columns = [x.lower() for x in df_venues.columns]
    df_venues.columns = df_venues.columns.str.replace("[ ]", "_", regex=True)
    df_venues = df_venues[df_venues.province != "Philippines"]
    
    df_quezon = df_venues[df_venues.province == "Quezon"]
    df_quezon.province = "Metro Manila"
    
    df_venues_qc_filtered = df_venues[df_venues.province != "Quezon"]
    
    df_venues_transformed = pd.concat([df_venues_qc_filtered, df_quezon])

    df_venues_transformed.to_csv(write_file_path_name, index=False)
    
    return df_venues_transformed
    

def transform_tourists(read_file_path_name, write_file_path_name):
    df_tourists = pd.read_csv(read_file_path_name)
    
    df_tourists.columns = [x.lower() for x in df_tourists.columns]
    df_tourists.columns = df_tourists.columns.str.replace("[ ]", "_", regex=True)
    
    df_tourists = df_tourists[df_tourists["no._of_foreign_tourists"] != "-"]
    
    df_tourists['no._of_foreign_tourists'] = df_tourists['no._of_foreign_tourists'].str.replace(',','')
    df_tourists["no._of_foreign_tourists"] = df_tourists["no._of_foreign_tourists"].astype('int')
    df_tourists.rename(columns = {"no._of_foreign_tourists" : "num_tourists"}, inplace = True)

    df_tourists.to_csv(write_file_path_name, index=False)
    
    
# def read_csv_files(data_directory):
#     dataframes = {}

#     csv_files = glob.glob(f"{data_directory}/*.csv")
#     for csv_file in csv_files:
#         df = pd.read_csv(csv_file)
#         file_name = csv_file.replace(data_directory + "\\", "").replace(".csv", "")  # Extract the file name
#         dataframes[file_name] = df

#     return dataframes

# def read_json_file(file_to_process):
#     dataframe = pd.read_json(data_directory + file_to_process)
#     return dataframe


# data_directory = "extracted_data"
# dataframes = read_csv_files(data_directory)

# for key, dataframe in dataframes.items():
#     globals()[f'df_{key}'] = dataframe

# print(df_provinces.head())
# print(df_coordinates.head())
# print(df_venues.head())
# print(df_tourists.head())

# df_province_geometry = read_json_file("/province_geometry.json")
# print(df_province_geometry.head())