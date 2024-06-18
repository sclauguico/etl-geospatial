import pandas as pd
from sqlalchemy import create_engine

host = "localhost"
user = "root"
password = "123"
database = "philippines"

engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

def load_daya(read_file_path_name, write_file_path_name):
    df = pd.read_csv(read_file_path_name)
    df.to_sql(write_file_path_name, con=engine, index=False, if_exists="replace")
    
    
engine.dispose()