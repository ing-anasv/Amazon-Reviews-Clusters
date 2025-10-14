# Data ingestion correction - chunks 

"""

This module is used for an adecuate data ingestion with the following parameters:

    - Big data management
    - Chunk reading - could be also used with DuckDB, Dask or Polars 
    - Parquet conversion
    - Column selection 


"""

from pathlib import Path
from src.load_data import multiple_files  

# Output for processed files
output_dir = Path("data/processed")


def list_raw_files():

    """
    Returns the files in data/raw.
    
    """
    files = multiple_files()
    print(f"{len(files)} files found in data/raw/")
    print(files)
    return files
