#Test 1: Load one file at time

"""

    This module defines two functions to load data from the data/raw directory. 
    
"""

import pandas as pd
from pathlib import Path

#Define root
root= "amazon-reviews"


#Function to read data
def load_info(file_path):
    
    """
    Reads a big file from Amazon's reviews (.json o .json.gz).
    Returns a pandas data frame.

    """

    f_path= Path(file_path)

    # Check if the path is correct

    if not f_path.exists():
        print(f"Path {f_path} not found.")
        return None

    print(f"Reading file: {f_path.name} ...")

    # Read JSON file 
    df = pd.read_json(f_path, lines=True)

    print(f" Loaded {len(df)} rows and {len(df.columns)} columns.")
    return df


#Test 2: Identify files in data - raw (first step to read multiple files)

raw_dir = Path("data/raw") #Directory where raw data is

def multiple_files():

    """
    Lists all the files in the data/raw directory

    """
    files = list(raw_dir.glob("*.json")) + list(raw_dir.glob("*.json.gz")) #Depending on how the file downloads
    return sorted(files) #Avoid confussion when loading more data sets

