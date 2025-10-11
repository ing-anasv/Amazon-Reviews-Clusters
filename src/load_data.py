#Try 1: Load one file at time

import pandas as pd
from pathlib import Path

#Function to read data
def load_info(file_path):
    
    """
    Reads a big file from Amazon's reviews (.json o .json.gz).
    Returns a pandas data frame.

    """

    f_path = Path(file_path)

    # Check if the path is correct

    if not f_path.exists():
        print(f"Path {f_path} not found.")
        return None

    print(f"Reading file: {f_path.name} ...")

    # Read JSON file 
    df = pd.read_json(f_path, lines=True)

    print(f" Loaded {len(df)} rows and {len(df.columns)} columns.")
    return df


