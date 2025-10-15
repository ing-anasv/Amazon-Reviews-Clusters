# Data ingestion correction - chunks 

"""

This module is used for an adecuate data ingestion with the following parameters:

    - Big data management
    - Chunk reading - could be also used with DuckDB, Dask or Polars 
    - Parquet conversion
    - Column selection 


"""

import pandas as pd
from pathlib import Path
import pyarrow as pa
from src.load_data import multiple_files  
from src.text.clean_text import clean
from src.text.lang_detection import is_english
from src.features.select_columns import split_columns



# Output for processed files
output_dir = Path("data/processed")


# List all the files in the raw directory

def list_raw_files():

    """
    Returns the files in data/raw.
    
    """
    files = multiple_files()
    print(f"{len(files)} files found in data/raw/")
    for f in files:
        print(f.name)
    return files


#Check if the processing is working

def processing_check():  
    
    """
    This function can be used to make sure the processing is working before
    using it in the whole file. 

    """

    # List files
    raw_files = multiple_files()

    # Verify raw_files is not empty 

    if len(raw_files) == 0: 
         print("No files to process in data/raw.")
         return None # Break function if raw is empty
        
    # If there's files found in the directory
    print(f"{len(raw_files)} files found. Ingestion started.")


    # Start file processing in chunks
    for f_path in raw_files:
        print(f"\nProcessing file {f_path.name}\n")

        # First Step: Read file in chunks
        try:
            read_chunk = pd.read_json(f_path, lines=True, chunksize=5000)  
        except Exception as e:
            print(f"ERROR: could not open file: {f_path.name}. Message: {e}")
            continue  # skip file if it cannot be read


        for chunk in read_chunk:
            # Preview just the first chunk to check if it's working
            print(f"{len(chunk)} rows loaded\n")

            # Rows before processing
            rows_before = len(chunk)

            # Second Step - apply cleaning function
            chunk["clean_review"] = chunk["reviewText"].apply(clean)
            chunk["clean_summary"] = chunk["summary"].apply(clean)

            # Third step - Apply language detection function
            # Using the reviewText as reference to keep or discard based on language
            chunk["is_english"] = chunk["clean_review"].apply(is_english) 

            # Count how many reviews are in english
            rows_after = 0 

            for r in chunk["is_english"]:
                if r == True:
                    rows_after +=1

            print(f"Original rows: {rows_before}")
            print(f"English reviews: {rows_after}")
            print(f"Reviews not considered: {rows_before-rows_after}\n")        
            
        
            break  # break after the first chunk



#Save english cleaned rows in a file to use in embedding

def save_to_parquet():

    """

    Pipeline:
    - Detect raw files
    - Read chunks
    - Clean and filter only  reviews
    - Select columns (select_columns.py)
    - Save to a single Parquet file

    """
    # Import parquet - is used only in this function
    import pyarrow.parquet as pq

    # List files
    raw_files = multiple_files()
    
    # Verify raw_files is not empty 

    if len(raw_files) == 0: 
         print("No files to process in data/raw.")
         return None # Break function if raw is empty
    
    # Create output file
    parquet_path = output_dir / "dataset_embeddings.parquet"

    # If the file already exists: delete to avoid duplicates and write from scratch
    
    if parquet_path.exists(): # checks if the file exists
        print("Existing parquet file. Deleting it to start from scratch.")
        parquet_path.unlink()  # deletes the file

    # Initialize writer as none before the for loop. Should be as none because the df is the chunk
    writer = None

    # Start processing 
    print("Processing started...")

    for f_path in raw_files:
        print(f"\nProcessing file {f_path.name}\n")

        # First Step: Read file in chunks
        try:
            read_chunk = pd.read_json(f_path, lines=True, chunksize=5000)  
        except Exception as e:
            print(f"ERROR: could not open file: {f_path.name}. Message: {e}")
            continue  # skip file if it cannot be read


        # Read chunks one by one
        for chunk in read_chunk:

            # Second Step - apply cleaning function
            chunk["clean_review"] = chunk["reviewText"].apply(clean)
            chunk["clean_summary"] = chunk["summary"].apply(clean)

            # Third step - Apply language detection function
            # Using the reviewText as reference to keep or discard based on language
            chunk["is_english"] = chunk["clean_review"].apply(is_english) 

            # Filter only english
            chunk = chunk[chunk["is_english"]]

            # Skip empty chunks to avoid writing empty data
            if len(chunk) == 0:
                print("Chunk skipped (no english reviews)")
                continue

            # Source column to keep track of the original category of the review
            chunk["source"] = f_path.stem

            # Step 4 - Use the select_columns module to pick the context columns
            cols_dict = split_columns(chunk.columns)
            context = cols_dict["context"]

            # Select columns of the process
            selected_cols = ["clean_review", "clean_summary", "reviewText"] + context + ["source"]
            chunk = chunk[selected_cols]

            # Step 5 -  Convert dataframe to table that works with writer
            table = pa.Table.from_pandas(chunk)

            # Step 6 - Check if the file is empty or not
            if writer is None: # is None on the first iteration of the first loop
                # Parquet writer: parquet.ParquetWriter(where, schema (table already created), 
                # compression (default is snappy), write_statistics (not needed, default = True),
                writer = pq.ParquetWriter(parquet_path, table.schema, compression="snappy", write_statistics=False)
           
            # Step 7  Append chunk
            writer.write_table(table)

            print(f"{len(chunk)} rows appended to {parquet_path.name}")

    # Close writer 
    if writer: #First check if it changed from None to a writer object
        writer.close()

        """
            DISCLAIMER - MEMORY MANAGEMENT
            Explicit memory release was not used:
                - after each iteration the chunk is overwritten
                - small chunksize
                - for bigger chunk management memory release options should be considered
             
        """
    print("\nFile dataset_embeddings.parquet generated successfully.")

