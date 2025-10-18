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
import pyarrow.parquet as pq
from multiprocessing import Pool, cpu_count
from src.load_data import multiple_files  
from src.text.clean_text import clean, clean_group
from src.text.lang_detection import is_english
from src.features.select_columns import split_columns
from src.pipeline.merge_parquet import merge_par



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

    # List files
    raw_files = multiple_files()
    
    # Verify raw_files is not empty 

    if len(raw_files) == 0: 
         print("No files to process in data/raw.")
         return None # Break function if raw is empty

    # Start processing 
    print("Processing started...")

    #Initialize one pool per file
    p = Pool(cpu_count()) # cpu_count returns cpu in system
    for f_path in raw_files:

        # Initialize writer as none before the for loop. Should be as none because the df is the chunk 
        writer = None

        print(f"\nProcessing file {f_path.name}\n")

        # Since processing everything in just one file takes to long the cleaned columns will be saved in different files

        source_name = f_path.stem 
    
        # If the file processing is interrupted the info will be saved in a temporary file to avoid skipping parts of the dataset
        # If the file is processed completly it will change names to final
        final_parquet = output_dir / f"{source_name}.parquet"
        temp_parquet = output_dir / f"{source_name}.temp.parquet"

        # Check if final file exists so we can skip that one and clean the next
        if final_parquet.exists():
            print(f"{source_name} already processed. Moving to the next file.")
            continue

        # If there is a temporary file, it needs to be deleted and start from scratch
        if temp_parquet.exists():
            print(f"{temp_parquet} incomplete. Starting from scratch...")
            temp_parquet.unlink()  # Delete temporary file

        # First Step: Read file in chunks
        try:
            read_chunk = pd.read_json(f_path, lines=True, chunksize=50000)  
        except Exception as e:
            print(f"ERROR: could not open file: {f_path.name}. Message: {e}")
            continue  # skip file if it cannot be read

        
        # Read chunks one by one
        for chunk in read_chunk:

            # Print to check where is taking long
            #print("Cleaning started")

            # Second Step - apply cleaning function
            chunk["clean_review"] = clean_group(chunk["reviewText"].tolist())
            chunk["clean_summary"] = clean_group(chunk["summary"].tolist())

            # Print to check where is takinkg long
            #print("Language detecting started")

            # Third step - Apply language detection function
            # Using the reviewText as reference to keep or discard based on language
            chunk["is_english"] = p.map(is_english, chunk["clean_review"].tolist())

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
            if writer is None: # is None on the first chunk iteration
                # Parquet writer: parquet.ParquetWriter(where, schema (table already created), 
                # compression (default is snappy), write_statistics (not needed, default = True),
                print(f"\nWriting in {temp_parquet} started.")
                writer = pq.ParquetWriter(temp_parquet, table.schema, compression="snappy", write_statistics=False)
        
            # Step 7  Append chunk
            writer.write_table(table)

        # Close writer 
        if writer: # First check if it changed from None to a writer object
            writer.close()
            # change the name to the final parquet
            temp_parquet.rename(final_parquet)
        else: # by the end the file is empty it needs to be deleted
            if temp_parquet.exists(): 
                temp_parquet.unlink()


        """
            DISCLAIMER - MEMORY MANAGEMENT
            Explicit memory release was not used:
                - after each iteration the chunk is overwritten
                - small chunksize
                - for bigger chunk management memory release options should be considered
             
        """
    print(f"\nFile {final_parquet} generated successfully.")

