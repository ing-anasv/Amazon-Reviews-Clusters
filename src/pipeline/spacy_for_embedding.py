# Module to process dataset_embedding.parquet with spacy for embeddings

import pyarrow.parquet as pq
from pathlib import Path
import pyarrow as pa
import gc # Library to help release memory so it doesn't crash
from multiprocessing import Pool, cpu_count

# Import functions
from src.text.combine_columns import join_summary_review
from src.text.spacy_process import spacy_processing

# Output for processed files
output_dir = Path("data/processed/spacy")

# Input path
input_path = Path("data/processed")


# Function to process JUST ONE parquet file
def process_file(f_path):

    """
    Reads parquet file in chunks, applies join_summary_review and spacy_processing, 
    and prepares the data for the next steps.

    """

    print(f"\nProcessing file {f_path.name}\n")

    source_name = Path(f_path.stem) # Since it keeps the .json.parquet extension, stem is used twice
    source_name = source_name.stem

    # If the file processing is interrupted the info will be saved in a temporary file to avoid skipping parts of the dataset
    # If the file is processed completly it will change names to final
    final_parquet = output_dir / f"{source_name}_spacy.parquet"
    temp_parquet = output_dir / f"{source_name}.temp_spacy.parquet"

    # Check if final file exists so we can skip that one and clean the next
    if final_parquet.exists():
        print(f"{source_name} already processed. Moving to the next file.")
        return

    # If there is a temporary file, it needs to be deleted and start from scratch
    if temp_parquet.exists():
        print(f"{temp_parquet} incomplete. Starting from scratch...")
        temp_parquet.unlink()  # Delete temporary file

    parquet_file = pq.ParquetFile(f_path)
    # Initialize writer as None
    writer = None

    # Iterate over batches
    for batch in parquet_file.iter_batches(batch_size=5000):

        # Take columns to save as lists so they can be processed
        print("Saving columns as lists\n")
        clean_summ = batch["clean_summary"].to_pylist()
        clean_rev = batch["clean_review"].to_pylist()
        asin = batch["asin"].to_pylist()
        source = batch["source"].to_pylist()
        overall = batch["overall"].to_pylist()

        #print("Joining columns\n")
        # Step 1  - Join columns 
        combined_texts = join_summary_review(clean_summ, clean_rev)

        #print("Cleaning columns\n")
        # Step 2 - Process columns with spacy
        embedding_clean = spacy_processing(combined_texts) 


        #print("Converting to table\n")
        # Convert columns to table to add the schema to writer
        table = pa.Table.from_arrays([pa.array(embedding_clean), pa.array(asin), pa.array(source), pa.array(overall)], 
                                    names = ["clean_embedding_text", "asin", "source", "overall"])

        if writer is None: # is None on the first chunk iteration
            print(f"\nWriting in {temp_parquet} started.")
            writer = pq.ParquetWriter(temp_parquet, table.schema, compression="snappy", write_statistics=False)


        print("Writing columns\n")
        writer.write_table(table)

        del clean_summ, clean_rev, asin, source, overall, combined_texts, embedding_clean
        gc.collect() # Avoid memory overload

    # Close writer 
    if writer: # First check if it changed from None to a writer object
        writer.close()
        # change the name to the final parquet
        temp_parquet.rename(final_parquet)
        print(f"\nFinished processing {source_name} and saved to {final_parquet}\n")
    else: # by the end the file is empty it needs to be deleted
        if temp_parquet.exists(): 
            temp_parquet.unlink()



# Function to process with pool 
def embedding_parquet():

    print("Looking for parquet files to process...")

    # Buscar todos los archivos .parquet
    files = list(input_path.glob("*.parquet"))

    # Filter files NOT included
    new_files = []

    for f in files:
        source_name = f.stem 
        if source_name != "dataset_embedding":
            new_files.append(f)

    # Verify raw_files is not empty 

    if len(new_files) == 0: 
        print("No files to process in data/processed.")
        return  # Break function if files is empty

    print(f"{len(new_files)} files found.")

    # Create process pool and execute
    with Pool(int(cpu_count() * 0.75)) as p:
        p.map(process_file, new_files)

