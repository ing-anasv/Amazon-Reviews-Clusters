# Module to merge all the parquet files in one to use for embedding

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import gc # Library to help release memory so it doesn't crash


def merge_par(): 

    """

    Module to merge all the .parquet files to use them in embedding
    Output: One .parquet file with all the processed data.

    If the file already exists, appends new entrys. 

    """

    # Directory to look for
    directory = Path("data/processed/spacy")

    # Final file name
    final_file = directory / "dataset_embedding_spacy.parquet"

    # File with the name of the processed datasets that are already in the parquet
    processed_txt = directory / "processed_spacy_sources.txt"


    # Error parquet size 0, delete it
    if final_file.exists() and final_file.stat().st_size == 0:
        print("Detected empty parquet file, deleting to recreate properly...")
        final_file.unlink()  # Deletes the file

    # If the txt file exists, load it
    processed_sources = []
    if processed_txt.exists():
        with open(processed_txt, "r") as f:
            lines = f.readlines()
            for l in lines: 
                processed_sources.append(l.strip())
        print(f"Loaded {len(processed_sources)} processed entries from TXT file.")


    # Find parquets not including the dataset_embedding
    files = list(directory.glob("*_spacy.parquet")) 

    # Filter files NOT included
    new_files = []

    for f in files:
        source_name = f.stem 
        if source_name not in processed_sources and source_name != "dataset_embedding_spacy":
            new_files.append(f)

    # CASE 1: If the final file doesn't exists - Create it
    if not final_file.exists():
        print(f"Creating file: {final_file.name}")

         # Initialize writer as none since no files are loaded yet 
        writer = None

        for par in new_files:
            # Read in batches
            pf = pq.ParquetFile(par)
            if writer is None:
                # Take schema first
                schema = pf.schema.to_arrow_schema()
                writer = pq.ParquetWriter(final_file, schema, compression="snappy", write_statistics=False)

            for batch in pf.iter_batches(batch_size=50000):
                # Use the method from_batches because traditional writing won't work
                writer.write_table(pa.Table.from_batches([batch]))

            # Save file name to TXT
            with open(processed_txt, "a") as source_name:
                source_name.write(par.stem + "\n")

            gc.collect()

        # Close writer 
        if writer: # First check if it changed from None to a writer object
            writer.close()

        print("Saving to parquet succesful")

        return
    
    # CASE 2: The final file already exists, just append new entrys
    else:
        print(f"{final_file.name} already exists, checking if there is new files to append...")
        
        # If not new files were found
        if len(new_files) == 0:
            print("\nNo files to add.")
            return # End function
        
        print(f"Found {len(new_files)} new files to process")

        # Take just the schema 
        schema = pq.ParquetFile(final_file).schema.to_arrow_schema()

        # Since pyarrow doesn't allow append=True directly, to prevent overwritting
        # binary append mode is being used (parquet files are binary)
        with open(final_file, "ab") as f: 
            writer = pq.ParquetWriter(f, schema, compression="snappy", write_statistics=False)

            for f_new in new_files:
                print(f"Appending: {f_new.name}")
                # Read in batches
                pf_new = pq.ParquetFile(f_new)
                for batch in pf_new.iter_batches(batch_size=50000):
                    writer.write_table(pa.Table.from_batches([batch]))

                 # Add this file to txt 
                with open(processed_txt, "a") as source_name:
                    source_name.write(f_new.stem + "\n")

                gc.collect()

            writer.close()

        print(f"Successfully added {len(new_files)} new files")
        return

