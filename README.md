# Amazon Reviews Clustering — Project in Progress

This repository contains a technical project focused on building a pipeline to analyze and group Amazon product reviews using unsupervised clustering and semantic embeddings.

---

## Project Goal

The objective is to develop a system capable of:

- Group similar reviews with no previous tags.
- Assign new reviews to the closest cluster or create a new one if it doesn't fit.
- Loading large `.json.gz` Amazon review datasets efficiently.
- Processing and cleaning raw review text.
- Generating semantic embeddings using transformer-based models.
- Grouping reviews into thematic clusters **without labels**.
- Automatically generating meaningful labels for each cluster.
- Exposing the results through an interactive Streamlit app.
  

---

## Current Project Structure (evolving)

```
amazon-reviews/
│
├── src/ # Code for loading, preprocessing, embeddings, ect.
│ └── load_data.py # Data loading with path validation
│ └── features
│  └── select_columns.py # Select columns that will be used for embedding
│ └── text
│  └── clean_text.py # Cleans url, emojis, punctuation
│  └── lang_detection.py # Language detection
│ └── pipeline
│  └── ingestion.py # Read files in chunks, process them and saves to parquet 
│ └── models
│  └── lid.176.ftz # Model to detect language using fasttext
│
├── data/
│ └── raw/ # Original .json.gz files (untouched and not uploaded to Github)
│ └── processed/ # Parquet file with processed data frame (not uploaded to Github)
│
├── notebooks/ # Initial exploration and testing
│ └── test_load_data.ipynb # Test data loading
│ └── explore_data.py # Data exploration
├── app/ # (To be implemented) Streamlit interface
├── requirements.txt
├── .gitignore
└── README.md


```
---
### DISCLAIMER : Needed directory for the pipeline

Before running the pipeline, the folder structure must be verified. Specifically check the  **data** directory that **is not included in the repository** so it should be created manually. Inside it ensure the following subdirectories are present:

```

data/
│ ├── raw/ # Original files (.json o .json.gz)
│ └── processed/ # Clean file(s) in .parquet format

```
---

## Progress Checklist

- [x] Virtual environment configured
- [x] Dependencies installed (`pip install -r requirements.txt`)
- [x] Sample dataset downloaded to `data/raw/`
- [x] Basic data loading function implemented with file validation
- [x] `src/` successfully tracked in Git
- [x] Explore dataset columns (`df.columns`, `df.head()`)
- [x] Select relevant fields for semantic analysis
- [x] Text cleaning module (normalization, stopwords, etc.)
- [ ] Embedding generation
- [ ] Clustering and evaluation
- [ ] Streamlit visualization

---

## Development Log (iterative progress)

| Date        | Update                                                                                       |
|-------------|----------------------------------------------------------------------------------------------|
| Day 1 — Setup and Data load | Repository initialized and basic structure created (`src/`, `notebooks/`, `data/raw/`). Virtual environment configured and dependencies installed. Data Loading Module implemented with `load_data.py`. |
| Day 2 — Initial EDA | Created `explore_data.ipynb` with preview to avoid memory issues. Confirmed available columns with 3 datasets and detected optional fields. Created file `select_columns.py` to separate text columns from context columns for further processing. |
| Day 3 — Data Cleaning | Created `clean_text.py` to clean text including: converting to lower case, emoji, spaces, punctuation and URL removal. |
| Day 4 — Language Detection | Created `lang_detection.py` to detect the review's language using langdetec library. Also made some test on `explore_data.ipynb`. Created file `ingestion.py` to ingest data by chunk and process it correctly. |
| Day 5 — Ingestion | Runned some tests to verify if the cleaning and language detection modules worked fine with chunk-loaded rows. Then used those tests to actually build the pipeline of ingestion-processing-data saving in parquet. **Issue found:** takes way to long to finish the ingestion process. |
| Day 6 — Ingestion Time Issues | Changed .apply() to a function that can process rows in groups or batches. The file was taking too long so the library  **multiprocessing** was implemented. Even after those tries, a 3GB file in .json.gz format took 9 hours to be processed. More optimization needs to be done. |
| Day 7 — Continue with Ingestion Time Issues | Using prints before each process helped identify the part of the code that took the longest: language detection. Based on this, a decision was made to change the library from *langdetect* to *fasttext*. With this change the data ingestion takes less time. |
| **Next** | Merge all the processed files in one. |


---
> This README will be updated progressively as the project evolves. The idea is to keep the process transparent and incremental, rather than jumping straight to the final result.
