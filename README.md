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
│
├── data/
│ └── raw/ # Original .json.gz files (untouched and not uploaded to Github)
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
| Day 4 — Language Detection | Created `lang_detection.py` to detect the review's language using langdetec library. Also made some test on `explore_data.ipynb`. |
| **Next** | Continue text pre-processing. |


---

> This README will be updated progressively as the project evolves. The idea is to keep the process transparent and incremental, rather than jumping straight to the final result.
