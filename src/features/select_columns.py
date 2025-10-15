# Select columns for further work

"""
This module defines which columns will be used for the semantic
embedding pipeline and which are kept for later interpretation.

Since embedding works with natural language text, the columns included are only the ones
with text fields. Other columns that were found relevant will be used as context columns (if needed). 

"""

# Columns with natural text
text_cols = ["reviewText", "summary"]  

# Context columns - just in case 
context_cols = ["asin", "overall", "unixReviewTime"]

# Define the base column that needs to be in the dataset - reviewText - gives out the most information
needed_col = ["reviewText"]


# Function to take the columns of the data set

def split_columns(data_cols):
   
    """
    Takes a list of columns from a dataset and separates them into:
    - Text columns and Context columns 

    """

    # Check if the review text column exists
    if needed_col[0] not in data_cols:
        raise ValueError(f"Required column '{needed_col[0]}' not found in dataset.")

    # Separate columns
    text = []
    context= []

    for col in text_cols:
        if col in data_cols:
            text.append(col)
    
    for col in context_cols:
        if col in data_cols:
            context.append(col)

    # Return lists as dictionary to keep track on which one is context and which one is text
    return {
        "text": text,
        "context": context
    }
    

