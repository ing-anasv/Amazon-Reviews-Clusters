# Spacy text processing module

import spacy
import re

# ner = named entity recognition, parser = dependency labels, textcat = document labels, tok2vec = assign token to vectors
# morphologizer = Assign morphological features
# Loading the model is taking a lot of time 
#nlp = spacy.load("en_core_web_sm", disable=["ner", "parser", "textcat", "tok2vec", "morphologizer"]) 

# Spacy blank creates a pipeline without any model loaded, so it is faster
nlp = spacy.blank("en")  
# just add lemmatizer
nlp.add_pipe("lemmatizer", config={"mode": "rule"})
nlp.initialize()

def spacy_processing(text_list):

    """
    Process a list of texts using Spacy for:

    - lemmatization
    - stopword removal
    - keeping negations: no, not, never
    - removal of symbols -, ().

    Returns: a list where each element is a cleaned string ready for embeddings.
    
    """

    # List to save results - each element will be one row of summary + review processed
    outputs = []

    # Stopwords to keep
    stopwords_keep = ["no", "not", "never"]

    # Save the cleaning regex
    clean_regex = re.compile(r"[^\w\s]+")

 

    # Process rows
    for doc in nlp.pipe(text_list, batch_size=5000):

        # List to save tokens of this row
        row_tokens = []  

        for token in doc: #Doc is the result of processing a text

             # When processing texts like don't, the negation is not recognized so there is a validation needed
            if "n't" in token.text: #token.text returns the original word  
                row_tokens.append("not")  # add the negation 
                continue  # ignore the rest of processing for this token because the negation is already added

            # If not a n't or a symbol, lematize
            lemma = token.lemma_  # lemma_ returns string

            # Verify stopwords
            if token.is_stop == True:  
                # Check if it is in the list of stopwords to keep
                if lemma not in stopwords_keep:
                    continue  # skip token so it does not get added to the list

            # Add lemma to the row list
            row_tokens.append(lemma)

        # Convert list of row token in a string to save it 
        row_output = " ".join(row_tokens).strip()

        # ^ not word characters, \w is a word character, \s is space
        row_output = clean_regex.sub(" ", row_output).strip()


        # Save in the main list
        outputs.append(row_output)

    return outputs
