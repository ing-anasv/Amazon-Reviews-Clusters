# Text cleaning module

"""

This module is used to clean of the text to prepare it for the embedding model.

"""
import re  # for regex

# Remove URLS

def remove_urls(text):

    """
    Removes URLs like http://..., https://..., www....

    """
    # "\S matches any character that is not a whitespace" and 
    # + "match 1 or more repetitions of the preceding RE" (re documentation)
    url_pattern = r"http\S+ | www\.\S+"

    # Re.sub (pattern, replacement, string)
    return re.sub(url_pattern, "", text)  # replace URLs with empty string


# Remove punctuation

def remove_punctuation(text):

    """
    Removes common punctuation like ! ? , . ; : but keeps normal words intact

    """
    punct = r"[!?.,;:]+"
    text = re.sub(punct, " ", text)  # Replace punctuation with a space to avoid word merging
    return text

# Remove emojis

import re

def remove_emojis(text):

    """

    Removes emojis from a text string

    """
    # compile: turn the codes to regular expression objects
    
    emojis = re.compile(
        "["                       
        u"\U0001F650-\U0001F67F"  # Ornamental Dingbats 
        u"\U0001F600-\U0001F64F"  # Emoticons
        u"\U0001F300-\U0001F5FF"  # Miscellaneous Symbols and Pictographs
        u"\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        u"\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        u"\U0001F680-\U0001F6FF"  # Transport and Map Symbols
        u"\U0001F1E0-\U0001F1FF"  # Flags 
        u"\u2700-\u27BF"          # Other symbols
        "]+",
        flags=re.UNICODE
    )
    return emojis.sub("", text)


def clean(text):
    """
    
    Cleaning steps:
    - convert to lowercase
    - remove extra spaces
    - remove URLs
    - remove punctuation
    - remove emojis

    """
    if not isinstance(text, str):
        return ""  # returns empty string to avoid problems with the model
    
    # First remove URLS
    text = remove_urls(text)

    # Remove punctuation
    text = remove_punctuation(text)

    # Remove emojis
    text = remove_emojis(text)

    # Lower case
    text = text.lower()

    # Leave just a normal space between words
    text = text.split() # Left the argument to none so it splits to any space
    text = " ".join(text)

    return text

def clean_group(text_group): #Use this to clean in batches so when called in the pipeline doesn't do it on each row separately
    
    """
    Input: list of text.
    Output: cleaned list.

    """
    cleaned_texts = []  # store the results

    for text in text_group:
        cleaned_texts.append(clean(text)) 

    return cleaned_texts
