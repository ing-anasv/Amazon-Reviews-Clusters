# Detect language 

"""

This module is designed to detect language anf filter only english reviews 

"""

from langdetect import detect, LangDetectException

def detect_lan(text):

    """

    This function uses the langdetect library to detect the text language and avoid errors 
    if language is not identified

    """ 
    # Verify if text is empty or null to return unknown
    if not text:
        return "unknown"

    # if is not "empty" but is a string of whitespaces solve with strip
    text = text.strip()

    # To identify the actual language 2 words (at least) are neededed
    if len(text.split()) < 2: # .split to actually know how many words
        return "unknown"

    # Detect language
    try:
        return detect(text)
    except LangDetectException: # error when language is not recognized
        return "unknown"
    except:
        return "unknown" # other possible errors


def is_english(text):

    """

    Filter english reviews 

    """
    # Returns true if the language is english
    return detect_lan(text) == "en"