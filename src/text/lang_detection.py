# Detect language 

"""

This module is designed to detect language anf filter only english reviews 

"""

# from langdetect import detect, LangDetectException Not used - too slow
# Library changed to fasttext to optimize time
import fasttext

# Load fasttext model
ft_model = fasttext.load_model("models/lid.176.ftz")

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

    # To identify the actual language 2 words (at least) are needed
    if len(text.split()) < 2: # .split to actually know how many words
        return "unknown"

    # Detect language
    try:
        prediction = ft_model.predict(text) #output ((__label__en),array[value])
        lan = prediction[0][0].replace("__label__", "")
        confidence = prediction[1][0] 

        # Only use english reviews over 0.8 precission to make sure the filter works properly
        if confidence >= 0.8:
            return lan
        else:
            return "unknown"
    except:
        return "unknown" # Not recognized


def is_english(text):

    """

    Filter english reviews 

    """
    # Returns true if the language is english
    return detect_lan(text) == "en"