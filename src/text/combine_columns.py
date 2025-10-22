# Function to join summary and review


def join_summary_review(summary, reviewText):

    """

    This module combines the summary and review text into a single string for each entry
    to avoid processing them separately.

    Output: A list with combined texts

    """

    #list to combine both texts
    combined = []

    for s,r in zip(summary, reviewText):

        # Check combinations of review and summary to decide how to join

        #  If both are empty
        if s == "" and r == "":
            combined.append("")

        # If both have text
        elif s != "" and r != "":
           combined.append(s + " " + r)

        # If only summary has text
        elif s != "":
            combined.append(s)
        else: # If only review has text 
            combined.append(r)

    return combined