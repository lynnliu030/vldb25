##############################################  FILTER  ###################################################
SENTIMENT_PROMPT = """Given the following ReviewText, answer whether the sentiment associated is "Positive" or "Negative". Answer with ONLY "Positive", "Negative", or "Neutral". Here are some examples first:
Example: ReviewText: "Very boring watch, the performances in this movie were horrible"
Answer: "Negative".

Example: ReviewText: "Entertaining fun time with family!"
Answer: "Positive"

Example: ReviewText: "Not sure how I feel about this movie..."
Answer: "Neutral"

Here is the actual review:
"""

MOVIES_PROMPT = """Given a movie description and a review, analyze whether the movie would be suitable for kids. Answer with ONLY "Yes" or "No". Here are some examples first:
Example: 
movie_info: "This violent depiction of war in Ancient Greece showcases the horrific realities of early life in the mediterranean"
review_content: "This was a tragic viewing"
Answer: "No".

Example:
movie_info: "Percy Jackson is a wonderful fantasy tale that is sure to be an entertaining fun time with family!"
review_content: "This was a fantastic watch with my kids!"
Answer: "Yes"

Here is the actual review:
"""

PRODUCTS_PROMPT = """Given a product description and a review, answer whether the product would be suitable for kids. Answer with ONLY "Yes" or "No". Here are some examples first:
Example: 
description: "1900 Classical Violin Music"
reviewText: "This is a comforting track that gives me peaceful time away from my children"
Answer: "No".

Example: 
description: "Smooth funky jazz music!"
reviewText: "My son really loves listening to this!"
Answer: "Yes"

Here is the actual review:
"""

MOVIES_PROMPT_SHORT = "Given the following fields, answer in ONE word, 'Yes' or 'No', whether the movie would be suitable for kids."

PRODUCTS_PROMPT_SHORT = """Given the following fields determine if the review speaks positively ('POSITIVE'), negatively ('NEGATIVE'), or netural ('NEUTRAL') about the product. Answer only 'POSITIVE', 'NEGATIVE', or 'NEUTRAL', nothing else."""

BEER_PROMPT_SHORT = """Based on the review text and overall rating, does this beer seem likely to be recommended by the reviewer? Answer only 'YES', 'NO', or 'NEUTRAL', nothing else."""

PDMX_PROMPT_SHORT = """Based on following fields, answer 'YES' or 'NO' if the song name appears to reference a specific individual. If song name is empty, then answer 'NO'. Answer only 'YES' or 'NO', nothing else. """

BIRD_PROMPT_SHORT = """Given the following fields related to posts in an online codebase community, answer whether the post is related to statistics. Answer with only 'YES' or 'NO'."""
