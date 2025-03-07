##############################################  MULTI-LLM ###################################################
SENTIMENT_PROMPT = """Given the following review, answer whether the sentiment associated is "Positive" or "Negative". Answer with ONLY "Positive" or "Negative". Here are some examples first:
Example: ReviewText: "Very boring watch, the performances in this movie were horrible"
Answer: "Negative".

Example: ReviewText: "Entertaining fun time with family!"
Answer: "Positive"

Example: ReviewText: "Not sure how I feel about this movie..."
Answer: "Negative"

Here is the actual review:
"""

MOVIES_PROMPT = """Given a movie description and a critic review, use the critic review and the description to recommend another movie. Answer with only the movie title or "unsure". Here are some examples:
Example: 
If Description = "A fun thrilling adventure movie!" and ReviewText = "Very boring watch, the performances in this movie were horrible",
answer with "Harry Potter".

Example 2:
If Description = "A factual and educational documentary of Napoleon Bonaparte" and ReviewText = "A dramatized telling of Napoleon's life",
answer with "Our Planet".

Example 3:
If Description = "A fun spin on Greek mythology through an adventure epic following a boy Percy Jackson" and ReviewText = "A fun fantasy adventure!", 
answer with "Percy Jackson".

Here is the actual description and review:
"""

PRODUCTS_PROMPT = """Answer whether the reviewText matches the quality indicated in the product description. If it matches, "yes", if it doesn't suggest,"no". Only answer with "yes" or "no", nothing else. Here are some examples:
Example 1: 
If description = "Loud 'N Clear Sound Amplifier allows you to 
listen without disturbing others." and
reviewText = "quiet", answer a single word "yes". 

Example 2:
If description = "Loud 'N Clear Sound Amplifier allows you to 
listen without disturbing others." and
reviewText = "bad quality, loud", answer a single word "no". 

Here is the actual description and review:
"""

SENTIMENT_PROMPT_SHORT = """Given the following review, answer whether the sentiment associated is "POSITIVE" or "NEGATIVE". Answer in all caps with ONLY "POSITIVE" or "NEGATIVE": """

MOVIES_PROMPT_SHORT = """Given information including movie descriptions and a critic reviews for movies with a positive sentiment, summarize the good qualities in this movie that led to a favorable rating."""

PRODUCTS_PROMPT_SHORT = """Given the following fields related to amazon products, summarize the product, then answer
whether the product description is consistent with the quality expressed in the review."""
