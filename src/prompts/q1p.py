##############################################  PROJECTION  ###################################################
MOVIES_PROMPT = """Given a set of movie descriptions and a critic review, use the critic review and the movie advertised by the description provided to recommend another movie. Answer with ONLY ONE movie title or "unsure". Here are some examples:
Example: 
If Description = "A fun thrilling adventure movie!" and ReviewText = "Very boring watch, the performances in this movie were horrible",
answer with "Harry Potter".

Example 2:
If Description = "A factual and educational documentary of Napoleon Bonaparte" and ReviewText = "A dramatized telling of Napoleon's life",
answer with "Our Planet".

Example 3:
If Description = "A fun spin on Greek mythology through an adventure epic following a boy Percy Jackson" and ReviewText = "A fun fantasy adventure!", 
answer with "Percy Jackson".
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

MOVIES_PROMPT_SHORT = """Given information including movie descriptions and a critic reviews for movies with a positive sentiment, summarize the good qualities in this movie that led to a favorable rating."""

PRODUCTS_PROMPT_SHORT = """Given the following fields related to amazon products, summarize the product, then answer
whether the product description is consistent with the quality expressed in the review."""

BIRD_PROMPT_SHORT = (
    "Given the following fields related to posts in an online codebase community, summarize how the comment Text related to the post Body"
)

PDMX_PROMPT_SHORT = (
    """Given the following fields, provide an overview on the music type, and analyze the given scores. Give exactly 50 words of summary."""
)

BEER_PROMPT_SHORT = """Given the following fields, provide an high-level overview on the beer and review in a 20 words paragraph."""
