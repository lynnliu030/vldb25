##############################################  AGGREGATION ###################################################
MOVIES_PROMPT = """Given a movie description as context, assign a sentiment score out of 5 to the provided movie review. Answer with ONLY a single integer between 1 (bad) and 5 (good). Here are some examples:
Example: 
description: "Cats is a wonderful musical that hopes to entertain all!"
review_content: "Very boring watch, the performances in this movie were horrible",
answer: 1

Example:
description: "This documentary provides insight into Napoleon's life"
review_content: "This is a dramatized telling that doesn't depict reality whatsoever."
answer:  2

Example:
description: "Percy Jackson is a fun fantasy adventure!"
review_content: "A very fun movie! I enjoyed it a lot!", 
answer: 5

Here is the actual movie description and review:
"""

PRODUCTS_PROMPT = """Given a product description as context, assign a sentiment score out of 5 to the provided product review. Answer with ONLY a single integer between 1 (bad) and 5 (good). Here are some examples:
Example: 
description: "A mechanical pencil that has a soft grip!"
reviewText: "amazing product! I loved it!"
answer: 5

Example:
description: "BOSE headphones have amazing sound quality!"
reviewText: "bad quality, not very loud"
answer: 1 

Here is the actual description and review:
"""

MOVIES_PROMPT_SHORT = """Given the following fields of a movie description and a user review, assign a sentiment score for the review out of 5. Answer with ONLY a single integer between 1 (bad) and 5 (good): """

PRODUCTS_PROMPT_SHORT = """Given the following fields of a product description and a user review, assign a sentiment score for the review out of 5. Answer with ONLY a single integer between 1 (bad) and 5 (good): """
