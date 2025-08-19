"""
Raw DAX queries template
This file contains the original DAX queries copied from PowerBI.
Customize it for your specific project.
"""

# Example query structure:
# QUERY_NAME = """
#     EVALUATE
#     SUMMARIZECOLUMNS(
#         'Table'[Column],
#         "Measure", [Measure Name]
#     )
# """

# Add your DAX queries below:

YOUR_DAX_QUERY_NAME = """  # CHANGE THIS
EVALUATE
SUMMARIZECOLUMNS(
    'YourTable'[Column1],      -- CHANGE THIS
    'YourTable'[Column2],      -- CHANGE THIS
    'YourTable'[Column3],      -- CHANGE THIS
    -- Add more columns as needed
    
    -- Optional: Add measures
    "Total Count", COUNTROWS('YourTable')
)
"""
