"""
Raw DAX queries from PowerBI
This file contains the original DAX queries copied from PowerBI.
Each query should be defined as a string variable with a descriptive name.
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

COMPANY_PRODUCTS_QUERY = """
EVALUATE
TOPN(20000, 
    SUMMARIZECOLUMNS(
        'CompanyProducts'[ID],
        'CompanyProducts'[Description],
        'CompanyProducts'[Brand],
        'CompanyProducts'[Category],
        'CompanyProducts'[Withdrawn_from_range],
        'CompanyProducts'[item_number],
        "Product Properties", 
        VAR CurrentProduct = SELECTEDVALUE('УТ_Номенклатура'[Артикул], "No Product Selected")
        RETURN
        CONCATENATEX(
            TOPN(
                20000,
                FILTER(
                    'Char_table',
                    [Артикул] = CurrentProduct
                ),
                [SortOrder]
            ),
            [_description] & ": " & [Значение],
            " | ",
            [SortOrder]
        )
    ),
    'CompanyProducts'[ID]
)
"""