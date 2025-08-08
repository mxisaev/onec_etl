"""
DAX queries configuration
This file contains all DAX queries used in the ETL process.
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

COMPANY_PRODUCTS_ETL_QUERY = """
DEFINE
    VAR __DS0FilterTable =
        TREATAS({"Смесители"}, 'УТ_Вид номенклатуры'[_description])

    VAR __DS0FilterTable2 =
        TREATAS({"Avrora"}, 'УТ_Марки'[_description])

    VAR __DS0Core =
        FILTER(
            KEEPFILTERS(
                SELECTCOLUMNS(
                    KEEPFILTERS(
                        FILTER(
                            KEEPFILTERS(
                                SUMMARIZECOLUMNS(
                                    'CompanyProducts'[ID],
                                    'CompanyProducts'[Description],
                                    'CompanyProducts'[Brand],
                                    'CompanyProducts'[Category],
                                    'CompanyProducts'[Withdrawn_from_range],
                                    'CompanyProducts'[item_number],
                                    'УТ_Товарные категории'[_description],
                                    'УТ_РСвДополнительныеСведения2_0'[Под заказ],
                                    __DS0FilterTable,
                                    __DS0FilterTable2,
                                    "Выводится_без_остатков", IGNORE('УТ_Номенклатура'[Выводится_без остатков]),
                                    "CountRowsУТ_РСвДополнительныеСведения2_0", COUNTROWS('УТ_РСвДополнительныеСведения2_0')
                                )
                            ),
                            // Здесь больше НЕТ никаких OR — только фильтруем CompanyProducts'[ID]
                            NOT(ISBLANK('CompanyProducts'[ID]))
                        )
                    ),
                    "'CompanyProducts'[ID]", 'CompanyProducts'[ID],
                    "'CompanyProducts'[Description]", 'CompanyProducts'[Description],
                    "'CompanyProducts'[Brand]", 'CompanyProducts'[Brand],
                    "'CompanyProducts'[Category]", 'CompanyProducts'[Category],
                    "'CompanyProducts'[Withdrawn_from_range]", 'CompanyProducts'[Withdrawn_from_range],
                    "'CompanyProducts'[item_number]", 'CompanyProducts'[item_number],
                    "'УТ_Товарные категории'[_description]", 'УТ_Товарные категории'[_description],
                    "'УТ_РСвДополнительныеСведения2_0'[Под заказ]", 'УТ_РСвДополнительныеСведения2_0'[Под заказ],
                    "Выводится_без_остатков", [Выводится_без_остатков]
                )
            ),
            [Выводится_без_остатков] = 0
        )

    VAR __DS0PrimaryWindowed =
        TOPN(
            501,
            __DS0Core,
            'CompanyProducts'[ID], 1,
            'CompanyProducts'[Description], 1,
            'CompanyProducts'[Brand], 1,
            'CompanyProducts'[Category], 1,
            'CompanyProducts'[Withdrawn_from_range], 1,
            'CompanyProducts'[item_number], 1,
            'УТ_Товарные категории'[_description], 1,
            'УТ_РСвДополнительныеСведения2_0'[Под заказ], 1
        )

EVALUATE
    __DS0PrimaryWindowed

ORDER BY
    'CompanyProducts'[ID],
    'CompanyProducts'[Description],
    'CompanyProducts'[Brand],
    'CompanyProducts'[Category],
    'CompanyProducts'[Withdrawn_from_range],
    'CompanyProducts'[item_number],
    'УТ_Товарные категории'[_description],
    'УТ_РСвДополнительныеСведения2_0'[Под заказ]
""" 