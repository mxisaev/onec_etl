"""
Dataset configurations and DAX queries
"""

# Default dataset ID
DEFAULT_DATASET_ID = "022e7796-b30f-44d4-b076-15331e612d47"

# Dataset configurations
DATASET_CONFIGS = {
    "company_products": {
        "dataset_id": DEFAULT_DATASET_ID,
        "source_table": "CompanyProducts",
        "target_table": "companyproducts",
        "dax_query": """
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
                                    OR(
                                        OR(
                                            OR(
                                                OR(
                                                    OR(
                                                        OR(
                                                            OR(
                                                                NOT(ISBLANK('CompanyProducts'[ID])),
                                                                NOT(ISBLANK('CompanyProducts'[Description]))
                                                            ),
                                                            NOT(ISBLANK('CompanyProducts'[Brand]))
                                                        ),
                                                        NOT(ISBLANK('CompanyProducts'[Category]))
                                                    ),
                                                    NOT(ISBLANK('CompanyProducts'[Withdrawn_from_range]))
                                                ),
                                                NOT(ISBLANK('CompanyProducts'[item_number]))
                                            ),
                                            NOT(ISBLANK('УТ_Товарные категории'[_description]))
                                        ),
                                        NOT(ISBLANK('УТ_РСвДополнительныеСведения2_0'[Под заказ]))
                                    )
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
                    'CompanyProducts'[ID],
                    1,
                    'CompanyProducts'[Description],
                    1,
                    'CompanyProducts'[Brand],
                    1,
                    'CompanyProducts'[Category],
                    1,
                    'CompanyProducts'[Withdrawn_from_range],
                    1,
                    'CompanyProducts'[item_number],
                    1,
                    'УТ_Товарные категории'[_description],
                    1,
                    'УТ_РСвДополнительныеСведения2_0'[Под заказ],
                    1
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
        """,
        "columns": {
            "CompanyProducts[ID]": "id",
            "CompanyProducts[Description]": "description",
            "CompanyProducts[Brand]": "brand",
            "CompanyProducts[Category]": "category",
            "CompanyProducts[Withdrawn_from_range]": "withdrawn_from_range",
            "CompanyProducts[item_number]": "item_number",
            "УТ_Товарные категории[_description]": "product_category",
            "УТ_РСвДополнительныеСведения2_0[Под заказ]": "on_order"
        }
    }
    # Add more dataset configurations here as needed
} 