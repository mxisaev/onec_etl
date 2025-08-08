"""
DAX query mappings configuration
This file contains mappings between DAX queries and database columns.
"""

DAX_QUERIES = {
    'COMPANY_PRODUCTS_ETL_QUERY': """
DEFINE VAR __DS0FilterTable = TREATAS({"Смесители"}, 'УТ_Вид номенклатуры'[_description]) 
VAR __DS0FilterTable2 = TREATAS({"Avrora"}, 'УТ_Марки'[_description]) 
VAR __DS0Core = FILTER( 
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
                            "Выводится_без_остатков", 
                            IGNORE('УТ_Номенклатура'[Выводится_без остатков]), 
                            "CountRowsУТ_РСвДополнительныеСведения2_0", 
                            COUNTROWS('УТ_РСвДополнительныеСведения2_0') 
                        ) 
                    ), 
                    AND(
                        NOT(ISBLANK('CompanyProducts'[ID])),
                        OR( 
                            OR( 
                                OR( 
                                    OR( 
                                        OR( 
                                            OR( 
                                                NOT(ISBLANK('CompanyProducts'[Description])), 
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
VAR __DS0PrimaryWindowed = TOPN( 
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
EVALUATE __DS0PrimaryWindowed 
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
    'company_products_etl_query': """
    EVALUATE
    SUMMARIZECOLUMNS(
        'CompanyProducts'[ID],
        'CompanyProducts'[Description],
        'CompanyProducts'[Brand],
        'CompanyProducts'[Category],
        'CompanyProducts'[Withdrawn_from_range],
        'CompanyProducts'[item_number],
        'УТ_Товарные категории'[_description],
        'УТ_РСвДополнительныеСведения2_0'[Под заказ],
        'Выводится_без_остатков',
        COUNTROWS('УТ_РСвДополнительныеСведения2_0')
    )
    """
}

DAX_MAPPINGS = {
    'company_products': {
        'query': DAX_QUERIES['COMPANY_PRODUCTS_ETL_QUERY'],
        'columns': {
            'CompanyProducts[ID]': 'id',
            'CompanyProducts[Description]': 'description',
            'CompanyProducts[Brand]': 'brand',
            'CompanyProducts[Category]': 'category',
            'CompanyProducts[Withdrawn_from_range]': 'withdrawn_from_range',
            'CompanyProducts[item_number]': 'item_number',
            'УТ_Товарные категории[_description]': 'product_category',
            'УТ_РСвДополнительныеСведения2_0[Под заказ]': 'on_order',
            'Выводится_без_остатков': 'is_vector',
            'CountRowsУТ_РСвДополнительныеСведения2_0': 'count_rows',
            'Product Properties': 'product_properties'
        },
        'table_template': 'company_products_template'
    }
}

def get_dax_mapping(mapping_name: str) -> dict:
    """
    Get DAX mapping configuration by name
    
    Args:
        mapping_name (str): Name of the mapping to retrieve
        
    Returns:
        dict: Mapping configuration
        
    Raises:
        KeyError: If mapping not found
    """
    if mapping_name not in DAX_MAPPINGS:
        raise KeyError(f"DAX mapping '{mapping_name}' not found")
    return DAX_MAPPINGS[mapping_name] 