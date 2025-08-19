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

SUPPLIERS_QUERY = """
DEFINE
	VAR __DS0Core = 
		SELECTCOLUMNS(
			KEEPFILTERS(
				FILTER(
					KEEPFILTERS(
						SUMMARIZECOLUMNS(
							'УТ_Партнеры'[Партнер.УТ11],
							'УТ_Контактные лица партнеров'[Контактное лицо],
							'УТ_Контактные лица партнеров'[email],
							'УТ_Пользователи'[_description],
							'УТ_Партнеры'[id_1c],
							'УТ_Партнеры'[is_client],
							'УТ_Партнеры'[is_supplier],
							'УТ_Контактные лица партнеров'[Роль],
							'УТ_Контактные лица партнеров'[id_1c],
							"CountRowsУТ_Контактные_лица_партнеров", COUNTROWS('УТ_Контактные лица партнеров')
						)
					),
					OR(
						OR(
							OR(
								OR(
									OR(
										OR(
											OR(
												OR(
													NOT(ISBLANK('УТ_Партнеры'[Партнер.УТ11])),
													NOT(ISBLANK('УТ_Контактные лица партнеров'[Контактное лицо]))
												),
												NOT(ISBLANK('УТ_Контактные лица партнеров'[email]))
											),
											NOT(ISBLANK('УТ_Пользователи'[_description]))
										),
										NOT(ISBLANK('УТ_Партнеры'[id_1c]))
									),
									NOT(ISBLANK('УТ_Партнеры'[is_client]))
								),
								NOT(ISBLANK('УТ_Партнеры'[is_supplier]))
							),
							NOT(ISBLANK('УТ_Контактные лица партнеров'[Роль]))
						),
						NOT(ISBLANK('УТ_Контактные лица партнеров'[id_1c]))
					)
				)
			),
			"'УТ_Партнеры'[Партнер.УТ11]", 'УТ_Партнеры'[Партнер.УТ11],
			"'УТ_Контактные лица партнеров'[Контактное лицо]", 'УТ_Контактные лица партнеров'[Контактное лицо],
			"'УТ_Контактные лица партнеров'[email]", 'УТ_Контактные лица партнеров'[email],
			"'УТ_Пользователи'[_description]", 'УТ_Пользователи'[_description],
			"'УТ_Партнеры'[id_1c]", 'УТ_Партнеры'[id_1c],
			"'УТ_Партнеры'[is_client]", 'УТ_Партнеры'[is_client],
			"'УТ_Партнеры'[is_supplier]", 'УТ_Партнеры'[is_supplier],
			"'УТ_Контактные лица партнеров'[Роль]", 'УТ_Контактные лица партнеров'[Роль],
			"'УТ_Контактные лица партнеров'[id_1c]", 'УТ_Контактные лица партнеров'[id_1c]
		)

	VAR __DS0PrimaryWindowed = 
		TOPN(
			501,
			__DS0Core,
			'УТ_Партнеры'[Партнер.УТ11],
			1,
			'УТ_Контактные лица партнеров'[Контактное лицо],
			1,
			'УТ_Контактные лица партнеров'[email],
			1,
			'УТ_Пользователи'[_description],
			1,
			'УТ_Партнеры'[id_1c],
			1,
			'УТ_Партнеры'[is_client],
			1,
			'УТ_Партнеры'[is_supplier],
			1,
			'УТ_Контактные лица партнеров'[Роль],
			1,
			'УТ_Контактные лица партнеров'[id_1c],
			1
		)

EVALUATE
	__DS0PrimaryWindowed

ORDER BY
	'УТ_Партнеры'[Партнер.УТ11],
	'УТ_Контактные лица партнеров'[Контактное лицо],
	'УТ_Контактные лица партнеров'[email],
	'УТ_Пользователи'[_description],
	'УТ_Партнеры'[id_1c],
	'УТ_Партнеры'[is_client],
	'УТ_Партнеры'[is_supplier],
	'УТ_Контактные лица партнеров'[Роль],
	'УТ_Контактные лица партнеров'[id_1c]
"""
