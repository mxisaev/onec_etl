import re

def normalize_column_name(col):
    """Нормализует имя колонки: нижний регистр, подчёркивания вместо пробелов и дефисов, убирает спецсимволы."""
    normalized = col.lower().replace(' ', '_').replace('-', '_')
    normalized = ''.join(c for c in normalized if c.isalnum() or c == '_')
    return normalized


def get_business_columns_from_dax(dax_query):
    """
    Извлекает имена бизнес-колонок из DAX-запроса (только из квадратных скобок SUMMARIZECOLUMNS), с нормализацией.
    Возвращает список нормализованных имён колонок.
    
    Поддерживает:
    - Колонки в формате 'Table'[Column]
    - Вычисляемые поля в двойных кавычках "Field Name"
    - Автоматически убирает дубликаты, сохраняя порядок первого появления
    - Исключает VAR-блоки и строковые литералы
    """
    # Находим SUMMARIZECOLUMNS
    match = re.search(r'SUMMARIZECOLUMNS\((.*?)\)', dax_query, re.DOTALL)
    if not match:
        raise ValueError("Не удалось найти SUMMARIZECOLUMNS в DAX-запросе!")
    
    args = match.group(1)
    
    # Более точное удаление VAR-блоков
    # Ищем VAR ... RETURN ... и удаляем весь блок до следующей запятой или конца
    def remove_var_blocks(text):
        # Разбиваем на части по запятой
        parts = []
        current_part = ""
        paren_count = 0
        
        for char in text:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0:
                parts.append(current_part.strip())
                current_part = ""
                continue
            current_part += char
        
        if current_part.strip():
            parts.append(current_part.strip())
        
        # Фильтруем части, исключая VAR-блоки
        filtered_parts = []
        for part in parts:
            if not part.strip().startswith('VAR '):
                filtered_parts.append(part)
        
        return ', '.join(filtered_parts)
    
    args_clean = remove_var_blocks(args)
    
    columns = []
    
    # Ищем все токены в формате 'Table'[Column] - только из основной части
    table_column_pattern = r"'[^']+'\[([^\]]+)\]"
    table_columns = re.findall(table_column_pattern, args_clean)
    columns.extend(table_columns)
    
    # Ищем вычисляемые поля в двойных кавычках, но исключаем строковые литералы
    computed_pattern = r'"([^"]+)"'
    computed_fields = re.findall(computed_pattern, args_clean)
    
    # Фильтруем: оставляем только те, что выглядят как имена полей
    for field in computed_fields:
        field_clean = field.strip()
        # Исключаем строковые литералы и служебные строки
        if (field_clean and 
            not field_clean.isupper() and  # Не все заглавные
            not field_clean.startswith('No ') and  # Не служебные сообщения
            not field_clean.startswith('Error') and
            len(field_clean) > 1):  # Не одиночные символы
            columns.append(field_clean)
    
    # Убираем дубликаты, сохраняя порядок первого появления
    seen = set()
    unique_columns = []
    for col in columns:
        if col not in seen:
            seen.add(col)
            unique_columns.append(col)
    
    return [normalize_column_name(col) for col in unique_columns] 