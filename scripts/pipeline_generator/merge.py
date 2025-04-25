import  json
import textwrap

from openpyxl.reader.excel import load_workbook

from scripts.pipeline_generator import process_filled_workbook_to_json
from scripts.utils import format_sql_content


def should_exclude_from_minus(logic):
    """Determine if a column should be excluded from MINUS comparison"""
    return (
            logic == "'''" or
            logic.startswith("'") or
            "CURRENT_USER()" in logic or
            "CURRENT_TIMESTAMP()" in logic
    )

def generate_merge_condition(unique_key):
    # Split the unique key into individual columns
    unique_columns = [col.strip() for col in unique_key.split(',')]

    # Generate the ON condition dynamically
    merge_on_condition = ' AND '.join([
        f'target.{col} = source.{col}' for col in unique_columns
    ])

    return merge_on_condition

def transform_column_logic(column):
    """Transform column logic for SELECT statement"""
    logic = column['Logic']
    target_col = column['Target Column']

    # Handle special cases and SQL keywords
    if not logic or logic == '""':
        return f"'' AS {target_col}"

    # Predefined SQL functions and keywords
    sql_keywords = [
        'CURRENT_USER()',
        'CURRENT_TIMESTAMP()',
        'CURRENT_DATE()',
        'CURRENT_TIME()',
        'USER()',
        'SYSTEM_USER()',
        'SESSION_USER()',
        'NULL'
    ]

    # Casting and type conversion functions
    cast_patterns = [
        r'^CAST\(.*\)$',
        r'^CONVERT\(.*\)$'
    ]

    # Check if logic is a complete SQL keyword or function
    if logic in sql_keywords:
        return f"{logic} AS {target_col}"

    # Check for casting functions using regex
    import re
    for pattern in cast_patterns:
        if re.match(pattern, logic, re.IGNORECASE):
            return f"{logic} AS {target_col}"

    # Check for string literals (enclosed in single or double quotes)
    if (logic.startswith("'") and logic.endswith("'")) or \
            (logic.startswith('"') and logic.endswith('"')):
        return f"{logic} AS {target_col}"

    # Check for numeric literals
    try:
        float(logic)
        return f"{logic} AS {target_col}"
    except ValueError:
        pass

    # Handle mathematical and aggregate functions
    math_and_agg_functions = [
        'SUM(', 'AVG(', 'COUNT(', 'MAX(', 'MIN(',
        'ABS(', 'ROUND(', 'FLOOR(', 'CEILING(',
        'SQRT(', 'POWER('
    ]

    for func in math_and_agg_functions:
        if logic.startswith(func) and logic.endswith(')'):
            return f"{logic} AS {target_col}"

    # Default case: assume it's a column from source
    return f"source.{logic} AS {target_col}"


def extract_join_clauses(mapping_sheet, main_table_alias='source'):
    """Extract join clauses from mapping sheet"""
    join_clauses = []
    join_aliases = []
    # Find the JOIN_TABLES section
    join_section_row = None
    for row in range(1, mapping_sheet.max_row + 1):
        if mapping_sheet.cell(row=row, column=1).value == 'JOIN_TABLES':
            join_section_row = row
            break

    if not join_section_row:
        return []

    # Join table headers are in the next row
    join_header_row = join_section_row + 1

    # Process join tables (starting from the row after headers)
    for row in range(join_header_row + 1, mapping_sheet.max_row + 1):
        join_type = mapping_sheet.cell(row=row, column=1).value
        table_type = mapping_sheet.cell(row=row, column=2).value
        source_name = mapping_sheet.cell(row=row, column=3).value
        table_name = mapping_sheet.cell(row=row, column=4).value
        alias = mapping_sheet.cell(row=row, column=5).value
        join_condition = mapping_sheet.cell(row=row, column=6).value

        # Stop when we reach an empty row or a new section
        if not join_type or not table_name:
            next_row = row + 1
            if next_row <= mapping_sheet.max_row:
                next_value = mapping_sheet.cell(row=next_row, column=1).value
                if next_value and next_value in ['WHERE_CONDITIONS', 'GROUP BY']:
                    break
            continue

        # Build the join clause
        join_clause = f"{join_type} JOIN "

        # Add the table reference based on type
        if table_type and table_type.lower() == 'source' and source_name:
            join_clause += f"{{{{ source('{source_name}', '{table_name}') }}}}"
        else:
            join_clause += f"{{{{ ref('{table_name}') }}}}"

        # Add alias if provided
        if alias:
            join_clause += f" AS {alias}"

        # Add join condition
        if join_condition:
            # Replace table aliases if needed
            join_condition = join_condition.replace("main.", f"{main_table_alias}.")

            # Add source alias to column references if not already present
            # This regex finds column names that don't have a table prefix
            import re
            column_pattern = r'(?<![a-zA-Z0-9_\.])([a-zA-Z0-9_]+)(?=\s*=)'

            def add_source_alias(match):
                col = match.group(1)
                # Skip adding alias to literals or functions
                if col.upper() in ['AND', 'OR', 'ON', 'NULL']:
                    return col
                return f"{main_table_alias}.{col}"

            join_condition = re.sub(column_pattern, add_source_alias, join_condition)
            join_clause += f" ON {join_condition}"

        join_clauses.append(join_clause)
        join_aliases = set()
        for row in range(join_header_row + 1, mapping_sheet.max_row + 1):
            alias = mapping_sheet.cell(row=row, column=5).value
            if alias:
                join_aliases.add(alias)

    return join_clauses, join_aliases


def extract_where_condition(mapping_sheet, main_table_alias='source'):
    """Extract WHERE condition from mapping sheet"""
    # Find the WHERE_CONDITIONS section
    where_section_row = None
    for row in range(1, mapping_sheet.max_row + 1):
        if mapping_sheet.cell(row=row, column=1).value == 'WHERE_CONDITIONS':
            where_section_row = row
            break

    if not where_section_row:
        return None

    # Get the condition from the merged cell
    where_condition = mapping_sheet.cell(row=where_section_row, column=2).value

    if where_condition:
        # Replace table aliases if needed
        where_condition = where_condition.replace("main.", f"{main_table_alias}.")
        return where_condition

    return None


def extract_group_by(mapping_sheet, main_table_alias='source'):
    """Extract GROUP BY clause from mapping sheet"""
    # Find the GROUP BY section
    group_by_row = None
    for row in range(1, mapping_sheet.max_row + 1):
        if mapping_sheet.cell(row=row, column=1).value == 'GROUP BY':
            group_by_row = row
            break

    if not group_by_row:
        return None

    # Get the group by columns from the merged cell
    group_by = mapping_sheet.cell(row=group_by_row, column=2).value

    if group_by:
        # Replace table aliases if needed
        group_by = group_by.replace("main.", f"{main_table_alias}.")
        return group_by

    return None


def generate_merge_sql(config_file, mapping_sheet):
    # Load configuration
    global main_sql
    with open(config_file, 'r') as f:
        config = json.load(f)

    # Extract configuration details
    source_details = {
        'db': config['Source']['Database'],
        'schema': config['Source']['Schema'],
        'table': config['Source']['Table Name']
    }

    target_details = {
        'schema': config['Target']['Schema'],
        'table': config['Target']['Table Name'],
        'unique_key': config['Target']['unique_key'],
        'merge_type': config['Target'].get('merge_type', 'update_insert'),
        'minus_logic_required': config['Target'].get('minus_logic_required', False),
        'pre_hook': config['Target'].get('pre_hook', ''),
        'post_hook': config['Target'].get('post_hook', '')
    }

    # Audit and exclusion columns
    audit_columns = [
        "DATA_SRC", "CREATE_DT", "CREATE_BY", "CREATE_PGM",
        "UPDATE_DT", "UPDATE_BY", "UPDATE_PGM"
    ]

    # Validate merge type
    valid_merge_types = ['update_insert', 'update_only', 'insert_only']
    if target_details['merge_type'] not in valid_merge_types:
        raise ValueError(f"Invalid merge_type. Must be one of {valid_merge_types}")


    # Prepare columns - ensure each column is a dictionary
    all_columns = []
    for col in config['Columns']:
        # Ensure each column is a dictionary
        if isinstance(col, dict) and col.get('Logic') != "''":
            all_columns.append(col)

    # Prepare columns for different operations
    select_columns = [transform_column_logic(column) for column in all_columns]
    target_columns = [col['Target Column'] for col in all_columns]

    # Identify columns for MINUS logic if required
    if target_details['minus_logic_required']:
        computed_columns = [
            col for col in all_columns
            if (col['Target Column'] in audit_columns or
                should_exclude_from_minus(col.get('Logic', '')))
        ]

        minus_columns = [
            col['Target Column']  for col in all_columns
            if col not in computed_columns
        ]
        # Start with basic SELECT and FROM
        main_table_alias = 'source'
        minus_logic_sql = textwrap.dedent(f'''
        Select 
            {', '.join([f"{col}" for col in minus_columns])}
        from {source_details['schema']}.{source_details['table']} AS {main_table_alias}
        ''').strip()

        # Add JOIN clauses if mapping_sheet is provided and contains joins
        if mapping_sheet:
            # Assuming these functions are defined elsewhere in your code
            join_clauses, join_aliases = extract_join_clauses(mapping_sheet, main_table_alias)
            if join_clauses:
                minus_logic_sql += "\n\t\t\t" + "\n\t\t\t".join(join_clauses)

            # Add WHERE clause if provided
            where_condition = extract_where_condition(mapping_sheet, main_table_alias)
            if where_condition:
                minus_logic_sql += f"\n\t\t\t WHERE {where_condition}"

            # Add GROUP BY clause if provided
            group_by = extract_group_by(mapping_sheet, main_table_alias)
            if group_by:
                minus_logic_sql += f"\n\t\t\t GROUP BY {group_by}"

            minus_logic_sql += "\n\t\t\t\n" + "MINUS \n\n"
            minus_logic_sql += textwrap.dedent(f'''
            Select 
                {', '.join([f"{col}" for col in minus_columns])}
            from {target_details['schema']}.{target_details['table']}
            ''').strip()

            main_sql = format_sql_content(minus_logic_sql)

    else:
        # Start with basic SELECT and FROM
        main_table_alias = 'source'
        main_sql = textwrap.dedent(f'''
        Select 
            {', '.join([f"{col}" for col in select_columns])}
        from {source_details['schema']}.{source_details['table']} AS {main_table_alias}
        ''').strip()
        # Add JOIN clauses if mapping_sheet is provided and contains joins

        # Add JOIN clauses if mapping_sheet is provided and contains joins
        if mapping_sheet:
            # Assuming these functions are defined elsewhere in your code
            join_clauses, join_aliases = extract_join_clauses(mapping_sheet, main_table_alias)
            if join_clauses:
                main_sql += "\n\t\t\t" + "\n\t\t\t".join(join_clauses)

            # Add WHERE clause if provided
            where_condition = extract_where_condition(mapping_sheet, main_table_alias)
            if where_condition:
                main_sql += f"\n\t\t\t WHERE {where_condition}"

            # Add GROUP BY clause if provided
            group_by = extract_group_by(mapping_sheet, main_table_alias)
            if group_by:
                main_sql += f"\n\t\t\t GROUP BY {group_by}"

    macro_name = f"MAC_{target_details['schema']}_{target_details['table']}_MERGE"
    # Add specific audit column updates
    audit_columns_mapping = [
        f"source.DATA_SRC = '{source_details['schema']}.{source_details['table']}'",
        "source.CREATE_DT = SYSDATE()",
        "source.CREATE_BY = 'SVC_DBT'",
        f"source.CREATE_PGM = '{macro_name}'",
        "source.UPDATE_DT = SYSDATE()",
        "source.UPDATE_BY = 'SVC_DBT'",
        f"source.UPDATE_PGM = '{macro_name}'",
    ]

    update_columns = [
        textwrap.dedent(f"target.{col['Target Column']} = source.{col['Target Column']}")
        for col in all_columns if col['Target Column'] not in audit_columns
    ]
    update_columns.extend(audit_columns_mapping)

    insert_columns = [col['Target Column'] for col in all_columns]
    insert_values = [f"source.{col['Target Column']}" for col in all_columns]

    merge_conditions = []
    # Construct MERGE SQL based on merge type
    if target_details['merge_type'] == 'update_insert':
        # Standard MERGE
        merge_conditions = textwrap.dedent('''
            WHEN MATCHED THEN
                UPDATE SET {update_columns}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
        ''').format(
            update_columns=', '.join(update_columns),
            insert_columns=', '.join(insert_columns),
            insert_values=', '.join(insert_values)
        ).strip().split('\n')

    elif target_details['merge_type'] == 'update_only':
        # Only update existing records
        merge_conditions = textwrap.dedent('''
            WHEN MATCHED THEN
                UPDATE SET {update_columns}
        ''').format(
            update_columns=', '.join(update_columns)
        ).strip().split('\n')

    elif target_details['merge_type'] == 'insert_only':
        # Only insert new records
        merge_conditions = textwrap.dedent('''
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
        ''').format(
            insert_columns=', '.join(insert_columns),
            insert_values=', '.join(insert_values)
        ).strip().split('\n')

    final_merge=''

    merge_on_condition = generate_merge_condition(target_details['unique_key'])

    if target_details['minus_logic_required']:
        merge_sql = textwrap.dedent(f'''
              {target_details['pre_hook']}
              MERGE INTO {target_details['schema']}.{target_details['table']} AS target
              USING (
                
                {main_sql} AS source
                
                )
              ON (
                    {merge_on_condition}
                )
              {""
               "    ".join(merge_conditions)};
              {target_details['post_hook']}
          ''').strip()
    else:
        merge_sql = textwrap.dedent(f'''
              MERGE INTO {target_details['schema']}.{target_details['table']} AS target
              USING (
                
                {main_sql} AS source
                
                )
              ON (
                    {merge_on_condition}
                )
              {""
               "    ".join(merge_conditions)};
              {target_details['post_hook']}
          ''').strip()


    formatted_merge_sql=format_sql_content(merge_sql)



    return formatted_merge_sql

if __name__ == "__main__":
    # Example usage
    config_file = r'C:\Users\shivamgarg4\Documents\Python Work\dbt_model_generator\dbt_model_generator\config\mappings\M01_F_RPT_TBA_Map.json'
    mapping_sheet = r'C:\Users\shivamgarg4\Documents\Python Work\dbt_model_generator\dbt_model_generator\mappings\merge_insert_pipeline_testing_mapping.xlsx'
 # Replace with actual mapping sheet object
    process_filled_workbook_to_json(mapping_sheet)
    workbook = load_workbook(mapping_sheet)
    mapping_sheet=workbook["M01_F_RPT_TBA_Map"]
    result = generate_merge_sql(config_file, mapping_sheet)

    print(result)
