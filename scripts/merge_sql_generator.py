import json
import os
from typing import Dict, List, Any
import textwrap

import sqlparse
from .utils import parse_ddl_file, format_sql_content


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



def merge_sql_generator(config_file, mapping_sheet=None, target_ddl_path=None):
    """Generate a MERGE SQL statement from JSON configuration"""

    with open(config_file) as f:
        config = json.load(f)

    # Extract source information
    source_schema = config['Source']['Schema']
    source_table = config['Source']['Table Name']
    source_name = config['Source']['Name']

    # Extract target information
    target_schema = config['Target']['Schema']
    target_table = config['Target']['Table Name']

    main_table_alias = 'source_table'
    # Generate file name
    macro_name = f"MAC_{config['Target']['Schema']}_{config['Target']['Table Name']}_MERGE"

    # Get target DDL columns and unique keys if path provided
    target_columns = []
    target_unique_keys = []
    if target_ddl_path:
        target_columns, target_unique_keys = parse_ddl_file(target_ddl_path)
        # Convert target_columns to dict for easier lookup
        target_columns_dict = {col[0]: idx for idx, col in enumerate(target_columns)}

    # Use unique keys for the ON condition
    unique_keys = config['Target'].get('unique_key', [])
    if not unique_keys:
        raise ValueError("Unique keys must be provided for MERGE operation.")

    # Build the ON condition using actual column names
    on_condition = " AND ".join([f"target.{key} = source.{key}" for key in unique_keys])

    # Build the INSERT and UPDATE clauses
    insert_columns = []
    insert_values = []
    update_clauses = []

    for column in config['Columns']:
        target_col = column['Target Column']
        logic = str(column['Logic'])

        # Skip unwanted columns
        if target_col in ["List (Y,N)", "Table Type", "ref"] or column['Logic'] == column['Target Column']:
            continue

        insert_columns.append(target_col)
        insert_values.append(logic)
        update_clauses.append(f"target.{target_col} = source.{target_col}")

    insert_columns_str = ", ".join(insert_columns)
    insert_values_str = ", ".join(insert_values)
    update_clauses_str = ", ".join(update_clauses)
    merge_sql = '{% macro'
    merge_sql += f" {macro_name}() "
    merge_sql += '%}\n'
    merge_sql += "    {% set query %}\n"

    model_content = f"""
        SELECT {insert_values_str}
        FROM {source_schema}.{source_table} AS source
    """
    # Add JOIN clauses if mapping_sheet is provided and contains joins
    if mapping_sheet:
        join_clauses, join_aliases = extract_join_clauses(mapping_sheet, main_table_alias)
        if join_clauses:
            model_content += "\n\t\t\t" + "\n\t\t\t".join(join_clauses)

        # Add WHERE clause if provided
    where_condition = extract_where_condition(mapping_sheet, main_table_alias)
    if where_condition:
        model_content += f"\n\t\t\tWHERE {where_condition}"

    # Add GROUP BY clause if provided
    group_by = extract_group_by(mapping_sheet, main_table_alias)
    if group_by:
        model_content += f"\n\t\t\tGROUP BY {group_by}"

        # Format the SQL statement using sqlparse
    formatted_model_content = sqlparse.format(
        model_content,
        reindent_aligned=True,
        indent_tabs=True,
        keyword_case='upper'
    )

    # Construct the MERGE SQL statement
    final_merge = f"""
    MERGE INTO {target_schema}.{target_table} AS target
        USING (
            {formatted_model_content}
        ) AS source
        ON {on_condition}
        WHEN MATCHED THEN
            UPDATE SET {update_clauses_str}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns_str})
            VALUES ({insert_values_str});\n
"""
    # Format the SQL statement using sqlparse
    formatted_final_merge = sqlparse.format(
        final_merge,
        reindent_aligned=True,
        indent_tabs=True,
        keyword_case='upper'
    )

    merge_sql += formatted_final_merge
    merge_sql += "\n\t{% endset %}\n"
    merge_sql += "  {% set results = run_query(query) %}  \n"
    merge_sql += "{% endmacro %}"
    # Create output directory if it doesn't exist
    output_dir = 'macros'
    os.makedirs(output_dir, exist_ok=True)

    file_name = f"{macro_name}.sql"
    file_path = os.path.join(output_dir, file_name)

    formatted_model_content = format_sql_content(merge_sql)

    # Write model file
    with open(file_path, 'w') as f:
        f.write(formatted_model_content)

    return True, file_path


def _generate_where_conditions(config):
    """Generate WHERE conditions if present"""
    where_conditions = config.get('where_conditions', [])
    if where_conditions:
        return "WHERE " + " AND ".join(where_conditions)
    return ""


def _generate_group_by(config):
    """Generate GROUP BY clause if present"""
    group_by = config.get('group_by', [])
    if group_by:
        return "GROUP BY " + ", ".join(group_by)
    return ""

def build_source_data_cte(source_details,
                          select_columns,
                          mapping_sheet):
    # Start with basic SELECT and FROM
    main_table_alias = 'source'

    # Use textwrap.dedent for clean multiline string
    model_content = textwrap.dedent(f'''
        WITH source_data AS (
            SELECT 
                {', '.join(select_columns)}
            FROM {source_details['schema']}.{source_details['table']} AS {main_table_alias}
    ''').strip()

    # Add JOIN clauses if mapping_sheet is provided and contains joins
    if mapping_sheet:
        # Assuming these functions are defined elsewhere in your code
        join_clauses, join_aliases = extract_join_clauses(mapping_sheet, main_table_alias)
        if join_clauses:
            model_content += "\n\t\t\t" + "\n\t\t\t".join(join_clauses)

        # Add WHERE clause if provided
        where_condition = extract_where_condition(mapping_sheet, main_table_alias)
        if where_condition:
            model_content += f"\n\t\t\t WHERE {where_condition}"

        # Add GROUP BY clause if provided
        group_by = extract_group_by(mapping_sheet, main_table_alias)
        if group_by:
            model_content += f"\n\t\t\t GROUP BY {group_by}"

    # Close the CTE
    model_content += "\n    )"

    return model_content



def should_exclude_from_minus(logic):
    """Determine if a column should be excluded from MINUS comparison"""
    return (
            logic == "'''" or
            logic.startswith("'") or
            "CURRENT_USER()" in logic or
            "CURRENT_TIMESTAMP()" in logic
    )

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


def generate_merge_sql_for_pipelines(config_file, mapping_sheet,object_name=None):

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
        'post_hook': config['Target'].get('post_hook', ''),
        'having': config.get('having', [''])[0] if config.get('having') else ''
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

    # Identify columns for MINUS logic if required
    if target_details['minus_logic_required']:
        computed_columns = [
            col for col in all_columns
            if (col['Target Column'] in audit_columns or
                should_exclude_from_minus(col.get('Logic', '')))
        ]

        minus_columns = [
            col['Target Column'] for col in all_columns
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

            # Add HAVING clause if provided
            having = config['Target'].get('having', '')
            if having:
                minus_logic_sql += f"\n\t\t\t HAVING {having}"

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

            if target_details['having']:
                main_sql += f"\n\t\t\t HAVING {target_details['having']}"


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

    final_merge = ''

    merge_on_condition = generate_merge_condition(target_details['unique_key'])

    if target_details['minus_logic_required']:
        merge_sql = textwrap.dedent(f'''
              {target_details['pre_hook']}
              MERGE INTO {target_details['schema']}.{target_details['table']} AS target
              USING (

                {main_sql}

                ) AS source
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

                {main_sql}

                ) AS source
              ON (
                    {merge_on_condition}
                )
              {""
               "    ".join(merge_conditions)};
              {target_details['post_hook']}
          ''').strip()

    formatted_merge_sql = format_sql_content(merge_sql)

    dbt_enclosed_merge= textwrap.dedent(f'''
     {{% macro MAC_{target_details['schema']}_{target_details['table']}_MERGE() %}}
    {{% set query %}}
    
    {formatted_merge_sql}
    
    {{% endset %}}
    {{% do run_query(query) %}}
    {{% endmacro %}} 
''').strip()
    # File writing logic
    output_dir = 'macros'
    os.makedirs(output_dir, exist_ok=True)



    macro_name = f"MAC_{target_details['schema']}_{target_details['table']}_MERGE.sql"

    if object_name:
        model_name = f"{object_name}"
    else:
        model_name = macro_name

    file_name = f"{model_name}.sql"
    file_path = os.path.join(output_dir, file_name)


    # Ensure unique file name if file already exists
    if os.path.exists(file_path):
        base_name, ext = os.path.splitext(file_name)
        counter = 1
        while os.path.exists(file_path):
            file_name = f"{base_name}_{counter}{ext}"
            file_path = os.path.join(output_dir, file_name)
            counter += 1

    # Write to file
    with open(file_path, 'w') as f:
        f.write(dbt_enclosed_merge)

    return True, file_path


def generate_merge_condition(unique_key):
    # Split the unique key into individual columns
    unique_columns = [col.strip() for col in unique_key.split(',')]

    # Generate the ON condition dynamically
    merge_on_condition = ' AND '.join([
        f'target.{col} = source.{col}' for col in unique_columns
    ])

    return merge_on_condition
