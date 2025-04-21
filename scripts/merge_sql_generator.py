import json
import os
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


def generate_merge_sql_for_pipelines(config_file):
    """
    Generate a precise MERGE SQL transformation based on JSON configuration
    """
    # Load configuration
    with open(config_file, 'r') as f:
        config = json.load(f)

    # Extract source and target details
    source_db = config['Source']['Database']
    source_schema = config['Source']['Schema']
    source_table = config['Source']['Table Name']

    target_schema = config['Target']['Schema']
    target_table = config['Target']['Table Name']

    unique_key = config['Target']['unique_key']

    pre_hook = config['Target']['pre_hook']
    post_hook = config['Target']['post_hook']

    # Column transformation function
    def transform_column_logic(column):
        logic = column['Logic']
        target_col = column['Target Column']

        # Handle special cases
        if logic == "''":
            return f"'' AS {target_col}"
        elif logic == "CAST(CURRENT_USER() AS VARCHAR(200))":
            return f"CAST(CURRENT_USER() AS VARCHAR(200)) AS {target_col}"
        elif logic.startswith("'"):  # Literal string
            return f"{logic} AS {target_col}"
        else:
            return f"source.{logic} AS {target_col}"

    # Generate SELECT columns
    select_columns = [
        transform_column_logic(column) for column in config['Columns']
    ]

    # Prepare column names for INSERT and UPDATE
    all_columns = config['Columns']

    # Generate UPDATE SET clause
    update_set_clauses = [
        f"target.{col['Target Column']} = source.{col['Target Column']}"
        for col in all_columns
        if col['Target Column'] != unique_key
    ]

    # Generate INSERT columns and values
    insert_columns = [col['Target Column'] for col in all_columns]
    insert_values = [f"source.{col['Target Column']}" for col in all_columns]

    merge_sql = fr'''
    {{% macro MAC_{target_schema}_{target_table}_MERGE() %}}
        {{% set query %}}
        {pre_hook}
        MERGE INTO {target_schema}.{target_table} AS target
        USING (
            SELECT 
                {', '.join(select_columns)}
            FROM {source_db}.{source_schema}.{source_table} AS source
        ) AS source
        ON target.{unique_key} = source.{unique_key}
        WHEN MATCHED THEN
            UPDATE SET 
                {', '.join(update_set_clauses)}
        WHEN NOT MATCHED THEN
            INSERT (
                {', '.join(insert_columns)}
            )
            VALUES (
                {', '.join(insert_values)}
            );
        {post_hook}
        {{% endset %}}

        {{% do run_query(query) %}}
    {{% endmacro %}}
    '''
    # Create output directory
    output_dir = 'macros'
    os.makedirs(output_dir, exist_ok=True)

    file_name = f"MAC_{target_schema}_{target_table}_MERGE.sql"
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
        f.write(merge_sql)

    return True, file_path

# Usage
# result, file_path = generate_merge_sql('path/to/your/config.json')
