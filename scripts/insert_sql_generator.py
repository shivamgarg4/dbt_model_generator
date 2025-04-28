import json
import os
from scripts.utils import parse_ddl_file,format_sql_content

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

        if table_type == 'table':
            join_clause += f" {source_name}.{table_name} "
        elif table_type and table_type.lower() == 'source' and source_name:
            join_clause += f"{{{{ source('{source_name}', '{table_name}') }}}}"
        elif table_type and table_type.lower() == 'ref' and table_name:
           join_clause += f"{{{{ ref('{table_name}') }}}}"
        else:
            join_clause += f" {source_name}.{table_name} "

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

def insert_sql_generator(config_file,mapping_sheet=None,target_ddl_path=None,object_name=None):
    """Generate an INSERT SQL statement from JSON configuration"""

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
    pre_hook = config['Target']['pre_hook']
    post_hook = config['Target']['post_hook']
    having= config.get('having', [''])[0] if config.get('having') else ''

    if object_name:
        macro_name = f"{object_name}"
    else:
        macro_name = f"MAC_{config['Target']['Schema']}_{config['Target']['Table Name']}_INSERT"
    # Generate file name

    # Get target DDL columns if path provided
    target_columns = []
    if target_ddl_path:
        target_columns, _ = parse_ddl_file(target_ddl_path)
        # Convert target_columns to dict for easier lookup
        target_columns_dict = {col[0]: idx for idx, col in enumerate(target_columns)}

    # Build the INSERT clause
    insert_columns = []
    insert_values = []
    # Add specific audit column updates
    # Audit columns mapping with dynamic logic
    audit_columns_mapping = {
        'DATA_SRC': f"'{source_schema}.{source_table}'",
        'CREATE_DT': 'SYSDATE()',
        'CREATE_BY': "'SVC_DBT'",
        'CREATE_PGM': f"'{macro_name}'",
        'UPDATE_DT': 'SYSDATE()',
        'UPDATE_BY': "'SVC_DBT'",
        'UPDATE_PGM': f"'{macro_name}'"
    }


    for column in config['Columns']:
        if isinstance(column, dict) and column.get('Logic') != "''":
            target_col = column['Target Column']
            logic = str(column['Logic'])

            # Skip unwanted columns
            if target_col in ["List (Y,N)", "Table Type", "ref"]:
                continue

            # Check if the column is an audit column
            if target_col in audit_columns_mapping:
                # Use predefined audit column logic
                insert_columns.append(target_col)
                insert_values.append(audit_columns_mapping[target_col])
            else:
                # Use the original logic for non-audit columns
                logic = str(column['Logic'])
                insert_columns.append(target_col)
                insert_values.append(logic)


    insert_columns_str = ", ".join(insert_columns)
    insert_values_str = ", ".join(insert_values)
    model_content = '{% macro'
    model_content+=f" {macro_name}()"
    model_content+='%}\n'
    model_content+="    {% set query %}"
    # Construct the INSERT SQL statement
    model_content += f"""
    {pre_hook}\n
    -- Insert data into {target_schema}.{target_table}
    INSERT INTO {target_schema}.{target_table} ({insert_columns_str})
    SELECT {insert_values_str}
    FROM {source_schema}.{source_table} as {main_table_alias}\n
    {post_hook}
"""

    # Add JOIN clauses if mapping_sheet is provided and contains joins
    if mapping_sheet:
        join_clauses, join_aliases = extract_join_clauses(mapping_sheet, main_table_alias)
        if join_clauses:
            model_content += "\n\t" + "\n\t".join(join_clauses)

        # Add WHERE clause if provided
    where_condition = extract_where_condition(mapping_sheet, main_table_alias)
    if where_condition:
        model_content += f"\n\tWHERE {where_condition}"

    # Add GROUP BY clause if provided
    group_by = extract_group_by(mapping_sheet, main_table_alias)
    if group_by:
        model_content += f"\n\tGROUP BY {group_by}"

    if having:
        model_content += f"\n\tHAVING {having}"

    model_content+=";\n"

    model_content += "    {% endset %}\n"
    model_content += "    {% set results = run_query(query) %}  \n"
    model_content+="{% endmacro %}"
    # Create output directory if it doesn't exist
    output_dir = 'macros'
    os.makedirs(output_dir, exist_ok=True)

    if object_name:
        model_name = f"{object_name}"
    else:
        model_name = macro_name

    file_name = f"{model_name}.sql"
    file_path = os.path.join(output_dir, file_name)

    # Format the SQL statement using sqlparse
    formatted_insert_sql = format_sql_content(model_content)

    # Ensure unique file name if file already exists
    if os.path.exists(file_path):
        base_name, ext = os.path.splitext(file_name)
        counter = 1
        while os.path.exists(file_path):
            file_name = f"{base_name}_{counter}{ext}"
            file_path = os.path.join(output_dir, file_name)
            counter += 1

    # Write model file
    with open(file_path, 'w') as f:
        f.write(formatted_insert_sql)

    return True,file_path