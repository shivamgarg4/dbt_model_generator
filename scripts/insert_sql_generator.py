import json
import os
import sqlparse

from scripts.utils import parse_ddl_file

def insert_sql_generator(config_file, target_ddl_path=None):
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

    # Get target DDL columns if path provided
    target_columns = []
    if target_ddl_path:
        target_columns, _ = parse_ddl_file(target_ddl_path)
        # Convert target_columns to dict for easier lookup
        target_columns_dict = {col[0]: idx for idx, col in enumerate(target_columns)}

    # Build the INSERT clause
    insert_columns = []
    insert_values = []

    for column in config['Columns']:
        target_col = column['Target Column']
        logic = column['Logic']

        # Skip unwanted columns
        if target_col in ["List (Y,N)", "Table Type", "ref"] or "=" in logic:
            continue

        insert_columns.append(target_col)
        insert_values.append(logic)

    insert_columns_str = ", ".join(insert_columns)
    insert_values_str = ", ".join(insert_values)

    # Construct the INSERT SQL statement
    insert_sql = f"""
INSERT INTO {target_schema}.{target_table} ({insert_columns_str})
SELECT {insert_values_str}
FROM {source_schema}.{source_table};
"""

    # Create output directory if it doesn't exist
    output_dir = 'macros'
    os.makedirs(output_dir, exist_ok=True)

    # Generate file name
    macro_name = f"MAC_{config['Target']['Schema']}_{config['Target']['Table Name']}_INSERT"
    file_name = f"{macro_name}.sql"
    file_path = os.path.join(output_dir, file_name)

    # Format the SQL statement using sqlparse
    formatted_insert_sql = sqlparse.format(
        insert_sql
    )

    # Write model file
    with open(file_path, 'w') as f:
        f.write(formatted_insert_sql)

    return file_path