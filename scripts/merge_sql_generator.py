import json
import os
import sqlparse

from scripts.utils import parse_ddl_file

def merge_sql_generator(config_file, target_ddl_path=None):
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

    # Get target DDL columns and unique keys if path provided
    target_columns = []
    target_unique_keys = []
    if target_ddl_path :
        target_columns, target_unique_keys = parse_ddl_file(target_ddl_path)
        # Convert target_columns to dict for easier lookup
        target_columns_dict = {col[0]: idx for idx, col in enumerate(target_columns)}

    # Use unique keys for the ON condition
    unique_keys = config['Target'].get('unique_key', [])
    if not unique_keys:
        raise ValueError("Unique keys must be provided for MERGE operation.")

    # Build the ON condition
    on_condition = " AND ".join([f"target.{key} = source.{key}" for key in unique_keys])

    # Build the INSERT and UPDATE clauses
    insert_columns = []
    insert_values = []
    update_clauses = []

    for column in config['Columns']:
        target_col = column['Target Column']
        logic = column['Logic']

        # Skip unwanted columns
        if target_col in ["List (Y,N)", "Table Type", "ref"] or "=" in logic:
            continue

        insert_columns.append(target_col)
        insert_values.append(logic)
        update_clauses.append(f"target.{target_col} = source.{target_col}")

    insert_columns_str = ", ".join(insert_columns)
    insert_values_str = ", ".join(insert_values)
    update_clauses_str = ", ".join(update_clauses)

    # Construct the MERGE SQL statement
    merge_sql = f"""
MERGE INTO {target_schema}.{target_table} AS target
USING (
    SELECT {insert_values_str}
    FROM {source_schema}.{source_table} AS source
) AS source
ON {on_condition}
WHEN MATCHED THEN
    UPDATE SET {update_clauses_str}
WHEN NOT MATCHED THEN
    INSERT ({insert_columns_str})
    VALUES ({insert_values_str});
"""
    # Create output directory if it doesn't exist
    output_dir = 'macros'
    os.makedirs(output_dir, exist_ok=True)

    # Generate file name
    macro_name = f"MAC_{config['Target']['Schema']}_{config['Target']['Table Name']}_MERGE"
    file_name = f"{macro_name}.sql"
    file_path = os.path.join(output_dir, file_name)

    # Format the SQL statement using sqlparse
    formatted_merge_sql = sqlparse.format(
        merge_sql,
        reindent_aligned=True,
        indent_tabs=True,
        keyword_case='upper'
    )

    # Write model file
    with open(file_path, 'w') as f:
        f.write(formatted_merge_sql)

    return file_path
