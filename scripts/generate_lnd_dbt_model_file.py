import json
import sqlparse
from openpyxl import load_workbook
import snowflake.connector
import openpyxl.worksheet.datavalidation
import os
from   .model_mapper  import ModelMapper
from .dbt_model_generator import extract_join_clauses, extract_where_condition

def format_columns(columns):
    """Format columns for the dbt model."""
    try:
        max_length = max(len(column[0]) for column in columns)
        return [f"    {column[0].ljust(max_length)}" for column in columns]
    except Exception as e:
        raise Exception(f"An error occurred while formatting columns: {e}")

def replace_audit_columns(columns, source_schema_name, source_view_name, target_schema_name, target_table_name):
    """Replace audit columns with static values."""
    try:
        audit_columns = {
            "DATA_SRC": f"'{source_schema_name}.{source_view_name}' AS DATA_SRC",
            "CREATE_DT": "CURRENT_TIMESTAMP() AS CREATE_DT",
            "CREATE_BY": "CAST(CURRENT_USER() AS VARCHAR(200)) AS CREATE_BY",
            "CREATE_PGM": f"'{target_schema_name}.{target_table_name}' AS CREATE_PGM",
            "UPDATE_DT": "CURRENT_TIMESTAMP() AS UPDATE_DT",
            "UPDATE_BY": "CAST(CURRENT_USER() AS VARCHAR(200)) AS UPDATE_BY",
            "UPDATE_PGM": f"'{target_schema_name}.{target_table_name}' AS UPDATE_PGM"
        }
        return [f"    {audit_columns.get(column.split()[0].strip(), column)}" for column in columns]
    except Exception as e:
        raise Exception(f"An error occurred while replacing audit columns: {e}")

def generate_lnd_dbt_model_file(config, file_path):
    
    """Generate the dbt model file."""
    try:
        with open(config) as f:
            config = json.load(f)

        # Extract source information
        source_name = config['Source']['Name']


        workbook = load_workbook(file_path)
        mapping_sheet, config_sheet = workbook['Mapping'], workbook['Config']

        """Extract target and source table information from mapping sheet"""
        target_table = None
        source_table = None

        for row in range(1, mapping_sheet.max_row + 1):
            cell_value = mapping_sheet.cell(row=row, column=1).value
            if cell_value == 'TARGET_TABLE':
                target_table = mapping_sheet.cell(row=row, column=2).value
            elif cell_value == 'SOURCE_TABLE':
                source_table = mapping_sheet.cell(row=row, column=2).value
            if target_table and source_table:
                break

        if not source_table:
            raise ValueError("Source table not found in mapping sheet")


        source_db, source_schema, source_table_name = source_table.split('.')
        target_parts = target_table.split('.')
        target_schema = target_parts[0] if len(target_parts) > 1 else None
        target_table_name = target_parts[-1]

        """Extract Snowflake configuration from Config sheet"""
        snowflake_config = {}
        in_snowflake_section = False

        for row in range(2, config_sheet.max_row + 1):
            param = config_sheet.cell(row=row, column=1).value
            if not param:
                continue
            if param == 'Snowflake Configuration':
                in_snowflake_section = True
                continue
            if in_snowflake_section and param in ['ROLE', 'WAREHOUSE', 'DATABASE', 'ACCOUNT', 'USER', 'AUTHENTICATOR']:
                value = config_sheet.cell(row=row, column=2).value
                if value:
                    snowflake_config[param] = value

        """Get column information from Snowflake"""
        conn = None
        cursor = None
        conn = snowflake.connector.connect(
            account=snowflake_config['ACCOUNT'],
            user=snowflake_config['USER'],
            authenticator=snowflake_config['AUTHENTICATOR'],
            warehouse=snowflake_config['WAREHOUSE'],
            role=snowflake_config['ROLE'],
            database=source_db,
            schema=source_schema
        )

        print(conn)

        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM {source_db}.INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = '{source_schema}'
            AND TABLE_NAME = '{source_table_name}'
            ORDER BY ORDINAL_POSITION
        """)

        columns = cursor.fetchall()
        formatted_columns = format_columns(columns)
        replaced_columns = replace_audit_columns(formatted_columns, source_schema, source_table_name, target_schema, target_table_name)
        model_config = f"""
        {{
            {{
                config(
                    schema = '{target_schema}',
                    tags=['{target_table_name}'],
                    alias='{target_table_name}',
                    materialized='table',
                    transient = false
                )
            }}
        }}
        -- Model: {target_schema}.{target_table_name}
        -- Source: {source_db}.{source_schema}.{source_table_name}
        """

        model_config += "\n\nSELECT\n"
        model_config += ",\n".join(replaced_columns) + "\n"
        model_config += f"FROM {{{{ source('{source_name}', '{source_table_name}') }}}}\n"

        # Create output directory if it doesn't exist
        output_dir = 'models'
        os.makedirs(output_dir, exist_ok=True)
    
        # Generate file name
        model_name = f"{target_schema}.{target_table_name}"
        file_name = f"{model_name}.sql"
        file_path = os.path.join(output_dir, file_name)

        # Write model file
        with open(file_path, 'w') as f:
            f.write(model_config)

        return True,file_path
    except Exception as e:
        raise Exception(f"An error occurred while generating the dbt model: {e}")

def get_snowflake_connection(config):
    """
    Create a Snowflake connection using the provided configuration
    
    Args:
        config (dict): Dictionary containing Snowflake connection parameters
            Required keys: account, user, password, warehouse, role, database, schema
            
    Returns:
        snowflake.connector.SnowflakeConnection: Snowflake connection object
    """
    try:
        conn = snowflake.connector.connect(
            account=config['account'],
            user=config['user'],
            password=config['password'],
            warehouse=config['warehouse'],
            role=config['role'],
            database=config['database'],
            schema=config['schema']
        )
        return conn
    except Exception as e:
        raise Exception(f"Failed to connect to Snowflake: {str(e)}")

def create_dp_view_file(config, file_path):
    """Generate the dbt model file."""
    try:
        with open(config) as f:
            config = json.load(f)

        # Extract source information
        source_name = config['Source']['Name']

        workbook = load_workbook(file_path)
        mapping_sheet, config_sheet = workbook['Mapping'], workbook['Config']

        """Extract target and source table information from mapping sheet"""
        target_table = None
        source_table = None

        for row in range(1, mapping_sheet.max_row + 1):
            cell_value = mapping_sheet.cell(row=row, column=1).value
            if cell_value == 'TARGET_TABLE':
                target_table = mapping_sheet.cell(row=row, column=2).value
            elif cell_value == 'SOURCE_TABLE':
                source_table = mapping_sheet.cell(row=row, column=2).value
            if target_table and source_table:
                break

        if not source_table:
            raise ValueError("Source table not found in mapping sheet")

        source_db, source_schema, source_table_name = source_table.split('.')
        target_parts = target_table.split('.')
        target_schema = target_parts[0] if len(target_parts) > 1 else None
        target_table_name = target_parts[-1]

        """Extract Snowflake configuration from Config sheet"""
        snowflake_config = {}
        in_snowflake_section = False

        for row in range(2, config_sheet.max_row + 1):
            param = config_sheet.cell(row=row, column=1).value
            if not param:
                continue
            if param == 'Snowflake Configuration':
                in_snowflake_section = True
                continue
            if in_snowflake_section and param in ['ROLE', 'WAREHOUSE', 'DATABASE', 'ACCOUNT', 'USER', 'AUTHENTICATOR']:
                value = config_sheet.cell(row=row, column=2).value
                if value:
                    snowflake_config[param] = value

        """Get column information from Snowflake"""
        conn = None
        cursor = None
        conn = snowflake.connector.connect(
            account=snowflake_config['ACCOUNT'],
            user=snowflake_config['USER'],
            authenticator=snowflake_config['AUTHENTICATOR'],
            warehouse=snowflake_config['WAREHOUSE'],
            role=snowflake_config['ROLE'],
            database=source_db,
            schema=source_schema
        )

        print(conn)

        cursor = conn.cursor()
        cursor.execute(f"""
               SELECT COLUMN_NAME
               FROM {source_db}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{source_schema}'
               AND TABLE_NAME = '{source_table_name}'
               ORDER BY ORDINAL_POSITION
           """)

        columns = cursor.fetchall()
        cursor.execute(f"""
               SELECT COLUMN_NAME
               FROM {source_db}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{target_schema}'
               AND TABLE_NAME = '{target_table_name}'
               ORDER BY ORDINAL_POSITION
           """)
        target_columns = cursor.fetchall()
        source_formatted_columns = format_columns(columns)
        source_replaced_columns = replace_audit_columns(source_formatted_columns, source_schema, source_table_name, target_schema,
                                                 target_table_name)
        target_formatted_columns = format_columns(target_columns)
        model_config = f"""
           -- Model: {target_schema}.{target_table_name}
           -- Source: {source_db}.{source_schema}.{source_table_name}
           """

        model_config += f"\n\ncreate or replace view {target_schema}.{target_table_name}(\n"
        model_config += ",\n".join(target_formatted_columns) + "\n)copy grants as\n"
        model_config += "\n\nSELECT\n"
        model_config += ",\n".join(source_replaced_columns) + "\n"
        model_config += f"FROM {source_schema}.{source_table_name}\n"

        # Create output directory if it doesn't exist
        output_dir = 'views'
        os.makedirs(output_dir, exist_ok=True)

        # Generate file name
        model_name = f"{target_schema}.{target_table_name}"
        file_name = f"{model_name}.sql"
        file_path = os.path.join(output_dir, file_name)

        # Write model file
        with open(file_path, 'w') as f:
            f.write(model_config)

        return True, file_path
    except Exception as e:
        raise Exception(f"An error occurred while generating the dbt model: {e}")

def create_test_model_file(json_file, mapping_sheet_path, test_model_output_path):
    """
    Generate a comprehensive dbt test model file based on JSON configuration and mapping sheet.

    Args:
        json_file (str): Path to the JSON configuration file
        mapping_sheet_path (str): Path to the Excel mapping sheet
        test_model_output_path (str): Directory to save the test model file

    Returns:
        tuple: (success_status, file_path)
    """
    try:
        # Load configuration from JSON file
        with open(json_file, 'r') as f:
            config = json.load(f)

        # Load mapping sheet
        mapping_sheet = openpyxl.load_workbook(mapping_sheet_path).active

        # Extract configuration details
        source_config = config.get('Source', {})
        target_config = config.get('Target', {})
        columns_config = config.get('Columns', [])

        # Extract joins and where conditions
        join_clauses, join_aliases = extract_join_clauses(mapping_sheet)
        where_condition = extract_where_condition(mapping_sheet)

        # Construct source and target table fully qualified names
        source_database = source_config.get('Database', '')
        source_schema = source_config.get('Schema', '')
        source_table_name = source_config.get('Table Name', '')
        full_source_table = f"{source_database}.{source_schema}.{source_table_name}"

        target_schema = target_config.get('Schema', '')
        target_table_name = target_config.get('Table Name', '')
        full_target_table = f"{target_schema}.{target_table_name}"

        # Construct source CTE with joins and where condition
        source_cte = f"(SELECT * FROM {full_source_table}"

        # Add joins
        for join_clause in join_clauses:
            source_cte += f" {join_clause}"

        # Add where condition
        if where_condition:
            source_cte += f" WHERE {where_condition}"

        source_cte += ")"

        # Identify key columns and audit columns
        key_columns = target_config.get('unique_key', [])
        audit_columns = [
            'DATA_SRC', 'CREATE_DT', 'CREATE_BY', 'CREATE_PGM',
            'UPDATE_DT', 'UPDATE_BY', 'UPDATE_PGM'
        ]

        # Process column mappings
        column_mappings = {}
        source_columns = []
        constant_columns = []

        for col in columns_config:
            target_col = col.get('Target Column', '')
            source_table = col.get('Source Table', '')
            logic = col.get('Logic', '')

            # Skip audit columns
            if target_col in audit_columns:
                continue

            # Categorize columns
            if source_table:
                source_columns.append(target_col)
                column_mappings[target_col] = {
                    'source_table': source_table,
                    'source_column': logic or target_col
                }
            elif logic:
                constant_columns.append(target_col)
                column_mappings[target_col] = {
                    'source_table': None,
                    'constant_value': logic
                }

        # Create output directory
        os.makedirs(test_model_output_path, exist_ok=True)

        # Generate file name
        file_name = f"{full_target_table}_test.sql"
        file_path = os.path.join(test_model_output_path, file_name)

        # Define test queries
        queries = {
            "Record Count Comparison": f"""
WITH 
    target_count AS (
        SELECT COUNT(*) AS target_count 
        FROM {full_target_table}
    ), 
    source_count AS (
        SELECT COUNT(*) AS source_count 
        FROM {source_cte}
    )
SELECT 
    'Record Count' AS comparison, 
    target_count, 
    source_count, 
    (target_count - source_count) AS count_difference
FROM 
    target_count, 
    source_count;
""",
            "Mapped Columns Validation": f"""
WITH target_data AS (
    SELECT 
        {', '.join([
                f"{col} AS {col}_target"
                for col in source_columns
            ])}
    FROM {full_target_table}
),
source_data AS (
    SELECT 
        {', '.join([
                f"{column_mappings[col]['source_column']} AS {col}_source"
                for col in source_columns
            ])}
    FROM {source_cte}
)
SELECT 
    'Mapped Columns Validation' AS validation_type,
    {', '.join([
                f"{col}_target, {col}_source"
                for col in source_columns
            ])}
FROM 
    target_data
FULL OUTER JOIN 
    source_data
ON 
    1=1;
""",
            "Constant Value Columns": f"""
SELECT 
    'Constant Value Columns' AS validation_type,
    {', '.join([
                f"{column_mappings[col]['constant_value']} AS {col}_value"
                for col in constant_columns
            ])}
FROM 
    {full_target_table};
""",
            "MINUS Validation": f"""
        WITH 
            target_data AS (
                SELECT 
                    {', '.join([
                f"{col}"
                for col in source_columns
            ])}
                FROM {full_target_table}
            ),
            source_data AS (
                SELECT 
                    {', '.join([
                f"{column_mappings[col]['source_column']} AS {col}"
                for col in source_columns
            ])}
                FROM {source_cte}
            ),
            target_minus_source AS (
                SELECT * FROM target_data
                MINUS
                SELECT * FROM source_data
            ),
            source_minus_target AS (
                SELECT * FROM source_data
                MINUS
                SELECT * FROM target_data
            )
        SELECT 
            'Target Minus Source' AS diff_type,
            * 
        FROM 
            target_minus_source
        UNION ALL
        SELECT 
            'Source Minus Target' AS diff_type,
            * 
        FROM 
            source_minus_target;
        """,
            "Duplicate Records": f"""
WITH 
    duplicate_check AS (
        SELECT 
            {', '.join(key_columns)},
            COUNT(*) AS record_count
        FROM 
            {full_target_table}
        GROUP BY 
            {', '.join(key_columns)}
        HAVING 
            COUNT(*) > 1
    )
SELECT 
    'Duplicates in Target' AS status,
    *
FROM 
    duplicate_check;
"""
        }

        # Write SQL to file
        with open(file_path, 'w') as f:
            for section, query in queries.items():
                f.write(f"-- {section}\n")
                f.write(query + "\n\n")

        return file_path

    except Exception as e:
        return False, f"An error occurred: {str(e)}"
