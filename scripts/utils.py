import json
import snowflake.connector
import os

def load_json_config(json_file_path):
    """Load configuration details from a JSON file."""
    try:
        with open(json_file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Error: The file {json_file_path} was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"Error: The file {json_file_path} is not a valid JSON file.")
    except Exception as e:
        raise Exception(f"An error occurred while loading the configuration: {e}")

def connect_to_snowflake(connection_details, source_table_details):
    """Establish a connection to Snowflake."""
    try:
        return snowflake.connector.connect(
            account=connection_details['ACCOUNT'],
            user=connection_details['USER'],
            warehouse=connection_details['WAREHOUSE'],
            database=source_table_details['Database'],
            schema=source_table_details['Schema'],
            role=connection_details['ROLE'],
            authenticator=connection_details['AUTHENTICATOR']
        )
    except snowflake.connector.errors.DatabaseError as e:
        raise snowflake.connector.errors.DatabaseError(f"Database error: {e}")
    except Exception as e:
        raise Exception(f"An error occurred while connecting to Snowflake: {e}")

def get_view_columns(cursor, view_name):
    """Fetch the columns of the specified view."""
    try:
        cursor.execute(f"DESCRIBE VIEW {view_name}")
        return cursor.fetchall()
    except snowflake.connector.errors.ProgrammingError as e:
        raise snowflake.connector.errors.ProgrammingError(f"Programming error: {e}")
    except Exception as e:
        raise Exception(f"An error occurred while fetching view columns: {e}")

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

def generate_dbt_model_file(columns, model_file_path, target_schema_name, target_table_name, materialization_type, source_schema_name, source_view_name):
    """Generate the dbt model file."""
    try:
        os.makedirs(os.path.dirname(model_file_path), exist_ok=True)
        formatted_columns = format_columns(columns)
        replaced_columns = replace_audit_columns(formatted_columns, source_schema_name, source_view_name, target_schema_name, target_table_name)
        with open(model_file_path, 'w') as model_file:
            model_file.write(
                f"{{{{ config(schema ='{target_schema_name}', alias ='{target_table_name}', tags ='{target_table_name}', materialized='{materialization_type}', transient=false) }}}}\n\n")
            model_file.write("SELECT\n")
            model_file.write(",\n".join(replaced_columns) + "\n")
            model_file.write(f"FROM {{{{ source('{source_schema_name}', '{source_view_name}') }}}}\n")
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
