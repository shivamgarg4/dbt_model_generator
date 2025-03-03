import json
import snowflake.connector
import os
from   .model_mapper  import ModelMapper
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

def generate_lnd_dbt_model_file(config, mapping_file_path):
    
    """Generate the dbt model file."""
    try:
        model_mapper = ModelMapper()
        workbook, mapping_sheet, config_sheet = model_mapper._load_workbook(mapping_file_path)
        # Get Snowflake configuration
        snowflake_config = model_mapper._get_snowflake_config(config_sheet)      
        # Connect to Snowflake and get column information
        columns = model_mapper._get_snowflake_columns(snowflake_config, source_info)
        
        source_schema = config['Source']['Schema']
        source_table = config['Source']['Table Name']

        formatted_columns = format_columns(columns)
        replaced_columns = replace_audit_columns(formatted_columns, source_schema, source_table, config['Target']['Schema'], config['Target']['Table Name'])
       
        model_config = f"""
        {{
            config(
                schema = '{config['Target']['Schema']}',
                alias = '{config['Target']['Table Name']}',
                tags = '{config['Target']['Table Name']}',
                transient = false
            )
        }}
        """

        model_config += "\n\nSELECT\n"
        model_config += ",\n".join(replaced_columns) + "\n"
        model_config += f"FROM {{{{ source('{source_schema}', '{source_table}') }}}}\n"

        # Create output directory if it doesn't exist
        output_dir = 'models'
        os.makedirs(output_dir, exist_ok=True)
    
        # Generate file name
        model_name = f"{config['Target']['Schema']}.{config['Target']['Table Name']}"
        file_name = f"{model_name}.sql"
        file_path = os.path.join(output_dir, file_name)

        # Write model file
        with open(file_path, 'w') as f:
            f.write(model_config)

        return file_path
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
