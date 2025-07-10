from openpyxl import load_workbook
import snowflake.connector
import os
import logging

from scripts.utils.snowflake_utils import get_snowflake_connection, get_cursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dbt_model_generator.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def generate_lnd_dbt_model_file(mapping_file_path):
    """Generate the dbt model file."""
    logger.info(f"Starting to generate dbt model file from {mapping_file_path}")
    try:
        # Extract source information
        source_name = 'source'
        logger.debug("Loading workbook")
        workbook = load_workbook(mapping_file_path)
        pipeline_sheet = workbook['Pipeline Details']

        logger.info("Finding header row and column indices")
        # Find the header row and column indices
        header_row = 1
        source_model_col = None
        target_model_col = None
        model_type_col = None

        for col in range(1, pipeline_sheet.max_column + 1):
            header_value = pipeline_sheet.cell(row=header_row, column=col).value
            if header_value == 'Source Model':
                source_model_col = col
            elif header_value == 'Target Model':
                target_model_col = col
            elif header_value == 'Model Type':
                model_type_col = col

        if not all([source_model_col, target_model_col, model_type_col]):
            logger.error("Required columns not found in the sheet")
            raise ValueError("Required columns not found in the sheet")

        logger.info("Extracting source and target information")
        source_table = None
        target_table = None

        logger.info("Extracting Snowflake configuration")
        snowflake_config = read_snowflake_config(mapping_file_path)

        for row in range(2, pipeline_sheet.max_row + 1):
            model_type = pipeline_sheet.cell(row=row, column=model_type_col).value
            if model_type == 'LND Load':
                source_table = pipeline_sheet.cell(row=row, column=source_model_col).value
                target_table = pipeline_sheet.cell(row=row, column=target_model_col).value

                if not source_table or not target_table:
                    logger.error("Source or target table not found for LND Load type")
                    raise ValueError("Source or target table not found for LND Load type")

                logger.debug(f"Processing source table: {source_table} and target table: {target_table}")
                source_parts = source_table.split('.')
                source_db, source_schema, source_table_name = source_parts[0], source_parts[1], source_parts[2]

                target_parts = target_table.split('.')
                target_schema = target_parts[0] if len(target_parts) > 1 else None
                target_table_name = target_parts[-1]

                logger.info("Getting Snowflake connection")
                # Get or create singleton connection
                connection = get_snowflake_connection(snowflake_config)

                with get_cursor(connection) as cursor:
                    cursor.execute("USE WAREHOUSE {}".format(snowflake_config['WAREHOUSE']))
                    version = cursor.fetchone()
                    print(f"Snowflake Version: {version}")

                    logger.debug(f"Executing query to get columns for {source_table_name}")
                    cursor.execute(f"""
                                SELECT COLUMN_NAME
                                FROM {source_db}.INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = '{source_schema}'
                                AND TABLE_NAME = '{source_table_name}'
                                ORDER BY ORDINAL_POSITION
                            """)

                    columns = cursor.fetchall()
                    logger.debug(f"Retrieved {len(columns)} columns")

                    formatted_columns = format_columns(columns)
                    replaced_columns = replace_audit_columns(formatted_columns, source_schema, source_table_name, target_schema,
                                                             target_table_name)

                logger.info("Generating model configuration")
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

                output_dir = 'models'
                os.makedirs(output_dir, exist_ok=True)

                model_name = f"{target_schema}.{target_table_name}"
                file_name = f"{model_name}.sql"
                file_path = os.path.join(output_dir, file_name)

                logger.info(f"Writing model file to {file_path}")
                with open(file_path, 'w') as f:
                    f.write(model_config)

                logger.info("Successfully generated dbt model file")
        return True, file_path
    except Exception as e:
        logger.error(f"Error generating dbt model: {str(e)}", exc_info=True)
        raise Exception(f"An error occurred while generating the dbt model: {e}")


def create_dp_view_file(file_path,object_name=None):
    """Generate the dbt model file for DP view."""
    logger.info(f"Starting to create DP view file using {file_path}")
    try:
        # Load workbook
        logger.debug("Loading workbook")
        workbook = load_workbook(file_path)
        pipeline_sheet = workbook['Pipeline Details']

        logger.info("Finding header row and column indices")
        # Find the header row and column indices
        header_row = 1
        source_model_col = None
        target_model_col = None
        model_type_col = None

        for col in range(1, pipeline_sheet.max_column + 1):
            header_value = pipeline_sheet.cell(row=header_row, column=col).value
            if header_value == 'Source Model':
                source_model_col = col
            elif header_value == 'Target Model':
                target_model_col = col
            elif header_value == 'Model Type':
                model_type_col = col

        if not all([source_model_col, target_model_col, model_type_col]):
            logger.error("Required columns not found in the sheet")
            raise ValueError("Required columns not found in the sheet")

        logger.info("Extracting source and target information")
        source_table = None
        target_table = None

        logger.info("Extracting Snowflake configuration")
        snowflake_config = read_snowflake_config(file_path)

        for row in range(2, pipeline_sheet.max_row + 1):
            model_type = pipeline_sheet.cell(row=row, column=model_type_col).value
            if model_type == 'DP/BI View':
                source_table = pipeline_sheet.cell(row=row, column=source_model_col).value
                target_table = pipeline_sheet.cell(row=row, column=target_model_col).value

                if not source_table or not target_table:
                    logger.error("Source or target table not found for DP View type")
                    raise ValueError("Source or target table not found for DP View type")

                logger.debug(f"Processing source table: {source_table} and target table: {target_table}")
                source_parts = source_table.split('.')
                source_db, source_schema, source_table_name = source_parts[0], source_parts[1], source_parts[2]

                target_parts = target_table.split('.')
                target_schema = target_parts[0] if len(target_parts) > 1 else None
                target_table_name = target_parts[-1]



                logger.info("Getting Snowflake connection")
                # Get or create singleton connection
                connection = get_snowflake_connection(snowflake_config)

                with get_cursor(connection) as cursor:
                    # Get source columns
                    logger.debug(f"Fetching source columns for {source_table_name}")
                    cursor.execute(f"USE WAREHOUSE {snowflake_config['WAREHOUSE']}")
                    cursor.execute(f"""
                        SELECT COLUMN_NAME
                        FROM {source_db}.INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{source_schema}'
                        AND TABLE_NAME = '{source_table_name}'
                        ORDER BY ORDINAL_POSITION
                    """)
                    source_columns = cursor.fetchall()
                    # Rest of your existing code remains the same
                    target_columns = source_columns

                    # Process columns
                    logger.debug("Processing source and target columns")
                    source_formatted_columns = format_columns(source_columns)
                    source_replaced_columns = source_formatted_columns
                    target_formatted_columns = format_columns(target_columns)


                # Generate model configuration
                logger.info("Generating view SQL")
                model_config = f"""
                    -- Model: {target_schema}.{target_table_name}
                    -- Source: {source_db}.{source_schema}.{source_table_name}
                """

                model_config += f"\n\ncreate or replace view {target_schema}.{target_table_name}(\n"
                model_config += ",\n".join(target_formatted_columns) + "\n)copy grants as\n"
                model_config += "\n\nSELECT\n"
                model_config += ",\n".join(source_replaced_columns) + "\n"
                model_config += f"FROM {source_schema}.{source_table_name}\n"

                # Create and write to file
                logger.info("Writing view SQL to file")
                output_dir = 'views'
                os.makedirs(output_dir, exist_ok=True)

                if object_name:
                    model_name = f"{object_name}"
                else:
                    model_name = f"{target_schema}.{target_table_name}"

                file_name = f"{model_name}.sql"
                file_path = os.path.join(output_dir, file_name)

                with open(file_path, 'w') as f:
                    f.write(model_config)

                logger.info(f"Successfully created DP view file at: {file_path}")
        return True, file_path

    except Exception as e:
        logger.error(f"Error creating DP view file: {str(e)}", exc_info=True)
        raise Exception(f"An error occurred while creating the DP view file: {e}")


def format_columns(columns):
    """Format columns for the dbt model."""
    logger.debug("Formatting columns")
    try:
        max_length = max(len(column[0]) for column in columns)
        return [f"    {column[0].ljust(max_length)}" for column in columns]
    except Exception as e:
        logger.error(f"Error formatting columns: {str(e)}", exc_info=True)
        raise Exception(f"An error occurred while formatting columns: {e}")

def replace_audit_columns(columns, source_schema_name, source_view_name, target_schema_name, target_table_name):
    """Replace audit columns with static values."""
    logger.debug("Replacing audit columns")
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
        logger.error(f"Error replacing audit columns: {str(e)}", exc_info=True)
        raise Exception(f"An error occurred while replacing audit columns: {e}")

def read_snowflake_config(excel_path, sheet_name='config'):
    """
    Read Snowflake configuration from Excel sheet with improved error handling.

    Args:
        excel_path (str): Path to the Excel file
        sheet_name (str): Name of the sheet containing configurations

    Returns:
        dict: Dictionary containing Snowflake configurations
    """
    global workbook
    try:
        logger.debug("Loading workbook")
        workbook = load_workbook(excel_path)
        config_sheet = workbook[sheet_name]

        # Initialize configuration dictionary
        snowflake_config = {}

        # Find Snowflake Configuration section
        config_start_row = None
        for row in range(1, config_sheet.max_row + 1):
            if config_sheet.cell(row=row, column=1).value == 'Snowflake Configuration':
                config_start_row = row
                break

        if config_start_row is None:
            raise ValueError("Snowflake Configuration section not found in the Excel file")

        # Read configurations until empty row
        for row in range(config_start_row + 1, config_sheet.max_row + 1):
            param = config_sheet.cell(row=row, column=1).value  # First column - parameter name
            value = config_sheet.cell(row=row, column=2).value  # Second column - value

            # Break if we hit an empty row
            if not param:
                break

            # Store valid configurations
            if param in ['ROLE', 'WAREHOUSE', 'DATABASE', 'ACCOUNT', 'USER', 'AUTHENTICATOR']:
                if value:
                    snowflake_config[param] = str(value).strip()

        # Validate all required configurations are present
        required_params = ['ROLE', 'WAREHOUSE', 'DATABASE', 'ACCOUNT', 'USER', 'AUTHENTICATOR']
        missing_params = [param for param in required_params if param not in snowflake_config]

        if missing_params:
            raise ValueError(f"Missing required Snowflake configurations: {', '.join(missing_params)}")

        logger.debug("Successfully read Snowflake configuration")
        return snowflake_config

    except ImportError:
        logger.error("openpyxl package is not installed")
        raise Exception("openpyxl package is not installed")
    except Exception as e:
        logger.error(f"Error reading Snowflake configuration: {str(e)}")
        raise Exception(f"Error reading Snowflake configuration: {str(e)}")
    finally:
        if 'workbook' in locals():
            workbook.close()
