import os
import logging
import sqlparse
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


def get_config_from_sheet(config_sheet):
    """Extract Snowflake configuration from Config sheet"""
    config = {}
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
                config[param] = value
    return config

def get_table_info_from_sheet(mapping_sheet):
    """Get target and source table information from mapping sheet"""
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
    return target_table, source_table

def format_sql_content(sql_content, verbose=False):
    """
    Advanced SQL formatting with precise configuration controls

    Args:
        sql_content (str): The SQL content to format
        verbose (bool): Enable detailed logging for debugging

    Returns:
        str: Formatted SQL content
    """
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        # Log formatting details if verbose
        logger.debug(f"Original SQL length: {len(sql_content)}")

        try:
            # Try formatting with T-SQL dialect

            # Fix the SQL code (automatically corrects identified issues)

            # Use sqlparse for initial formatting
            formatted_sql = sqlparse.format(
                sql_content,
                reindent=True,
                keyword_case='upper',
            )
            logger.info(f"SQL formatting completed successfully {formatted_sql}")
            logger.debug(f"Formatted SQL length: {len(formatted_sql)}")
            return formatted_sql

        except Exception as format_error:
            logger.error(f"Formatting error: {format_error}")
            # Log the specific SQL content that caused the error
            logger.error(f"Problematic SQL Content:\n{sql_content}")

            # Optional: Try alternative formatting or return original
            return sql_content

    except Exception as e:
        logger.error(f"Unexpected error during SQL formatting: {e}")
        return sql_content


