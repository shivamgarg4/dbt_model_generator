import tempfile
import sqlfluff
from openpyxl import load_workbook
import openpyxl

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

def format_sql_content(sql_content):
    """
    Format SQL content using SQLFluff with dbt templating support

    Args:
        sql_content (str): The SQL content to format

    Returns:
        str: Formatted SQL content
    """
    try:
        # Create temporary file for SQLFluff to process
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
            temp_file.write(sql_content)
            temp_file.flush()

        # Configure SQLFluff
        config = sqlfluff.config.FluffConfig(
            configs={
                "dialect": "snowflake",
                "templater": "dbt",
                "exclude_rules": "L031,L034",
                "max_line_length": 120,
                "indent_unit": "space",
                "tab_space_size": 4,
            }
        )

        # Format the SQL content
        result = sqlfluff.fix(
            temp_file.name,
            config=config
        )

        return result

    except Exception as e:
        print(f"Warning: SQL formatting failed: {str(e)}")
        return sql_content  # Return original content if formatting fails
    finally:
        # Clean up temporary file
        import os
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
