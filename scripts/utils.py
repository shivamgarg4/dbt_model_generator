
import os
import json
import logging
import sqlfluff
import tempfile

def ensure_directory_exists(directory_path):
    """Ensure that a directory exists, creating it if necessary"""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logging.info(f"Created directory: {directory_path}")
    return directory_path


def save_json_file(data, file_path):
    """Save data to a JSON file"""
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info(f"Saved JSON file: {file_path}")
        return True
    except Exception as e:
        logging.error(f"Error saving JSON file {file_path}: {str(e)}")
        return False

def get_file_extension(file_path):
    """Get the extension of a file"""
    _, ext = os.path.splitext(file_path)
    return ext.lower()

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
