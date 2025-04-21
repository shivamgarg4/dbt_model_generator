import json
import os
import openpyxl
import yaml


def load_json_file(file_path):
    """Load JSON file from path or parse JSON string"""
    if isinstance(file_path, str):
        try:
            # Try to load from file
            with open(file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            # If not a file, try parsing as JSON string
            return json.loads(file_path)
    elif isinstance(file_path, dict):
        return file_path
    else:
        raise ValueError("Invalid input: must be file path, JSON string, or dictionary")


def _extract_config_from_sheet(sheet):
    """
    Extract configuration information from a specific sheet (e.g., Config).
    Assumes key-value pairs in the first two columns.
    """
    config = {}
    for row in sheet.iter_rows(min_row=1, max_row=10, min_col=1, max_col=2):  # Adjust row range as needed
        key = row[0].value
        value = row[1].value
        if key:
            # Split by dots to create nested structure
            key_parts = str(key).split('.')
            current = config
            for i, part in enumerate(key_parts):
                if i == len(key_parts) - 1:
                    current[part] = value
                else:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
    return config


def _extract_mappings_from_sheet(mapping_sheet):
    # Initialize the comprehensive model configuration dictionary
    model_config = {
        "Source": {
            "Type": "",
            "Database": "",
            "Schema": "",
            "Table Name": "",
            "Name": ""
        },
        "Target": {
            "Schema": "",
            "Table Name": "",
            "materialization": "",
            "unique_key": "",
            "pre_hook": "",
            "post_hook": ""
        },
        "Columns": [],
        "dag_configuration": {
            "DAG Type": "",
            "Schedule Interval": "",
            "dependencies": []
        },
        "where_conditions": [],
        "group_by": [],
        "join_tables": []
    }

    # Helper function to safely get cell value
    def get_cell_value(row, col, default=''):
        cell = mapping_sheet.cell(row=row, column=col)
        return str(cell.value).strip() if cell.value is not None else default

    # Extract table and configuration information
    for row in range(1, mapping_sheet.max_row + 1):
        key = get_cell_value(row, 1)
        value = get_cell_value(row, 2)

        # Table Configuration Parsing
        if key == 'TARGET_TABLE':
            target_parts = value.split('.')
            if len(target_parts) < 2:
                raise ValueError(f"Invalid target table format: {value}. Expected SCHEMA.TABLE")
            model_config['Target']['Schema'] = target_parts[0]
            model_config['Target']['Table Name'] = target_parts[1]

        elif key == 'SOURCE_TABLE':
            source_parts = value.split('.')
            if len(source_parts) < 3:
                raise ValueError(f"Invalid source table format: {value}. Expected DATABASE.SCHEMA.TABLE")
            model_config['Source']['Database'] = source_parts[0]
            model_config['Source']['Schema'] = source_parts[1]
            model_config['Source']['Table Name'] = source_parts[2]

        elif key == 'SOURCE_TYPE':
            model_config['Source']['Type'] = value or 'source'

        elif key == 'SOURCE_NAME':
            model_config['Source']['Name'] = value or model_config['Source']['Schema'] or 'default_source'

        elif key == 'MATERIALIZATION':
            model_config['Target']['materialization'] = value or 'incremental'

        elif key == 'UNIQUE_KEY':
            model_config['Target']['unique_key'] = value

        elif key == 'PRE_HOOK':
            model_config['Target']['pre_hook'] = value

        elif key == 'POST_HOOK':
            model_config['Target']['post_hook'] = value

        # Column Mapping Section
        elif key == 'S.NO':
            # This indicates the start of column mapping
            column_header_row = row
            for mapping_row in range(column_header_row + 1, mapping_sheet.max_row + 1):
                target_column = get_cell_value(mapping_row, 2)

                # Stop if no target column
                if not target_column:
                    break

                column_mapping = {
                    'Target Column': target_column,
                    'Source Table': get_cell_value(mapping_row, 3),
                    'Logic': get_cell_value(mapping_row, 4) or "''",
                    'Description': get_cell_value(mapping_row, 5),
                    'Test': get_cell_value(mapping_row, 6)
                }
                model_config['Columns'].append(column_mapping)

        # Join Tables Section
        elif key == 'JOIN_TABLES':
            join_row = row + 1
            while join_row < mapping_sheet.max_row + 1:
                join_type = get_cell_value(join_row, 1)
                join_table = get_cell_value(join_row, 4)
                join_condition = get_cell_value(join_row, 5)

                if not join_table or not join_condition:
                    break

                join_info = {
                    'type': join_type,
                    'table': join_table,
                    'condition': join_condition
                }
                model_config['join_tables'].append(join_info)
                join_row += 1

        # Where Conditions
        elif key == 'WHERE_CONDITIONS':
            condition = get_cell_value(row, 2)
            if condition:
                model_config['where_conditions'].append(condition)

        # Group By Conditions
        elif key == 'GROUP_BY':
            group_by_col = get_cell_value(row, 2)
            if group_by_col:
                model_config['group_by'].append(group_by_col)

    # Validate required configurations
    required_source_fields = ['Database', 'Schema', 'Table Name']
    required_target_fields = ['Schema', 'Table Name']

    for field in required_source_fields:
        if not model_config['Source'][field]:
            raise ValueError(f"Missing required source configuration: {field}")

    for field in required_target_fields:
        if not model_config['Target'][field]:
            raise ValueError(f"Missing required target configuration: {field}")

    return model_config


def create_dbt_yml_from_config(model_config, output_path):
    """
    Generate a dbt YAML configuration with exact structural requirements
    """
    try:
        # Ensure model_config is a dictionary
        if isinstance(model_config, str):
            model_config = load_json_file(model_config)

        # Extract key components
        schema = model_config.get('Target', {}).get('Schema', 'unknown_schema')
        table_name = model_config.get('Target', {}).get('Table Name', 'unknown_table')

        # Create a more standard model name
        model_name = f"{schema}.{table_name}"

        # Initialize dbt YAML structure
        dbt_yml = {
            'version': 2,
            'models': []
        }

        # Prepare model entry
        model_entry = {
            'name': model_name
        }

        # Collect columns with unique combination test
        unique_combination_columns = []

        # Prepare columns list
        columns = []
        for column in model_config.get('Columns', []):
            column_entry = {
                'name': column.get('Target Column', ''),
                'description': column.get('Description', '')
            }

            # Add tests if present
            column_tests = []
            test = column.get('Test', '')
            if test:
                if test == 'not_null':
                    column_tests.append('not_null')
                elif test == 'unique':
                    column_tests.append('unique')
                elif test == 'unique_combination':
                    unique_combination_columns.append(column['Target Column'])

            if column_tests:
                column_entry['tests'] = column_tests

            columns.append(column_entry)

        # Add columns if not empty
        if columns:
            model_entry['columns'] = columns

        # Add unique combination test if applicable
        if len(unique_combination_columns) > 1:
            model_entry['tests'] = [{
                'dbt_utils.unique_combination_of_columns': {
                    'combination_of_columns': unique_combination_columns
                }
            }]

        # Add model to models list
        dbt_yml['models'].append(model_entry)

        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Generate file path
        yml_file_path = os.path.join(output_path, f"{schema}_{table_name}.yml")

        # Write YAML file with custom formatting
        with open(yml_file_path, 'w') as f:
            # Write version first
            f.write('version: 2\n\n')
            f.write('models:\n\n')

            # Write models with proper indentation
            for model in dbt_yml['models']:
                f.write(f'  - name: {model["name"]}\n')

                # Write tests if present
                if 'tests' in model:
                    f.write('    tests:\n')
                    for test in model['tests']:
                        f.write('      - dbt_utils.unique_combination_of_columns:\n')
                        f.write('          combination_of_columns:\n')
                        for column in test['dbt_utils.unique_combination_of_columns']['combination_of_columns']:
                            f.write(f'            - {column}\n')

                # Write columns if present
                if 'columns' in model:
                    f.write('    columns:\n')
                    for column in model['columns']:
                        # Write column name
                        f.write(f'      - name: {column["name"]}\n')

                        # Always write description with quotes
                        f.write(f'        description: "{column.get("description", "")}"\n')

                        # Write tests if present
                        if 'tests' in column:
                            f.write('        tests:\n')
                            for test in column['tests']:
                                f.write(f'          - {test}\n')

        return dbt_yml

    except Exception as e:
        print(f"Error creating dbt YAML configuration: {e}")
        return None



