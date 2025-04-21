import json
import openpyxl


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
    # Initialize the model configuration dictionary
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

    # Extract table information
    for row in range(1, mapping_sheet.max_row + 1):
        cell_value = mapping_sheet.cell(row=row, column=1).value

        if cell_value == 'TARGET_TABLE':
            target_table = mapping_sheet.cell(row=row, column=2).value
            if target_table:
                target_parts = target_table.split('.')
                if len(target_parts) < 2:
                    raise ValueError("Target table should be in format: SCHEMA.TABLE")
                model_config['Target']['Schema'] = target_parts[0]
                model_config['Target']['Table Name'] = target_parts[1]

        elif cell_value == 'SOURCE_TABLE':
            source_table = mapping_sheet.cell(row=row, column=2).value
            if source_table:
                source_parts = source_table.split('.')
                if len(source_parts) < 3:
                    raise ValueError("Source table should be in format: DATABASE.SCHEMA.TABLE")
                model_config['Source']['Database'] = source_parts[0]
                model_config['Source']['Schema'] = source_parts[1]
                model_config['Source']['Table Name'] = source_parts[2]

        elif cell_value == 'SOURCE_TYPE':
            model_config['Source']['Type'] = mapping_sheet.cell(row=row, column=2).value or 'source'

        elif cell_value == 'SOURCE_NAME':
            source_name = mapping_sheet.cell(row=row, column=2).value
            if not source_name:
                source_name = model_config['Source']['Schema'] or 'default_source'
            model_config['Source']['Name'] = source_name

        elif cell_value == 'MATERIALIZATION':
            model_config['Target']['materialization'] = mapping_sheet.cell(row=row, column=2).value or 'incremental'

        elif cell_value == 'UNIQUE_KEY':
            unique_key_str = mapping_sheet.cell(row=row, column=2).value or ""
            model_config['Target']['unique_key'] = unique_key_str

        # DAG Configuration - Table Details
        elif cell_value == 'DAG Type':
            key = mapping_sheet.cell(row=row, column=1).value
            value = mapping_sheet.cell(row=row, column=2).value
            if key and value:
                model_config['dag_configuration']['DAG Type'] = value

        # DAG Configuration - Table Details
        elif cell_value == 'Schedule Interval':
            key = mapping_sheet.cell(row=row, column=1).value
            value = mapping_sheet.cell(row=row, column=2).value
            if key and value:
                model_config['dag_configuration']['Schedule Interval'] = value

        elif cell_value == 'PRE_HOOK':
            pre_hook = mapping_sheet.cell(row=row, column=2).value
            if pre_hook:
                model_config['Target']['pre_hook'] = pre_hook

        elif cell_value == 'POST_HOOK':
            post_hook = mapping_sheet.cell(row=row, column=2).value
            if post_hook:
                model_config['Target']['post_hook'] = post_hook

        # DAG Configuration - DAG Details
        elif cell_value == 'Dependencies':
            for dep_row in range(row + 1, mapping_sheet.max_row + 1):
                dep_number = mapping_sheet.cell(row=dep_row, column=1).value
                dep_schema = mapping_sheet.cell(row=dep_row, column=2).value
                dep_table = mapping_sheet.cell(row=dep_row, column=3).value

                if dep_number == '#':
                    continue

                if not dep_number or not dep_schema or not dep_table:
                    # Stop processing if a row is empty
                    break

                dependency = {
                    'Schema': dep_schema,
                    'Table': dep_table
                }
                model_config['dag_configuration']['dependencies'].append(dependency)


        # WHERE Conditions
        elif cell_value == 'WHERE_CONDITIONS':
            condition = mapping_sheet.cell(row=row, column=2).value
            if condition:
                model_config['where_conditions'].append(condition)

        # GROUP BY Conditions
        elif cell_value == 'GROUP_BY':
            group_by_col = mapping_sheet.cell(row=row, column=2).value
            if group_by_col:
                model_config['group_by'].append(group_by_col)

        # JOIN Tables
        elif cell_value == 'JOIN_TABLES':
            join_table = mapping_sheet.cell(row=row, column=2).value
            join_condition = mapping_sheet.cell(row=row, column=3).value
            join_type = mapping_sheet.cell(row=row, column=4).value
            if join_table and join_condition:
                join_info = {
                    'table': join_table,
                    'condition': join_condition
                }
                if join_type:
                    join_info['type'] = join_type
                model_config['join_tables'].append(join_info)

    # Find column mapping header row
    header_row = None
    for row in range(1, mapping_sheet.max_row + 1):
        if mapping_sheet.cell(row=row, column=1).value == 'S.NO':
            header_row = row
            break

    if not header_row:
        raise ValueError("Column mapping header not found in mapping file")

    # Extract column mappings
    for row in range(header_row + 1, mapping_sheet.max_row + 1):
        target_column = mapping_sheet.cell(row=row, column=2).value

        if not target_column:
            # Stop when we reach an empty row
            break

        source_table_col = mapping_sheet.cell(row=row, column=3).value
        logic = mapping_sheet.cell(row=row, column=4).value

        column_mapping = {
            'Target Column': target_column,
            'Source Table': source_table_col,
            'Logic': logic if logic is not None else "''"
        }

        model_config['Columns'].append(column_mapping)

    # Validate required fields
    if not model_config['Target']['Schema'] or not model_config['Target']['Table Name']:
        raise ValueError("Target table information is incomplete")

    if not model_config['Source']['Database'] or not model_config['Source']['Schema'] or not model_config['Source'][
        'Table Name']:
        raise ValueError("Source table information is incomplete")

    return model_config

