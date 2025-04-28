import os
import logging
import openpyxl
import json
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.comments import Comment
from openpyxl.utils import get_column_letter
from ..dbt_model_generator import create_dbt_model_from_json
from ..insert_sql_generator import insert_sql_generator
from ..merge_sql_generator import generate_merge_sql_for_pipelines
from ..utils import parse_ddl_file as parse_ddl_file
from ..excel_to_json import _extract_config_from_sheet, _extract_mappings_from_sheet,create_dbt_yml_from_config
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from openpyxl import load_workbook
from scripts.pipeline_generator.dag_generators import create_cron_dag, create_dataset_dag, create_sns_dag
from .model_generator_pipelines import generate_lnd_dbt_model_file, create_dp_view_file
from ..utils.snowflake_utils import get_snowflake_connection, get_cursor
from .source_column_mapper import update_source_mappings


def read_pipeline_sheet(pipeline_details_sheet):
    """
    Read the pipeline sheet and return a list of pipeline configurations

    :param pipeline_details_sheet: Worksheet containing pipeline details
    :return: List of dictionaries with pipeline configurations
    """
    # Extract pipeline configurations from the sheet
    pipelines = []

    # Define headers with their expected positions
    headers = {
        1: "Model Type",
        2: "Source Model",
        3: "Target Model",
        4: "Generated Mapping Sheet",
        5: "Generated Object Name"
    }

    for row in range(2, pipeline_details_sheet.max_row + 1):
        # Create a dictionary, skipping None headers or empty cells
        pipeline = {}
        for col, header in headers.items():
            cell_value = pipeline_details_sheet.cell(row=row, column=col).value

            # Only add non-None values to the dictionary
            if cell_value is not None:
                # Convert cell value to string if needed
                pipeline[header] = str(cell_value).strip() if cell_value != '' else None

        # Only append non-empty dictionaries
        if pipeline:
            pipelines.append(pipeline)

    return pipelines


def write_pipeline_details(pipeline_config_sheet):
    """
    Write pipeline details to the configuration sheet

    :param pipeline_config_sheet: Excel worksheet for pipeline configuration
    """

    pipeline_config_sheet.title = 'Pipeline Details'

    # Write the header row with simplified columns
    headers = [
        "Model Type", "Source Model", "Target Model", "Generated Mapping Sheet","Generated Object Name"
    ]

    # Write headers and apply formatting
    for col_num, header in enumerate(headers, start=1):
        cell = pipeline_config_sheet.cell(row=1, column=col_num, value=header)
        # Add white text and blue background formatting
        cell.font = Font(bold=True, color='FFFFFF')
        cell.fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")

    # Model Type Validation
    model_type_dv = DataValidation(
        type="list",
        formula1='"LND Load, DW Load, DP/BI View, Merge Macro, Insert Macro"',
        allow_blank=True
    )
    pipeline_config_sheet.add_data_validation(model_type_dv)

    # Apply validation to the entire column (starting from row 2)
    for row in range(2, 25):
        model_type_dv.add(pipeline_config_sheet.cell(row=row, column=1))

    # Adjust column widths for better readability
    for col_num in range(1, len(headers) + 1):
        column_letter = get_column_letter(col_num)
        pipeline_config_sheet.column_dimensions[column_letter].width = 25


def create_mapping_sheet(sheet, pipeline, columns):
    # Extract table name from pipeline
    table_name = pipeline.get('Target Model', 'Unknown')
    model_type = pipeline.get('Model Type', 'Unknown')

    # Add table name + "Mapping" as header (only once)
    sheet.merge_cells('A1:D1')
    header_cell = sheet.cell(row=1, column=1, value=f"{table_name} Mapping")
    header_cell.font = Font(bold=True, color='FFFFFF')
    header_cell.fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    header_cell.alignment = Alignment(horizontal='center')

    current_row = 2

    # Add Target and Source Configuration section
    sheet.cell(row=current_row, column=1, value='Table Configuration').font = Font(bold=True)
    current_row += 1

    # Add Target and Source table details
    config_items = [
        ('TARGET_TABLE', table_name),
        ('SOURCE_TABLE', pipeline.get('Source Model', '')),
        ('SOURCE_TYPE', ''),  # Empty for dropdown
        ('SOURCE_NAME', ''),
        ('MATERIALIZATION', ''),  # Empty for dropdown
        ('UNIQUE_KEY', ''),
        ('MERGE_TYPE',''),
        ('MINUS_LOGIC_REQUIRED', 'N'),
        ('DISTINCT_REQUIRED','N'),
        ('TRANSIENT_TABLE', 'N'),
        ('PRE_HOOK', ''),
        ('POST_HOOK', '')
    ]

    for item in config_items:
        label_cell = sheet.cell(row=current_row, column=1, value=item[0])
        label_cell.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
        value_cell = sheet.cell(row=current_row, column=2, value=item[1])

        # Add validations for specific fields
        if item[0] == 'SOURCE_TYPE':
            dv = DataValidation(type="list", formula1='"source,ref"', allow_blank=True)
            sheet.add_data_validation(dv)
            dv.add(value_cell)
        elif item[0] == 'MATERIALIZATION':
            dv = DataValidation(type="list", formula1='"incremental,truncate_load,lnd_load"', allow_blank=True)
            sheet.add_data_validation(dv)
            dv.add(value_cell)
        elif item[0] == 'MERGE_TYPE':
            dv = DataValidation(type="list", formula1='"update_insert,update_only,insert_only"', allow_blank=True)
            sheet.add_data_validation(dv)
            dv.add(value_cell)
        elif item[0] == 'MINUS_LOGIC_REQUIRED' or item[0] == 'TRANSIENT_TABLE':
            dv = DataValidation(type="list", formula1='"Y,N"', allow_blank=True)
            sheet.add_data_validation(dv)
            dv.add(value_cell)

        # Add comments for specific fields
        if item[0] == 'UNIQUE_KEY':
            value_cell.comment = Comment('For incremental models, specify comma-separated column names', 'System')
        elif item[0] == 'MINUS_LOGIC_REQUIRED':
            value_cell.comment = Comment('Set to Y to exclude audit columns and unique key combination', 'System')
        elif item[0] == 'TRANSIENT_TABLE':
            value_cell.comment = Comment('Set to Y to set true tag for transient model property', 'System')
        elif item[0] == 'PRE_HOOK':
            value_cell.comment = Comment('Pre-hook SQL command', 'System')
        elif item[0] == 'POST_HOOK':
            value_cell.comment = Comment('Post-hook SQL command', 'System')

        current_row += 1

    # Add blank row for separation
    current_row += 1

    # Add DAG Configuration section
    sheet.cell(row=current_row, column=1, value='DAG Configuration').font = Font(bold=True)
    current_row += 1

    # DAG config options
    dag_options = [
        {
            'param': 'DAG Type',
            'value': '',
            'options': ['DATASET DEPENDENCY', 'CRON', 'SNS'],
            'description': 'Select the type of DAG to generate'
        },
        {
            'param': 'Schedule Interval',
            'value': '0 */4 * * *',
            'description': 'Cron expression for scheduling (e.g., 0 */4 * * * for every 4 hours)'
        }
    ]

    # Add DAG config rows
    for config in dag_options:
        # Parameter name with yellow background
        param_cell = sheet.cell(row=current_row, column=1, value=config['param'])
        param_cell.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

        # Value cell
        value_cell = sheet.cell(row=current_row, column=2)
        value_cell.value = config['value']

        # Description
        desc_cell = sheet.cell(row=current_row, column=3, value=config['description'])

        # If it's DAG Type, create a data validation dropdown
        if config['param'] == 'DAG Type':
            dv = DataValidation(
                type="list",
                formula1=f'"{",".join(config["options"])}"',
                allow_blank=True
            )
            sheet.add_data_validation(dv)
            dv.add(value_cell)

        current_row += 1

    # Add blank row for separation
    current_row += 1

    # Add Dependencies section
    dep_header = sheet.cell(row=current_row, column=1, value='Dependencies')
    dep_header.font = Font(bold=True)
    dep_header.fill = PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")
    current_row += 1

    # Add dependency column headers
    headers = ['#', 'Dependency Schema', 'Dependency Table']
    for col, header in enumerate(headers, start=1):
        cell = sheet.cell(row=current_row, column=col, value=header)
        cell.font = Font(bold=True, color='FFFFFF')
        cell.fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")

    # Add 5 empty rows for dependencies
    for i in range(5):
        current_row += 1
        row_num = sheet.cell(row=current_row, column=1, value=i + 1)
        row_num.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

        # Add example values for the first row
        if i == 0:
            sheet.cell(row=current_row, column=2, value="DW")
            sheet.cell(row=current_row, column=3, value="D_CUSTOMER")
            comment = Comment('Enter the schema and table name of the dependency', 'System')
            sheet.cell(row=current_row, column=2).comment = comment

    # Add blank row for separation
    current_row += 2

    # Add Columns section
    columns_mapping_header = sheet.cell(row=current_row, column=1, value='Columns Mapping')
    columns_mapping_header.font = Font(bold=True)
    columns_mapping_header.fill = PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")
    current_row += 1

    # Updated headers to include Column Description
    headers = [
        'S.NO',
        'TargetColumn',
        'Source Table',
        'Logic/Mapping/Constant Value',
        'Column Description',  # New column for description
        'Test Types'
    ]

    # Define available test types
    test_types = [
        'unique_combination',  # Unique combination of specified columns
        'not_null',  # Ensures column doesn't contain null values
        'unique'  # Ensures column values are unique
    ]


    # Modify column processing for LND Load
    valid_columns = []
    if model_type == 'LND Load':
        # For LND Load, use the columns as-is from the source table
        valid_columns = [(col['COLUMN_NAME'], '') for col in columns]
    else:
        # For other model types, filter out non-column entries
        valid_columns = []
        for column_name, definition in columns:
            if (column_name.upper() not in ["UNIQUE", "PRIMARY", "CONSTRAINT"] and
                    not column_name.isdigit() and
                    not column_name.endswith(')') and
                    column_name.strip()):
                valid_columns.append((column_name, definition))

    # Add column mapping section with new description column
    for col, header in enumerate(headers, start=1):
        cell = sheet.cell(row=current_row, column=col, value=header)
        cell.font = Font(bold=True, color='FFFFFF')
        cell.fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    header_row = current_row

    # Test Types Dropdown
    test_type_dv = DataValidation(
        type="list",
        formula1=f'"{",".join(test_types)}"',
        allow_blank=True
    )

    # Now add the valid columns to the sheet with test configuration
    for i, (column_name, _) in enumerate(valid_columns, start=1):
        row = header_row + i
        sheet.cell(row=row, column=1, value=i)  # S.NO
        sheet.cell(row=row, column=2, value=column_name)  # TargetColumn

        # Description Cell (New)
        description_cell = sheet.cell(row=row, column=5)
        description_cell.comment = Comment(
            'Enter a clear, concise description of the column.\n'
            'What does this column represent?\n'
            'Example: "Unique identifier for customer accounts"',
            'Column Description Guide'
        )

        # Test Types Cell
        test_type_cell = sheet.cell(row=row, column=6)  # Shifted due to new description column
        test_type_dv.add(test_type_cell)
        sheet.add_data_validation(test_type_dv)

        test_type_cell.comment = Comment(
            'Data Validation Options:\n'
            '- unique_combination: System-wide unique constraint\n'
            '- not_null: Prevents empty values\n'
            '- unique: No duplicate values allowed',
            'Test Configuration'
        )

        current_row = row

    # Adjust column widths for readability
    column_widths = {
        'A': 10,  # S.NO
        'B': 30,  # TargetColumn
        'C': 30,  # Source Table
        'D': 40,  # Logic/Mapping/Constant Value
        'E': 50,  # Column Description (New)
        'F': 30,  # Test Types
    }

    for col, width in column_widths.items():
        sheet.column_dimensions[col].width = width

    # Calculate the last row of column mappings
    last_column_row = header_row + len(valid_columns)

    # Add blank row for separation
    current_row += 2

    # Add JOIN_TABLES section header
    join_header = sheet.cell(row=current_row, column=1, value='Join Tables')
    join_header.font = Font(bold=True)
    join_header.fill = PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")
    current_row += 1

    # Add JOIN_TABLES section
    join_cell = sheet.cell(row=current_row, column=1, value='JOIN_TABLES')
    join_cell.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    current_row += 1

    # Add join table headers
    join_headers = ['Join Type', 'Table Type', 'Source Name', 'Table Name', 'Alias', 'Join Condition']
    for col, header in enumerate(join_headers, start=1):
        cell = sheet.cell(row=current_row, column=col, value=header)
        cell.font = Font(bold=True, color='FFFFFF')
        cell.fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")

    # Add 5 empty rows for joins with validations
    for i in range(5):
        current_row += 1

        # Join Type validation
        join_type_cell = sheet.cell(row=current_row, column=1, value='LEFT')
        join_type_dv = DataValidation(
            type="list",
            formula1='"LEFT,INNER"',
            allow_blank=True
        )
        sheet.add_data_validation(join_type_dv)
        join_type_dv.add(join_type_cell)

        # Table Type validation
        table_type_cell = sheet.cell(row=current_row, column=2)
        table_type_dv = DataValidation(
            type="list",
            formula1='"source,ref,table"',
            allow_blank=True
        )
        sheet.add_data_validation(table_type_dv)
        table_type_dv.add(table_type_cell)

    # Add blank row for separation
    current_row += 2

    # Add WHERE_CONDITIONS section
    where_cell = sheet.cell(row=current_row, column=1, value='WHERE_CONDITIONS')
    where_cell.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    sheet.merge_cells(start_row=current_row, start_column=2, end_row=current_row, end_column=6)

    # Add blank row for separation
    current_row += 2

    # Add GROUP_BY section
    group_cell = sheet.cell(row=current_row, column=1, value='GROUP BY')
    group_cell.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    sheet.merge_cells(start_row=current_row, start_column=2, end_row=current_row, end_column=6)

    # Add blank row for separation
    current_row += 2

    # Add GROUP_BY section
    having_cell = sheet.cell(row=current_row, column=1, value='HAVING')
    having_cell.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    sheet.merge_cells(start_row=current_row, start_column=2, end_row=current_row, end_column=6)

    # Adjust column widths
    column_widths = {
        'A': 15,  # S.NO/Join Type
        'B': 40,  # TargetColumn/Table Type
        'C': 40,  # Source Table/Source Name
        'D': 50,  # Logic/Table Name
        'E': 20,  # Alias
        'F': 60,  # Join Condition
    }

    for col, width in column_widths.items():
        sheet.column_dimensions[col].width = width


def create_config_sheet(config_sheet):
    # Start from the first row
    current_row = 1

    # Add Snowflake Configuration section
    config_sheet.cell(row=current_row, column=1, value='Snowflake Configuration').font = openpyxl.styles.Font(bold=True)
    current_row += 1

    # Snowflake config options
    snowflake_options = [
        {
            'param': 'ROLE',
            'value': 'REV_GROWTH_MGMT_DEV_EDITOR_ROLE',
            'description': 'Snowflake role to use'
        },
        {
            'param': 'WAREHOUSE',
            'value': 'REV_GROWTH_MGMT_DEV_LOAD_WH',
            'description': 'Snowflake warehouse to use'
        },
        {
            'param': 'DATABASE',
            'value': 'REV_GROWTH_MGMT_DEV_DB',
            'description': 'Snowflake database to use'
        },
        {
            'param': 'ACCOUNT',
            'value': 'cbrands.us-east-1',
            'description': 'Snowflake account identifier'
        },
        {
            'param': 'USER',
            'value': '',
            'description': 'Snowflake username'
        },
        {
            'param': 'AUTHENTICATOR',
            'value': 'externalbrowser',
            'description': 'Authentication method'
        }
    ]

    # Add Snowflake config rows
    for config in snowflake_options:
        # Parameter name with yellow background
        param_cell = config_sheet.cell(row=current_row, column=1, value=config['param'])
        param_cell.fill = openpyxl.styles.PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

        # Value cell
        value_cell = config_sheet.cell(row=current_row, column=2, value=config['value'])

        # Description
        desc_cell = config_sheet.cell(row=current_row, column=3, value=config['description'])

        current_row += 1

    # Adjust column widths
    config_sheet.column_dimensions['A'].width = 20  # Parameter
    config_sheet.column_dimensions['B'].width = 40  # Value
    config_sheet.column_dimensions['C'].width = 60  # Description

    # Add borders to all cells
    for row in config_sheet.iter_rows(min_row=1, max_row=current_row):
        for cell in row:
            cell.border = openpyxl.styles.Border(
                left=openpyxl.styles.Side(style='thin'),
                right=openpyxl.styles.Side(style='thin'),
                top=openpyxl.styles.Side(style='thin'),
                bottom=openpyxl.styles.Side(style='thin')
            )

    # Add section headers styling
    for row in [1, current_row - len(snowflake_options)]:
        cell = config_sheet.cell(row=row, column=1)
        cell.font = openpyxl.styles.Font(bold=True)
        cell.fill = openpyxl.styles.PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")


def process_pipeline_sheet(workbook_path, ddl_path):
    # Load the workbook and get the pipeline details sheet
    workbook = load_workbook(workbook_path)
    pipeline_details_sheet = workbook['Pipeline Details']
    config_sheet = workbook['config']

    # Read pipeline configurations
    pipelines = read_pipeline_sheet(pipeline_details_sheet)

    # Dictionary to keep track of count for each table
    table_counter = {}

    # Model type abbreviations
    type_abbrev = {
        'Merge': 'M',
        'Insert': 'I',
        'DW': 'D',
        'LND': 'L'
    }

    # Create mapping sheets for each pipeline
    for pipeline_index, pipeline in enumerate(pipelines, start=2):  # start=2 because row 1 is header
        source_model_name = pipeline.get('Source Model')
        target_model = pipeline.get("Target Model", "Unknown")
        model_type = pipeline.get("Model Type", "Unknown")
        generated_mapping_sheet_name = pipeline.get("Generated Mapping Sheet", "Unknown")

        # If no mapping sheet name exists, generate one
        if not generated_mapping_sheet_name or generated_mapping_sheet_name == "Unknown":
            model_type_split = pipeline.get("Model Type", "Unknown").split()[0]
            target_model_split = target_model.split('.')[1]

            # Create mapping sheet for all Load models
            if model_type in ['DW Load', 'Merge Macro', 'Insert Macro', 'LND Load']:
                # Get abbreviated model type
                type_short = type_abbrev.get(model_type_split, model_type_split[0])

                # Generate unique key for the table
                table_key = f"{type_short}_{target_model_split}"

                # Initialize or increment counter for this table
                if table_key not in table_counter:
                    table_counter[table_key] = 1
                else:
                    table_counter[table_key] += 1

                # Shorten target model name if it's too long (take first 15 chars)
                short_target = target_model_split[:15]

                # Create sheet name with counter (padded with zero)
                generated_mapping_sheet_name = f"{type_short}{str(table_counter[table_key]).zfill(2)}_{short_target}" + ("_Map" if not short_target.endswith('_') else "Map")

        # Create mapping sheet using either the existing or newly generated name
        if generated_mapping_sheet_name and generated_mapping_sheet_name != "Unknown":
            mapping_sheet = workbook.create_sheet(title=generated_mapping_sheet_name)
            workbook.save(workbook_path)
            process_filled_workbook_to_json(workbook_path)

            # Modify column retrieval logic
            if model_type == 'LND Load':
                # For LND Load, get columns from source table
                columns = read_source_table_columns(source_model_name)
            else:
                # For other models, parse DDL file
                columns = parse_ddl_file(ddl_path)

            # Ensure columns is a list of tuples or a list of dictionaries
            if isinstance(columns, tuple):
                columns = columns[0]

            create_mapping_sheet(mapping_sheet, pipeline, columns)

            # Update the pipeline details sheet with the mapping sheet name
            pipeline_details_sheet.cell(row=pipeline_index, column=4, value=generated_mapping_sheet_name)

    workbook.save(workbook_path)
    process_filled_workbook_to_json(workbook_path)
    return workbook

def read_source_table_columns(source_model):
    source_parts = source_model.split('.')
    source_db, source_schema, source_table_name = source_parts[0], source_parts[1], source_parts[2]
    config_path_src = Path("config/config.json")

    logging.info("Extracting Snowflake configuration")
    with open(config_path_src, 'r') as f:
        config = json.load(f)
        # Keep the keys in uppercase to match the expected format
        snowflake_config = {
            'USER': config.get('USER'),
            'AUTHENTICATOR': config.get('AUTHENTICATOR'),
            'ACCOUNT': config.get('ACCOUNT'),
            'WAREHOUSE': config.get('WAREHOUSE'),
            'DATABASE': config.get('DATABASE'),
            'ROLE': config.get('ROLE')
        }

    logging.info("Getting Snowflake connection")
    # Get or create singleton connection
    connection = get_snowflake_connection(snowflake_config)

    with get_cursor(connection) as cursor:
        # Get source columns
        logging.debug(f"Fetching source columns for {source_table_name}")
        cursor.execute(f"""
             SELECT 
                 COLUMN_NAME
             FROM {source_db}.INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = '{source_schema}'
             AND TABLE_NAME = '{source_table_name}'
             ORDER BY ORDINAL_POSITION
         """)
        source_columns = cursor.fetchall()

        # Convert to a list of dictionaries with name and definition
        source_columns = [
            {
                'COLUMN_NAME': col[0]
            } for col in source_columns
        ]
        return source_columns

def process_filled_workbook_to_json(workbook_path: str) -> Tuple[Optional[Dict], Optional[List[Dict]]]:
    """
    Process an Excel workbook and extract configuration and mapping data to JSON files.
    Args:
        workbook_path: Path to the Excel workbook
    Returns:
        Tuple containing config_data and list of mapping_data
    """
    # Configure logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    try:
        workbook = load_workbook(workbook_path)
        pipeline_details_sheet = workbook['Pipeline Details']
        config_data = None
        all_mapping_data = []
        table_counter = {}  # Added to track table counts

        # Add type abbreviation mapping
        type_abbrev = {
            'Merge': 'M',
            'Insert': 'I',
            'DW': 'D',
            'LND': 'L'
        }

        # Read pipeline configurations
        pipelines = read_pipeline_sheet(pipeline_details_sheet)
        logger.info(f"Loaded {len(pipelines)} pipelines from workbook")

        config_data = _extract_config_from_sheet(workbook['config'])
        output_directory = Path("config")
        output_directory.mkdir(parents=True, exist_ok=True)
        file_path = output_directory / "config.json"

        try:
            with open(file_path, 'w') as f:
                json.dump(config_data, f, indent=2)
            logger.info(f"Successfully wrote config to {file_path}")
        except IOError as e:
            logger.error(f"Error writing config file: {e}")

        # Create mapping sheets for each pipeline
        for pipeline_index, pipeline in enumerate(pipelines, start=2):
            try:
                target_model = pipeline.get("Target Model", "Unknown")
                model_type = pipeline.get("Model Type", "Unknown")
                target_sheet = pipeline.get("Generated Mapping Sheet")

                # Generate mapping sheet name if not present
                model_type_split = model_type.split()[0]
                target_model_split = target_model.split('.')[-1]

                if model_type in ['DW Load', 'Merge Macro', 'Insert Macro', 'LND Load']:
                    type_short = type_abbrev.get(model_type_split, model_type_split[0])
                    table_key = f"{type_short}_{target_model_split}"

                    # Initialize or increment counter for this table
                    if table_key not in table_counter:
                        table_counter[table_key] = 1
                    else:
                        table_counter[table_key] += 1

                    short_target = target_model_split[:15]
                    # Conditionally add underscore based on whether short_target already ends with an underscore
                    target_sheet = f"{type_short}{str(table_counter[table_key]).zfill(2)}_{short_target}" + (
                        "_Map" if not short_target.endswith('_') else "Map")

                    # Update the pipeline details sheet with the new mapping sheet name
                    pipeline_details_sheet.cell(row=pipeline_index, column=4, value=target_sheet)

                    target_workbook = _load_workbook(workbook_path, target_sheet)
                    mapping_data = _extract_mappings_from_sheet(target_workbook)
                    all_mapping_data.append(mapping_data)

                    # Write the extracted mappings to a JSON file
                    output_directory = Path("config/mappings")
                    output_directory.mkdir(parents=True, exist_ok=True)
                    file_path = output_directory / f"{target_sheet}.json"

                    with open(file_path, 'w') as f:
                        json.dump(mapping_data, f, indent=2)

                    logger.info(f"Successfully processed and wrote mapping for {target_sheet}")

            except Exception as e:
                logger.error(f"Error processing pipeline {target_sheet if target_sheet else 'unknown'}: {e}",
                             exc_info=True)
                continue

        # Save the workbook with updated mapping sheet names
        workbook.save(workbook_path)
        logger.info(f"Successfully saved workbook with updated mapping sheet names")
        return config_data, all_mapping_data

    except Exception as e:
        logger.error(f"Error processing workbook: {e}", exc_info=True)
        return None, None


def process_filled_sheet(workbook_path: str, logger: Optional[logging.Logger] = None) -> None:
    """
    Process a filled workbook and update source mappings if configuration exists.

    Args:
        workbook_path: Path to the workbook file
        logger: Optional logger instance. If not provided, creates a new logger.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        # Create console handler if no handlers exist
        if not logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

    logger.info(f"Starting to process workbook: {workbook_path}")

    try:
        process_filled_workbook_to_json(workbook_path)
        logger.info("Successfully processed workbook to JSON")
    except Exception as e:
        logger.error(f"Failed to process workbook to JSON: {str(e)}", exc_info=True)
        raise

    mappings_dir_src = Path("config/mappings/")
    config_path_src = Path("config/config.json")

    # Update source mappings if configuration is provided
    if mappings_dir_src.exists() and config_path_src.exists():
        logger.info("Found mapping configuration files, updating source mappings")
        try:
            update_source_mappings(
                workbook_path=workbook_path,
                mappings_dir=str(mappings_dir_src),
                config_path=str(config_path_src)
            )
            logger.info("Source column mapping completed successfully")
        except Exception as e:
            logger.warning(f"Source column mapping failed: {str(e)}", exc_info=True)
    else:
        logger.info("Source column mapping skipped - mappings_dir or config_path not found")


def _load_workbook(file_path, generated_mapping_sheet):
    """Load and validate Excel workbook"""
    if not file_path:
        raise ValueError("No Excel file selected")

    workbook = load_workbook(file_path)
    # Check if the required sheets exist
    if 'Pipeline Details' not in workbook.sheetnames:
        raise ValueError("Required sheet (Pipeline Details) not found in Excel")
    if generated_mapping_sheet not in workbook.sheetnames:
        raise ValueError(f"Required sheet ({generated_mapping_sheet}) not found in Excel")

    worksheets = workbook[generated_mapping_sheet]

    return worksheets


def generate_dbt_models(workbook_path, output_dir, json_config_dir, target_ddl_path):
    """
    Controller function to generate different types of DBT models

    Args:
        workbook_path (str): Path to the Excel workbook
        output_dir (str): Directory to save generated models
        json_config_dir (str): Directory with JSON configuration files
        target_ddl_path (str): Path to the target DDL file
    """
    global target_model
    try:
        # Validate input paths
        if not os.path.exists(workbook_path):
            raise FileNotFoundError(f"Workbook not found: {workbook_path}")

        if not os.path.exists(json_config_dir):
            raise FileNotFoundError(f"JSON config directory not found: {json_config_dir}")

        # Create output directory if not exists
        os.makedirs(output_dir, exist_ok=True)

        # Load and process workbook
        workbook = load_workbook(workbook_path, data_only=True)
        pipeline_details_sheet = workbook['Pipeline Details']

        # Read pipeline configurations
        pipelines = read_pipeline_sheet(pipeline_details_sheet)
        process_filled_workbook_to_json(workbook_path)

        # Process each pipeline
        for pipeline in pipelines:
            try:
                model_type = pipeline.get("Model Type", "Unknown")
                target_model = pipeline.get("Target Model", "Unknown")
                mapping_sheet_name = pipeline.get("Generated Mapping Sheet", "")
                generated_object_name = pipeline.get("Generated Object Name", "")

                # Find corresponding mapping sheet
                mapping_sheet = None
                if mapping_sheet_name and mapping_sheet_name in workbook.sheetnames:
                    mapping_sheet = workbook[mapping_sheet_name]

                # Find corresponding JSON config
                config_filename = f"{mapping_sheet_name}.json"
                config_file_path = os.path.join(json_config_dir, config_filename)

                if not os.path.exists(config_file_path) and not any(
                        mt in model_type for mt in ["DP/BI View"]):
                    logging.warning(f"No config file found for {target_model}")
                    continue

                # Generate model based on type
                model_content = None
                output_filename = f"{target_model.lower().replace('.', '_')}.sql"
                output_path = os.path.join(output_dir, output_filename)

                if "DW Load" in model_type :
                    model_content = create_dbt_model_from_json(
                        config_file=config_file_path,
                        mapping_sheet=mapping_sheet,
                        target_ddl_path=target_ddl_path,
                        object_name=generated_object_name
                    )
                    generate_dag_file("DW", config_file_path, "dags/")
                    create_dbt_yml_from_config(config_file_path, "yml/")

                elif "Merge Macro" in model_type:
                    model_content = generate_merge_sql_for_pipelines(
                        config_file=config_file_path,
                        mapping_sheet=mapping_sheet,
                        object_name=generated_object_name
                    )
                    generate_dag_file("DW", config_file_path, "dags/")

                elif "Insert Macro" in model_type:
                    model_content = insert_sql_generator(
                        config_file=config_file_path,
                        mapping_sheet=mapping_sheet,
                        object_name=generated_object_name
                    )
                    generate_dag_file("DW", config_file_path, "dags/")

                elif "LND Load" in model_type:
                    model_content = create_dbt_model_from_json(
                        config_file=config_file_path,
                        mapping_sheet=mapping_sheet,
                        target_ddl_path=target_ddl_path,
                        object_name=generated_object_name
                    )
                    generate_dag_file("LND", config_file_path, "dags/", workbook_path)
                    create_dbt_yml_from_config(config_file_path, "yml/")

                elif "DP/BI View" in model_type:
                    model_content = create_dp_view_file(
                        file_path=workbook_path,
                        object_name=generated_object_name
                    )
                    generate_dag_file("DP", None, "dags/", workbook_path)
                    create_dbt_yml_from_config(config_file_path, "yml/")


                else:
                    logging.warning(f"Unsupported model type: {model_type}")
                    continue


            except Exception as e:
                logging.error(f"Error processing {target_model}: {e}")
                continue

        generate_dbt_jobs(workbook_path, "config/mappings/", "jobs/")

        logging.info("DBT model generation completed")
        # # Clean up existing JSON files before generation
        # try:
        #     # Clean up config directory
        #     config_dir = "config/"
        #     for root, dirs, files in os.walk(config_dir):
        #         for file in files:
        #             if file.endswith('.json'):
        #                 file_path = os.path.join(root, file)
        #                 try:
        #                     os.remove(file_path)
        #                     logging.info(f"Deleted existing JSON file: {file_path}")
        #                 except OSError as e:
        #                     logging.error(f"Error deleting file {file_path}: {str(e)}")
        #
        #     logging.info("Cleanup of existing JSON files completed")
        # except Exception as e:
        #     logging.error(f"Error during cleanup process: {str(e)}")

    except Exception as e:
        logging.error(f"Fatal error in model generation: {e}")
        raise


def generate_dag_file(type_of_model, json_output_path, dag_output_path, workbook_path=None):
    """Generate the appropriate DAG file based on configuration
    Args:
        type_of_model (str): Type of model (LND, DP, or DW)
        json_output_path (str): Path to JSON config file
        dag_output_path (str): Output directory for DAG files
        workbook_path (str): Path to the Excel workbook
    """
    try:
        dag_type = None
        schedule_interval = None
        dependencies = []
        schema_name = None
        table_name = None
        source_name = None

        if json_output_path is None and workbook_path:
            # For LND and DP models without config
            workbook = load_workbook(workbook_path)
            pipeline_sheet = workbook['Pipeline Details']

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
                raise ValueError("Required columns not found in Pipeline Details sheet")

            # Find the row with matching model type and target model
            target_row = None
            for row in range(2, pipeline_sheet.max_row + 1):
                model_type_value = pipeline_sheet.cell(row=row, column=model_type_col).value
                target_model_value = pipeline_sheet.cell(row=row, column=target_model_col).value

                if (type_of_model == "DP" and "DP/BI View" in str(model_type_value)) and target_model_value == target_model:
                    target_row = row
                    break

            # if not target_row:
            #     raise ValueError(f"No matching row found for {type_of_model} and target model {target_model}")

            # Extract source and target information
            source_table = pipeline_sheet.cell(row=target_row, column=source_model_col).value
            target_table = pipeline_sheet.cell(row=target_row, column=target_model_col).value

            if not source_table or not target_table:
                raise ValueError("Source or target table not found")

            logging.debug(f"Processing source table: {source_table} and target table: {target_table}")

            # Parse source table components
            source_parts = source_table.split('.')
            source_db, source_schema, source_table_name = source_parts[0], source_parts[1], source_parts[2]
            source_name = source_table_name

            # Parse target table components
            target_parts = target_table.split('.')
            if len(target_parts) < 2:
                raise ValueError(f"Invalid target table format: {target_table}. Expected format: schema.table")

            schema_name = target_parts[0]
            target_table_name = target_parts[-1]
            table_name = target_table_name.lower()

            if type_of_model in ["DP"]:
                dag_type = "cron"
                schedule_interval = "0 */4 * * *"
                dependencies = []
                schema_name = schema_name
                table_name = target_table_name
                source_name = source_name

        else:
            # For models with config file
            with open(json_output_path, 'r') as json_file:
                config = json.load(json_file)
                dag_config = config.get('dag_configuration', {})
                if not dag_config:
                    dag_config = {
                        "DAG Type": "DATASET DEPENDENCY",
                        "Schedule Interval": "0 */4 * * *",
                        "dependencies": []
                    }

                if type_of_model == "DW" or type_of_model == "LND":
                    dag_type = dag_config.get("DAG Type", "DATASET DEPENDENCY").lower()
                    schedule_interval = dag_config.get("Schedule Interval", "0 */4 * * *")
                    dependencies = dag_config.get("dependencies", [])
                    schema_name = config['Target']['Schema']
                    table_name = config['Target']['Table Name']
                    source_name = config.get('Source', {}).get('Table Name')

        if not table_name:
            raise ValueError("Could not determine table name")
        if not schema_name:
            raise ValueError("Could not determine schema name")

        logging.info(f"DAG type: {dag_type}")
        logging.info(f"Schedule interval: {schedule_interval}")
        logging.info(f"Dependencies: {dependencies}")
        logging.info(f"Source name: {source_name}")
        logging.info(f"Schema name: {schema_name}")
        logging.info(f"Table name: {table_name}")

        # Map generator functions to the normalized DAG types
        dag_generators = {
            "dataset dependency": create_dataset_dag,
            "cron": create_cron_dag,
            "sns": create_sns_dag
        }

        # Get the appropriate generator function
        generator = dag_generators.get(dag_type.lower())
        if not generator:
            raise ValueError(f"Unsupported DAG Type: {dag_type}")

        # Create DAG filename
        dag_filename = f"{schema_name}_{table_name}_dag.py"
        dag_file_path = os.path.join(dag_output_path, dag_filename)

        # Create the DAG using the selected generator
        generator_config = {
            'json_path': json_output_path,
            'dag_file_path': dag_file_path,
            'schedule_interval': schedule_interval,
            'dependencies': dependencies,
            'schema_name': schema_name,
            'table_name': table_name
        }

        generator(**generator_config)
        logging.info(f"DAG file generated: {dag_file_path}")

        return dag_file_path

    except Exception as e:
        logging.error(f"Error generating DAG file: {e}")
        raise


def generate_dbt_jobs(workbook_path, json_config_dir, output_dir='jobs'):
    """
    Generate DBT job files by grouping pipelines based on their target model.

    Args:
        workbook_path (str): Path to the Excel workbook containing pipeline details
        json_config_dir (str): Directory containing JSON configuration files
        output_dir (str): Directory to save generated job files (default: 'jobs')
    """
    try:
        if not os.path.exists(workbook_path):
            raise FileNotFoundError(f"Workbook not found: {workbook_path}")
        if not os.path.exists(json_config_dir):
            raise FileNotFoundError(f"JSON config directory not found: {json_config_dir}")

        os.makedirs(output_dir, exist_ok=True)

        workbook = load_workbook(workbook_path, data_only=True)
        pipeline_details_sheet = workbook['Pipeline Details']
        pipelines = read_pipeline_sheet(pipeline_details_sheet)

        # Group pipelines by Target Model
        pipeline_groups = {}
        for pipeline in pipelines:
            target_model = pipeline.get("Target Model", "Unknown")
            if target_model not in pipeline_groups:
                pipeline_groups[target_model] = []
            pipeline_groups[target_model].append(pipeline)

        # Process each target model group
        for target_model, group_pipelines in pipeline_groups.items():
            try:
                sorted_pipelines = sorted(group_pipelines,
                                          key=lambda x: int(x.get("Execution Order", 999))
                                          if x.get("Execution Order") else 999)

                job_content = ""
                schema_table = target_model.split(".")

                if len(schema_table) != 2:
                    logging.warning(f"Invalid target model format: {target_model}")
                    continue

                target_schema, target_table = schema_table

                # Process pipelines based on Model Type
                for pipeline in sorted_pipelines:
                    model_type = pipeline.get("Model Type", "")
                    generated_object_name = pipeline.get("Generated Object Name", "")

                    if generated_object_name:

                        if model_type == "Merge Macro":
                            macro_name = f"MAC_{generated_object_name}_MERGE"
                            job_content += f"dbt run-operation {macro_name}\n"
                        if model_type == "Insert Macro":
                            macro_name = f"MAC_{generated_object_name}_INSERT"
                            job_content += f"dbt run-operation {macro_name}\n"

                    else:
                        if model_type == "LND Load":
                            job_content = f"dbt build --select {target_schema}.{target_table}\n"

                        elif model_type == "DW Load":
                            job_content = f"dbt build --select {target_schema}.{target_table}\n"

                            # Add any merge and insert operations that follow
                            for subsequent_pipeline in sorted_pipelines:
                                subsequent_type = subsequent_pipeline.get("Model Type", "")
                                if subsequent_type == "Merge Macro":
                                    macro_name = f"MAC_{target_schema}_{target_table}_MERGE"
                                    job_content += f"dbt run-operation {macro_name}\n"
                                elif subsequent_type == "Insert Macro":
                                    macro_name = f"MAC_{target_schema}_{target_table}_INSERT"
                                    job_content += f"dbt run-operation {macro_name}\n"

                        elif model_type == "DP Load":
                            job_content = f"dbt build --select {target_schema}.{target_table}\n"


                # Write job file if we have content
                if job_content:
                    job_name = f"{target_schema}_{target_table}"
                    job_file_path = os.path.join(output_dir, f"{job_name}.dbt")
                    with open(job_file_path, 'w') as f:
                        f.write(job_content)
                    logging.info(f"Created DBT job file for {target_model}: {job_file_path}")

            except Exception as e:
                logging.error(f"Error processing target model group {target_model}: {e}")
                continue

        logging.info("DBT job generation completed")

    except Exception as e:
        logging.error(f"Fatal error in job generation: {e}")
        raise
