import os
import logging
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.comments import Comment
from openpyxl.utils import get_column_letter

from ..dbt_model_generator import create_dbt_model_from_json
from ..generate_lnd_dbt_model_file import generate_lnd_dbt_model_file
from ..insert_sql_generator import insert_sql_generator
from ..merge_sql_generator import merge_sql_generator, generate_merge_sql_for_pipelines
from ..utils import parse_ddl_file as parse_ddl_file
from ..excel_to_json import _extract_config_from_sheet, _extract_mappings_from_sheet
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from openpyxl import load_workbook
import json
from .source_column_mapper import update_source_mappings as update_source_mappings


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
        1: "Pipeline Name",
        2: "Model Type",
        3: "Source Model",
        4: "Target Model",
        5: "Execution Order",
        6: "Generated Mapping Sheet"
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
        "Pipeline Name", "Model Type", "Source Model", "Target Model", "Execution Order", "Generated Mapping Sheet"
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
        formula1='"LND Load, DW Load, DP Load, Merge Macro, Insert Macro"',
        allow_blank=True
    )
    pipeline_config_sheet.add_data_validation(model_type_dv)
    model_type_dv.add(pipeline_config_sheet.cell(row=3, column=2))

    # Adjust column widths for better readability
    for col_num in range(1, len(headers) + 1):
        column_letter = get_column_letter(col_num)
        pipeline_config_sheet.column_dimensions[column_letter].width = 25


def create_mapping_sheet(sheet, pipeline, columns):
    # Extract table name from pipeline
    table_name = pipeline.get('Target Model', 'Unknown')

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
        ('MINUS_LOGIC_REQUIRED', 'N'),
        ('TRANSIENT_TABLE', 'N')
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

    # Add column mapping section
    headers = ['S.NO', 'TargetColumn', 'Source Table', 'Logic/Mapping/Constant Value']
    for col, header in enumerate(headers, start=1):
        cell = sheet.cell(row=current_row, column=col, value=header)
        cell.font = Font(bold=True, color='FFFFFF')
        cell.fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    header_row = current_row
    # Add the data - Filter out any non-column entries like constraints
    valid_columns = []
    for column_name, definition in columns:
        # Skip if column_name is "UNIQUE", "PRIMARY", "CONSTRAINT", or any other non-column entry
        if (column_name.upper() in ["UNIQUE", "PRIMARY", "CONSTRAINT"] or
                column_name.isdigit() or  # Skip numeric entries
                column_name.endswith(')') or  # Skip entries ending with )
                not column_name.strip()):  # Skip empty entries
            continue

        valid_columns.append((column_name, definition))

    # Now add the valid columns to the sheet
    for i, (column_name, _) in enumerate(valid_columns, start=1):
        row = header_row + i
        sheet.cell(row=row, column=1, value=i)  # S.NO
        sheet.cell(row=row, column=2, value=column_name)  # TargetColumn
        current_row = row

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
            formula1='"source,ref"',
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

    # Read pipeline configurations
    pipelines = read_pipeline_sheet(pipeline_details_sheet)
    config_sheet = workbook.create_sheet(title=f"config")
    create_config_sheet(config_sheet)

    # Dictionary to keep track of count for each table
    table_counter = {}

    # Model type abbreviations
    type_abbrev = {
        'Merge': 'M',
        'Insert': 'I',
        'DW': 'D'
    }

    # Create mapping sheets for each pipeline
    for pipeline_index, pipeline in enumerate(pipelines, start=2):  # start=2 because row 1 is header
        target_model = pipeline.get("Target Model", "Unknown")
        model_type = pipeline.get("Model Type", "Unknown")
        model_type_split = pipeline.get("Model Type", "Unknown").split()[0]
        target_model_split = target_model.split('.')[1]

        # Create mapping sheet only for specific model types
        if model_type in ['DW Load', 'Merge Macro', 'Insert Macro']:
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
            sheet_name = f"{type_short}{str(table_counter[table_key]).zfill(2)}_{short_target}_Map"

            mapping_sheet = workbook.create_sheet(title=sheet_name)
            columns = parse_ddl_file(ddl_path)
            create_mapping_sheet(mapping_sheet, pipeline, columns[0] if isinstance(columns, tuple) else columns)

            # Update the pipeline details sheet with the mapping sheet name directly using pipeline_index
            pipeline_details_sheet.cell(row=pipeline_index, column=6, value=sheet_name)

    workbook.save(workbook_path)
    return workbook


def process_filled_workbook_to_json(workbook_path: str) -> Tuple[Optional[Dict], Optional[List[Dict]]]:
    """
    Process an Excel workbook and extract configuration and mapping data to JSON files.
    Args:
        workbook_path: Path to the Excel workbook
    Returns:
        Tuple containing config_data and list of mapping_data
    """
    try:
        workbook = load_workbook(workbook_path)
        pipeline_details_sheet = workbook['Pipeline Details']
        config_data = None
        all_mapping_data = []

        # Add type abbreviation mapping
        type_abbrev = {
            'Merge': 'M',
            'Insert': 'I',
            'DW': 'D'
        }

        # Read pipeline configurations
        pipelines = read_pipeline_sheet(pipeline_details_sheet)

        config_data = _extract_config_from_sheet(workbook['config'])
        output_directory = Path("config")
        output_directory.mkdir(parents=True, exist_ok=True)
        file_path = output_directory / "config.json"

        try:
            with open(file_path, 'w') as f:
                json.dump(config_data, f, indent=2)
        except IOError as e:
            print(f"Error writing config file: {e}")

        # Create mapping sheets for each pipeline
        for pipeline in pipelines:
            try:
                target_sheet = pipeline.get("Generated Mapping Sheet")
                if not target_sheet:
                    print(f"Warning: Missing mapping sheet name for pipeline")
                    continue

                target_workbook = _load_workbook(workbook_path, target_sheet)
                target_model = pipeline.get("Target Model", "Unknown")
                model_type = pipeline.get("Model Type", "Unknown").split()[0]
                mapping_data = _extract_mappings_from_sheet(target_workbook)
                all_mapping_data.append(mapping_data)

                # Generate file name using the same convention as mapping sheet names
                target_model_split = target_model.split('.')[-1]
                type_short = type_abbrev.get(model_type, model_type[0])
                short_target = target_model_split[:15]  # Limit target name to 15 chars

                # Extract the counter number from the mapping sheet name
                counter = int(target_sheet.split('_')[0][1:3])  # Extract "XX" from "MXX_..."

                # Create file name with same pattern as sheet name
                file_name = f"{type_short}{str(counter).zfill(2)}_{short_target}_Map"

                # Write the extracted mappings to a JSON file
                output_directory = Path("config/mappings")
                output_directory.mkdir(parents=True, exist_ok=True)
                file_path = output_directory / f"{file_name}.json"

                with open(file_path, 'w') as f:
                    json.dump(mapping_data, f, indent=2)

            except Exception as e:
                print(f"Error processing pipeline {target_sheet}: {e}")
                continue

        return config_data, all_mapping_data

    except Exception as e:
        print(f"Error processing workbook: {e}")
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


def generate_dbt_models(workbook_path, output_dir, json_config_dir,target_ddl_path):
    """
    Controller function to generate different types of DBT models

    Args:
        workbook_path (str): Path to the Excel workbook
        output_dir (str): Directory to save generated models
        json_config_dir (str): Directory with JSON configuration files
    """
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

        # Process each pipeline
        for pipeline in pipelines:
            try:
                model_type = pipeline.get("Model Type", "Unknown")
                target_model = pipeline.get("Target Model", "Unknown")
                mapping_sheet_name = pipeline.get("Generated Mapping Sheet", "")

                # Find corresponding mapping sheet
                mapping_sheet = None
                if mapping_sheet_name and mapping_sheet_name in workbook.sheetnames:
                    mapping_sheet = workbook[mapping_sheet_name]

                # Find corresponding JSON config
                config_filename = f"{mapping_sheet_name}.json"
                config_file = os.path.join(json_config_dir, config_filename)

                if not os.path.exists(config_file):
                    logging.warning(f"No config file found for {target_model}")
                    continue

                # Generate model based on type
                model_content = None
                output_filename = f"{target_model.lower().replace('.', '_')}.sql"
                output_path = os.path.join(output_dir, output_filename)

                if "DW Load" in model_type:
                    model_content = create_dbt_model_from_json(
                        config_file=config_file,
                        mapping_sheet=mapping_sheet,
                        target_ddl_path=target_ddl_path
                    )

                elif "Merge Macro" in model_type:
                    model_content = generate_merge_sql_for_pipelines(
                        config_file=config_file
                    )

                elif "Insert Macro" in model_type:
                    model_content = insert_sql_generator(
                        config_file=config_file,
                        mapping_sheet=mapping_sheet
                    )

                elif "LND Load" in model_type:
                    model_content = generate_lnd_dbt_model_file(
                        config=config_file,
                        file_path=mapping_sheet
                    )

                else:
                    logging.warning(f"Unsupported model type: {model_type}")
                    continue

            except Exception as e:
                logging.error(f"Error processing {target_model}: {e}")
                continue

        logging.info("DBT model generation completed")

    except Exception as e:
        logging.error(f"Fatal error in model generation: {e}")
        raise
