import json
import os
import logging
from typing import Dict, List, Tuple
from openpyxl import load_workbook
import snowflake.connector

from scripts.utils.snowflake_utils import DatabaseConnection

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SourceColumnMapper:
    def __init__(self):
        self._db_connection = None
        self.audit_columns = {
            "DATA_SRC": lambda s, t: f"'{s}.{t}'",
            "CREATE_DT": lambda *_: "CURRENT_TIMESTAMP()",
            "CREATE_BY": lambda *_: "CAST(CURRENT_USER() AS VARCHAR(200))",
            "CREATE_PGM": lambda s, t: f"'{s}.{t}'",
            "UPDATE_DT": lambda *_: "CURRENT_TIMESTAMP()",
            "UPDATE_BY": lambda *_: "CAST(CURRENT_USER() AS VARCHAR(200))",
            "UPDATE_PGM": lambda s, t: f"'{s}.{t}'"
        }


    def update_source_mapping(self, workbook_path: str, mappings_dir: str, config_path: str) -> None:
        """
        Updates source columns using Snowflake and target details from JSON files
        """
        try:
            logger.info("Starting source mapping update process...")
            workbook = self._load_workbook(workbook_path)
            snowflake_config = self._load_snowflake_config(config_path)

            sheets_updated = False  # Track if any changes were made

            # Process each mapping sheet
            for sheet_name in workbook.sheetnames :
                logger.info(f"Processing sheet: {sheet_name}")
                mapping_sheet = workbook[sheet_name]
                json_file = self._find_mapping_file(sheet_name, mappings_dir)

                if json_file:
                    # Load mapping configuration
                    mapping_config = self._load_mapping_json(json_file)
                    source_info = self._get_source_info(mapping_config)

                    # Get Snowflake columns
                    if source_info['schema'] and source_info['table']:
                        columns = self._get_snowflake_columns(snowflake_config, source_info)

                        # Update both Excel and JSON
                        mapped_count, total_count = self._update_mapping_sheet(
                            mapping_sheet, mapping_config, columns, source_info
                        )

                        if mapped_count > 0:
                            sheets_updated = True
                            logger.info(f"Updated {sheet_name}: Mapped {mapped_count} out of {total_count} columns")

                        # Update JSON file
                        self._update_mapping_json(mapping_config, columns, json_file)

                        # Try to save after each sheet update
                        try:
                            workbook.save(workbook_path)
                            logger.info(f"Successfully saved updates for sheet: {sheet_name}")
                        except Exception as save_error:
                            logger.error(f"Error saving workbook after updating {sheet_name}: {str(save_error)}")
                            raise

            if sheets_updated:
                # Final save attempt
                try:
                    workbook.save(workbook_path)
                    logger.info("Successfully saved all updates to workbook")
                except Exception as final_save_error:
                    logger.error(f"Error during final workbook save: {str(final_save_error)}")
                    raise
            else:
                logger.warning("No updates were made to any sheets in the workbook")

            logger.info("Source mapping update process completed successfully")

        except Exception as e:
            logger.error(f"Error in update_source_mapping: {str(e)}")
            raise

    def _update_mapping_sheet(self, mapping_sheet, mapping_config: Dict,
                              snowflake_columns: List[Dict], source_info: Dict) -> Tuple[int, int]:
        """Updates the mapping sheet with source information"""
        if not snowflake_columns:
            logger.error("No Snowflake columns provided")
            return 0, 0

        mapped_count = 0
        total_count = 0
        start_row = 2  # Start from row 2 assuming row 1 is header

        # Get source and target table info from mapping config
        source_table = mapping_config['Source']['Table Name']
        target_table = mapping_config['Target']['Table Name']

        # Define audit column transformations
        audit_mappings = {
            'DATA_SRC': lambda: f"'{target_table}'",
            'CREATE_DT': lambda: "CURRENT_TIMESTAMP()",
            'CREATE_BY': lambda: "CAST(CURRENT_USER() AS VARCHAR(200))",
            'CREATE_PGM': lambda: f"'{target_table}'",
            'UPDATE_BY': lambda: "CAST(CURRENT_USER() AS VARCHAR(200))",
            'UPDATE_DT': lambda: "CURRENT_TIMESTAMP()",
            'UPDATE_PGM': lambda: f"'{target_table}'"
        }

        logger.info(f"Processing sheet with {mapping_sheet.max_row} rows")

        try:
            for row in range(start_row, mapping_sheet.max_row + 1):
                target_column = mapping_sheet.cell(row=row, column=2).value

                if not target_column:
                    continue

                total_count += 1
                target_column = target_column.upper()

                try:
                    # Handle audit columns
                    if target_column in audit_mappings:
                        # Column 2: Target column (already set)
                        # Column 3: Source Schema.Table
                        mapping_sheet.cell(row=row, column=3).value = source_table
                        # Column 4: Transformation logic
                        mapping_sheet.cell(row=row, column=4).value = audit_mappings[target_column]()
                        mapped_count += 1
                        logger.info(f"Mapped audit column: {target_column} in row {row}")
                    else:
                        # Look for matching column in source columns
                        matching_col = next(
                            (col for col in snowflake_columns if col['name'].upper() == target_column),
                            None
                        )

                        if matching_col:
                            # Column 2: Target column (already set)
                            mapping_sheet.cell(row=row, column=3).value = source_table
                            mapping_sheet.cell(row=row, column=4).value = matching_col['name']
                            mapped_count += 1
                            logger.info(f"Mapped column: {target_column} -> {matching_col['name']} in row {row}")

                except Exception as row_error:
                    logger.error(f"Error processing row {row}: {str(row_error)}")
                    continue

        except Exception as e:
            logger.error(f"Error processing mapping sheet: {str(e)}")
            raise

        logger.info(f"Mapped {mapped_count} out of {total_count} columns")
        return mapped_count, total_count

    def _load_workbook(self, workbook_path: str):
        """Loads the Excel workbook"""
        try:
            logger.info(f"Loading workbook: {workbook_path}")
            if not os.path.exists(workbook_path):
                raise FileNotFoundError(f"Workbook not found: {workbook_path}")
            return load_workbook(workbook_path)
        except Exception as e:
            logger.error(f"Error loading workbook: {str(e)}")
            raise

    def _find_mapping_file(self, sheet_name: str, mappings_dir: str) -> str:
        """
        Finds corresponding JSON mapping file for the sheet

        Args:
            sheet_name: Name of the Excel sheet
            mappings_dir: Directory containing mapping JSON files

        Returns:
            str: Path to the JSON mapping file if found, None otherwise
        """
        try:
            # First try direct match with sheet name
            json_file = os.path.join(mappings_dir, f"{sheet_name}.json")
            if os.path.exists(json_file):
                logger.info(f"Found mapping file using direct match: {json_file}")
                return json_file

            # Try with _Map suffix if not already present
            if not sheet_name.endswith('_Map'):
                json_file = os.path.join(mappings_dir, f"{sheet_name}_Map.json")
                if os.path.exists(json_file):
                    logger.info(f"Found mapping file with _Map suffix: {json_file}")
                    return json_file

            # Try without _Map suffix if present
            if sheet_name.endswith('_Map'):
                base_name = sheet_name.replace('_Map', '')
                json_file = os.path.join(mappings_dir, f"{base_name}_Map.json")
                if os.path.exists(json_file):
                    logger.info(f"Found mapping file using base name: {json_file}")
                    return json_file

            logger.error(f"No mapping file found for sheet: {sheet_name}")
            return None

        except Exception as e:
            logger.error(f"Error finding mapping file for {sheet_name}: {str(e)}")
            return None

    def _load_mapping_json(self, json_file: str) -> Dict:
        """Loads and returns the mapping configuration from JSON file"""
        try:
            logger.info(f"Loading mapping JSON: {json_file}")
            with open(json_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading mapping JSON: {str(e)}")
            raise

    def _load_snowflake_config(self, config_path: str) -> Dict:
        """Loads Snowflake configuration from JSON file"""
        try:
            logger.info(f"Loading Snowflake config: {config_path}")
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Config file not found: {config_path}")
            with open(config_path, 'r') as f:
                config = json.load(f)
                # Keep the keys in uppercase to match the expected format
                return {
                    'USER': config.get('USER'),
                    'AUTHENTICATOR': config.get('AUTHENTICATOR'),
                    'ACCOUNT': config.get('ACCOUNT'),
                    'WAREHOUSE': config.get('WAREHOUSE'),
                    'DATABASE': config.get('DATABASE'),
                    'ROLE': config.get('ROLE')
                }
        except Exception as e:
            logger.error(f"Error loading Snowflake config: {str(e)}")
            raise

    def _get_source_info(self, mapping_config: Dict) -> Dict:
        """Extracts source table information from mapping configuration"""
        try:
            source = mapping_config.get('Source', {})
            schema = source.get('Schema', '')
            table = source.get('Table Name', '')
            if schema and table:
                return {
                    'schema': schema,
                    'table': table
                }
            logger.warning(f"Invalid source configuration: {source}")
            return {'schema': '', 'table': ''}
        except Exception as e:
            logger.error(f"Error extracting source info: {str(e)}")
            return {'schema': '', 'table': ''}

    def _get_target_info(self, mapping_config: Dict) -> Dict:
        """Extracts source table information from mapping configuration"""
        try:
            source = mapping_config.get('Target', {})
            schema = source.get('Schema', '')
            table = source.get('Table Name', '')
            if schema and table:
                return {
                    'schema': schema,
                    'table': table
                }
            logger.warning(f"Invalid source configuration: {source}")
            return {'schema': '', 'table': ''}
        except Exception as e:
            logger.error(f"Error extracting source info: {str(e)}")
            return {'schema': '', 'table': ''}

    def _get_snowflake_connection(self, snowflake_config: Dict):
        """Get or create Snowflake connection using DatabaseConnection class"""
        if self._db_connection is None:
            self._db_connection = DatabaseConnection(config=snowflake_config)
        return self._db_connection.get_connection()

    def _get_snowflake_columns(self, snowflake_config: Dict, source_info: Dict) -> List[Dict]:
        """Retrieves column information from Snowflake"""
        if not source_info['schema'] or not source_info['table']:
            logger.error(f"Invalid source info: schema={source_info['schema']}, table={source_info['table']}")
            return []

        try:
            db_connection = DatabaseConnection(config=snowflake_config)

            # Use a context manager to ensure proper cursor handling
            with db_connection.get_cursor() as cursor:
                query = """
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                """
                logger.info(f"Executing Snowflake query for {source_info['schema']}.{source_info['table']}")
                cursor.execute(query, (source_info['schema'], source_info['table']))
                columns = [
                    {
                        'name': row[0],
                        'data_type': row[1],
                        'is_nullable': row[2]
                    }
                    for row in cursor.fetchall()
                ]
                logger.info(f"Retrieved {len(columns)} columns from Snowflake")
                return columns

        except Exception as e:
            logger.error(f"Error getting Snowflake columns: {str(e)}")
            raise

    def close_connection(self):
        """Close Snowflake connection when done"""
        if self._db_connection:
            self._db_connection.disconnect()
            self._db_connection = None

    def _update_mapping_json(self, mapping_config: Dict, snowflake_columns: List[Dict], json_file: str) -> None:
        """Updates the mapping JSON file with source column information"""
        try:
            # Validate input mapping configuration
            if not mapping_config or 'Source' not in mapping_config or 'Target' not in mapping_config:
                logger.error("Invalid mapping configuration")
                return

            # Extract source and target table information
            source_table = mapping_config['Source']['Table Name']
            target_table = mapping_config['Target']['Table Name']

            # Define audit column transformations
            audit_mappings = {
                'DATA_SRC': f"'{target_table}'",
                "CREATE_DT": "CURRENT_TIMESTAMP()",
                'CREATE_BY': "CAST(CURRENT_USER() AS VARCHAR(200))",
                'CREATE_PGM': f"'{target_table}'",
                'UPDATE_BY': "CAST(CURRENT_USER() AS VARCHAR(200))",
                "UPDATE_DT": "CURRENT_TIMESTAMP()",
                'UPDATE_PGM': f"'{target_table}'"
            }

            # Create a lookup dictionary for snowflake columns
            snowflake_lookup = {col['name'].upper(): col for col in snowflake_columns}

            # Update column mappings
            for column in mapping_config['Columns']:
                target_column = column.get('Target Column', '').upper()
                if not target_column:
                    continue

                # Handle audit columns
                if target_column in audit_mappings:
                    column.update({
                        'Source Table': source_table,
                        'Logic': audit_mappings[target_column]
                    })
                else:
                    # Look for matching column in source
                    matching_col = snowflake_lookup.get(target_column)
                    if matching_col:
                        column.update({
                            'Source Table': source_table,
                            'Logic': matching_col['name']
                        })

            # Write updated config back to file
            with open(json_file, 'w') as f:
                json.dump(mapping_config, f, indent=2)
                logger.info(f"Successfully updated mapping JSON file: {json_file}")

        except Exception as e:
            logger.error(f"Error updating mapping JSON: {str(e)}")
            raise

def update_source_mappings(workbook_path: str, mappings_dir: str, config_path: str):
    """
    Updates source mappings using both Snowflake and JSON files
    Args:
        workbook_path: Path to the Excel workbook
        mappings_dir: Directory containing the JSON mapping files
        config_path: Path to Snowflake config JSON file
    """
    try:
        if not os.path.exists(workbook_path):
            raise FileNotFoundError(f"Workbook not found: {workbook_path}")
        if not os.path.exists(mappings_dir):
            raise FileNotFoundError(f"Mappings directory not found: {mappings_dir}")
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        logger.info(f"Starting source mapping update process...")
        logger.info(f"Workbook: {workbook_path}")
        logger.info(f"Mappings directory: {mappings_dir}")
        logger.info(f"Config file: {config_path}")

        mapper = SourceColumnMapper()
        mapper.update_source_mapping(workbook_path, mappings_dir, config_path)

        logger.info("Source mapping update process completed successfully")
    except Exception as e:
        logger.error(f"Error in update_source_mappings: {str(e)}")
        raise

if __name__ == "__main__":
    # Example usage
    workbook_path = "path/to/your/workbook.xlsx"
    mappings_dir = "path/to/your/mappings/directory"
    config_path = "path/to/your/config.json"
    update_source_mappings(workbook_path, mappings_dir, config_path)
