import os
import json
from scripts.utils import extract_table_name, parse_ddl_file
import re

def create_dbt_model_from_json(config_file, mapping_sheet=None):
    """Generate a DBT model file from JSON configuration"""
    with open(config_file) as f:
        config = json.load(f)

    # Extract source information
    source_type = config['Source']['Type'].lower()
    source_db = config['Source']['Database']
    source_schema = config['Source']['Schema']
    source_table = config['Source']['Table Name']
    source_name = config['Source']['Name']

    # Use "source" as the alias for the main table
    main_table_alias = 'source'

    # Build the FROM clause based on source type
    if source_type == 'source' or source_type == 'src':
        from_clause = f"{{{{ source('{source_name}', '{source_table}') }}}} AS {main_table_alias}"
    else:
        # For ref type, combine schema and table
        ref_path = f"{source_schema}.{source_table}"
        from_clause = f"{{{{ ref('{ref_path}') }}}} AS {main_table_alias}"

    # Get materialization type
    materialization = config['Target']['materialization']
    
    # Check if minus logic is required
    minus_logic_required = False
    if mapping_sheet:
        for row in range(1, 10):  # Check first few rows
            if mapping_sheet.cell(row=row, column=1).value == 'MINUS_LOGIC_REQUIRED':
                minus_value = mapping_sheet.cell(row=row, column=2).value
                if minus_value and minus_value.upper() == 'Y':
                    minus_logic_required = True
                break
    
    # Build model config with appropriate settings
    model_config = f"""{{{{ config(
    materialized='{materialization}',
    schema='{config['Target']['Schema']}'"""
    
    # Add pre-hook for truncate_load
    if materialization == 'truncate_load':
        # Use table instead of incremental materialization with truncate pre-hook
        model_config = model_config.replace("materialized='truncate_load'", "materialized='table'")
        target_table = f"{config['Target']['Schema']}.{config['Target']['Table Name']}"
        model_config += f""",
    pre_hook=\"\"\"
        TRUNCATE TABLE {target_table}
    \"\"\""""
    
    # Add unique keys for incremental models if provided
    unique_keys = []
    if materialization == 'incremental' and 'unique_key' in config['Target']:
        unique_keys = config['Target']['unique_key']
        if isinstance(unique_keys, list):
            # Format list of keys as a comma-separated string
            unique_key_str = ', '.join([f'"{key}"' for key in unique_keys])
            model_config += f""",
    unique_key=[{unique_key_str}]"""
        else:
            # Single key
            model_config += f""",
    unique_key="{unique_keys}\""""
            unique_keys = [unique_keys]  # Convert to list for later use
        
        # Print debug info
        print(f"Added unique key to model config: {unique_keys}")
        
        # Add merge_update_columns for incremental models
        # Get excluded columns from mapping sheet
        excluded_columns = ["CREATE_DT", "CREATE_BY", "CREATE_PGM"]  # Default excluded columns
        if mapping_sheet:
            for row in range(1, 10):  # Check first few rows
                if mapping_sheet.cell(row=row, column=1).value == 'MERGE_UPDATE_EXCLUDE_COLUMNS':
                    exclude_value = mapping_sheet.cell(row=row, column=2).value
                    if exclude_value:
                        # Parse comma-separated list of columns to exclude
                        excluded_columns = [col.strip() for col in exclude_value.split(',')]
                    break
        
        # Add unique keys to excluded columns
        excluded_columns.extend(unique_keys)
        
        # This includes all columns except excluded columns and unwanted columns
        update_columns = []
        for column in config['Columns']:
            target_col = column['Target Column']
            # Skip unwanted columns and excluded columns
            if (target_col in ["List (Y,N)", "Table Type", "ref"] or 
                target_col in excluded_columns or "=" in column['Logic']):
                continue
            
            # Add the column to the update_columns list
            update_columns.append(target_col)
        
        if update_columns:
            # Format the list of columns as a comma-separated string with single quotes
            update_columns_str = ', '.join([f"'{col}'" for col in update_columns])
            model_config += f""",
    merge_update_columns = [{update_columns_str}]"""
            # Print debug info with the actual columns that are added to the model configuration
            formatted_columns = [f"'{col}'" for col in update_columns]
            print(f"Added merge_update_columns to model config: {formatted_columns}")
    elif materialization == 'incremental':
        # If no unique key is provided but model is incremental, add a warning comment
        model_config += """
    /* WARNING: No unique_key specified for incremental model.
       This may cause duplicate records. Please specify a unique_key. */"""
        print("WARNING: No unique_key specified for incremental model.")
    
    # Close the config block
    model_config += """
)}}"""

    # Build model content
    model_content = f"""{model_config}

-- Model: {config['Target']['Schema']}.{config['Target']['Table Name']}
-- Source: {source_db}.{source_schema}.{source_table}

SELECT
"""

    # Add column mappings
    for column in config['Columns']:
        target_col = column['Target Column']
        logic = column['Logic']
        
        # Skip unwanted columns
        if target_col in ["List (Y,N)", "Table Type", "ref"] or "=" in logic:
            continue
        
        # Handle special cases for column names with spaces or special characters
        quoted_target = target_col
        if ' ' in target_col or '(' in target_col or ')' in target_col:
            # For columns with spaces or special characters, use quotes
            quoted_target = f'"{target_col}"'
        
        # Use the logic exactly as written without adding table aliases
        model_content += f"    {logic} as {quoted_target},\n"

    # Remove trailing comma and add FROM clause
    model_content = model_content.rstrip(',\n')
    model_content += f"\nFROM {from_clause}"
    
    # Add JOIN clauses if mapping_sheet is provided and contains joins
    if mapping_sheet:
        join_clauses = extract_join_clauses(mapping_sheet, main_table_alias)
        if join_clauses:
            model_content += "\n" + "\n".join(join_clauses)
        
        # Add WHERE clause if provided
        where_condition = extract_where_condition(mapping_sheet, main_table_alias)
        if where_condition:
            model_content += f"\nWHERE {where_condition}"
        
        # Add GROUP BY clause if provided
        group_by = extract_group_by(mapping_sheet, main_table_alias)
        if group_by:
            model_content += f"\nGROUP BY {group_by}"
    
    # Add minus logic if required
    if minus_logic_required:
        # Define audit columns to exclude from the MINUS operation
        audit_columns = [
            "DATA_SRC", "CREATE_DT", "CREATE_BY", "CREATE_PGM", 
            "UPDATE_DT", "UPDATE_BY", "UPDATE_PGM"
        ]
        
        # Create a new model content with the subquery structure
        final_model_content = f"""{model_config}

-- Model: {config['Target']['Schema']}.{config['Target']['Table Name']}
-- Source: {source_db}.{source_schema}.{source_table}

SELECT
"""
        
        # Identify columns that should be excluded from the MINUS subquery
        # These are columns that don't match between target and source
        target_only_columns = []
        computed_columns = []
        unique_key_columns = []
        primary_key_columns = []  # For columns like OPCO_ID that should be in the outer query
        
        # First, identify primary key columns (like OPCO_ID)
        for column in config['Columns']:
            target_col = column['Target Column']
            if target_col.endswith('_ID') and target_col not in audit_columns:
                primary_key_columns.append(column)
                print(f"Moving primary key column to outer query: {target_col}")
        
        for column in config['Columns']:
            target_col = column['Target Column']
            logic = column['Logic']
            source_col = column.get('Source Table', '')
            
            # Skip columns already identified as primary keys
            if column in primary_key_columns:
                continue
                
            # Check if this is a unique key column
            if target_col in unique_keys:
                unique_key_columns.append(column)
                continue
                
            # More generic approach to identify columns that should be excluded from MINUS:
            # 1. Columns with empty source (target-only columns)
            # 2. Columns with literals/constants (containing quotes)
            # 3. Columns with functions (containing parentheses)
            # 4. Columns with complex expressions (containing operators)
            # 5. Columns with CASE statements
            if (target_col not in audit_columns and
                (not source_col or  # No source column
                 "'" in logic or    # Contains literal string
                 "(" in logic or    # Contains function call
                 " " in logic.strip() or  # Contains spaces (likely an expression)
                 any(op in logic for op in ["+", "-", "*", "/", "||"]) or  # Contains operators
                 "CASE" in logic.upper())):  # Contains CASE statement
                if target_col not in audit_columns:
                    computed_columns.append(column)
                    print(f"Excluding column from MINUS: {target_col} (Logic: {logic})")
        
        # Add all computed columns and audit columns to the outer query
        # First add computed columns
        for column in computed_columns:
            target_col = column['Target Column']
            logic = column['Logic']
            
            # Handle special cases for column names with spaces or special characters
            quoted_target = target_col
            if ' ' in target_col or '(' in target_col or ')' in target_col:
                # For columns with spaces or special characters, use quotes
                quoted_target = f'"{target_col}"'
            
            # Use the logic exactly as written
            final_model_content += f"    {logic} AS {quoted_target},\n"
            
        # Add primary key columns next
        for column in primary_key_columns:
            target_col = column['Target Column']
            logic = column['Logic']
            
            # Handle special cases for column names with spaces or special characters
            quoted_target = target_col
            if ' ' in target_col or '(' in target_col or ')' in target_col:
                # For columns with spaces or special characters, use quotes
                quoted_target = f'"{target_col}"'
            
            # Use the logic exactly as written
            final_model_content += f"    {logic} AS {quoted_target},\n"
            
        # Add unique key columns next
        for column in unique_key_columns:
            target_col = column['Target Column']
            logic = column['Logic']
            
            # Handle special cases for column names with spaces or special characters
            quoted_target = target_col
            if ' ' in target_col or '(' in target_col or ')' in target_col:
                # For columns with spaces or special characters, use quotes
                quoted_target = f'"{target_col}"'
            
            # Use the logic exactly as written
            final_model_content += f"    {logic} AS {quoted_target},\n"
        
        # Then add audit columns
        for column in config['Columns']:
            target_col = column['Target Column']
            logic = column['Logic']
            
            # Only include audit columns
            if target_col not in audit_columns:
                continue
            
            # Handle special cases for column names with spaces or special characters
            quoted_target = target_col
            if ' ' in target_col or '(' in target_col or ')' in target_col:
                # For columns with spaces or special characters, use quotes
                quoted_target = f'"{target_col}"'
            
            # Use the logic exactly as written
            final_model_content += f"    {logic} AS {quoted_target},\n"
        
        # Add * to include all columns from the subquery
        final_model_content += "    *\nFROM\n(\n"
        
        # Create a list of columns for the MINUS comparison (excluding computed, audit, unique key, and primary key columns)
        minus_columns = []
        for column in config['Columns']:
            target_col = column['Target Column']
            logic = column['Logic']
            
            # Skip unwanted columns, audit columns, computed columns, unique key columns, and primary key columns
            if (target_col in ["List (Y,N)", "Table Type", "ref"] or "=" in logic or
                target_col in audit_columns or column in computed_columns or
                target_col in unique_keys or column in primary_key_columns):
                continue
                
            minus_columns.append(column)
        
        # Start the subquery
        subquery_content = "SELECT\n"
        
        # Add columns for the main query (only those that will be in the MINUS)
        for column in minus_columns:
            target_col = column['Target Column']
            
            # Handle special cases for column names with spaces or special characters
            quoted_target = target_col
            if ' ' in target_col or '(' in target_col or ')' in target_col:
                # For columns with spaces or special characters, use quotes
                quoted_target = f'"{target_col}"'
            
            # Use the target column name directly
            subquery_content += f"    {quoted_target},\n"
        
        # Remove trailing comma
        subquery_content = subquery_content.rstrip(',\n')
        
        # Add FROM clause for the subquery
        subquery_content += f"\nFROM {from_clause}"
        
        # Add JOIN clauses if mapping_sheet is provided and contains joins
        if mapping_sheet:
            join_clauses = extract_join_clauses(mapping_sheet, main_table_alias)
            if join_clauses:
                subquery_content += "\n" + "\n".join(join_clauses)
            
            # Add WHERE clause if provided
            where_condition = extract_where_condition(mapping_sheet, main_table_alias)
            if where_condition:
                subquery_content += f"\nWHERE {where_condition}"
        
        # Add MINUS section
        subquery_content += f"\n\nMINUS\n\nSELECT\n"
        
        # Add the same columns to the MINUS part for exact matching
        for column in minus_columns:
            target_col = column['Target Column']
            
            # Handle special cases for column names with spaces or special characters
            quoted_target = target_col
            if ' ' in target_col or '(' in target_col or ')' in target_col:
                # For columns with spaces or special characters, use quotes
                quoted_target = f'"{target_col}"'
            
            # For the MINUS part, use the target column name directly
            subquery_content += f"    {quoted_target},\n"
        
        # Remove trailing comma
        subquery_content = subquery_content.rstrip(',\n')
        
        # Add the target table reference for the MINUS part
        target_table_ref = f"{{{{ source('{config['Target']['Schema']}','{config['Target']['Table Name']}') }}}}"
        subquery_content += f"\nFROM {target_table_ref}"
        
        # Add the subquery with proper indentation
        indented_subquery = "\n".join(["    " + line for line in subquery_content.split("\n")])
        final_model_content += indented_subquery
        
        # Close the subquery
        final_model_content += "\n)"
        
        # Replace the original model content with the new one
        model_content = final_model_content
    
    # Create output directory if it doesn't exist
    output_dir = 'models'
    os.makedirs(output_dir, exist_ok=True)

    # Generate file name
    model_name = f"{config['Target']['Schema']}.{config['Target']['Table Name']}"
    file_name = f"{model_name}.sql"
    file_path = os.path.join(output_dir, file_name)

    # Write model file
    with open(file_path, 'w') as f:
        f.write(model_content)

    return file_path

def extract_join_clauses(mapping_sheet, main_table_alias='source'):
    """Extract join clauses from mapping sheet"""
    join_clauses = []
    
    # Find the JOIN_TABLES section
    join_section_row = None
    for row in range(1, mapping_sheet.max_row + 1):
        if mapping_sheet.cell(row=row, column=1).value == 'JOIN_TABLES':
            join_section_row = row
            break
    
    if not join_section_row:
        return []
    
    # Join table headers are in the next row
    join_header_row = join_section_row + 1
    
    # Process join tables (starting from the row after headers)
    for row in range(join_header_row + 1, mapping_sheet.max_row + 1):
        join_type = mapping_sheet.cell(row=row, column=1).value
        table_type = mapping_sheet.cell(row=row, column=2).value
        source_name = mapping_sheet.cell(row=row, column=3).value
        table_name = mapping_sheet.cell(row=row, column=4).value
        alias = mapping_sheet.cell(row=row, column=5).value
        join_condition = mapping_sheet.cell(row=row, column=6).value
        
        # Stop when we reach an empty row or a new section
        if not join_type or not table_name:
            next_row = row + 1
            if next_row <= mapping_sheet.max_row:
                next_value = mapping_sheet.cell(row=next_row, column=1).value
                if next_value and next_value in ['WHERE_CONDITIONS', 'GROUP BY']:
                    break
            continue
        
        # Build the join clause
        join_clause = f"{join_type} JOIN "
        
        # Add the table reference based on type
        if table_type and table_type.lower() == 'source' and source_name:
            join_clause += f"{{{{ source('{source_name}', '{table_name}') }}}}"
        else:
            join_clause += f"{{{{ ref('{table_name}') }}}}"
        
        # Add alias if provided
        if alias:
            join_clause += f" AS {alias}"
        
        # Add join condition
        if join_condition:
            # Replace table aliases if needed
            join_condition = join_condition.replace("main.", f"{main_table_alias}.")
            
            # Add source alias to column references if not already present
            # This regex finds column names that don't have a table prefix
            import re
            column_pattern = r'(?<![a-zA-Z0-9_\.])([a-zA-Z0-9_]+)(?=\s*=)'
            
            def add_source_alias(match):
                col = match.group(1)
                # Skip adding alias to literals or functions
                if col.upper() in ['AND', 'OR', 'ON', 'NULL']:
                    return col
                return f"{main_table_alias}.{col}"
            
            join_condition = re.sub(column_pattern, add_source_alias, join_condition)
            join_clause += f" ON {join_condition}"
        
        join_clauses.append(join_clause)
    
    return join_clauses

def extract_where_condition(mapping_sheet, main_table_alias='source'):
    """Extract WHERE condition from mapping sheet"""
    # Find the WHERE_CONDITIONS section
    where_section_row = None
    for row in range(1, mapping_sheet.max_row + 1):
        if mapping_sheet.cell(row=row, column=1).value == 'WHERE_CONDITIONS':
            where_section_row = row
            break
    
    if not where_section_row:
        return None
    
    # Get the condition from the merged cell
    where_condition = mapping_sheet.cell(row=where_section_row, column=2).value
    
    if where_condition:
        # Replace table aliases if needed
        where_condition = where_condition.replace("main.", f"{main_table_alias}.")
        return where_condition
    
    return None

def extract_group_by(mapping_sheet, main_table_alias='source'):
    """Extract GROUP BY clause from mapping sheet"""
    # Find the GROUP BY section
    group_by_row = None
    for row in range(1, mapping_sheet.max_row + 1):
        if mapping_sheet.cell(row=row, column=1).value == 'GROUP BY':
            group_by_row = row
            break
    
    if not group_by_row:
        return None
    
    # Get the group by columns from the merged cell
    group_by = mapping_sheet.cell(row=group_by_row, column=2).value
    
    if group_by:
        # Replace table aliases if needed
        group_by = group_by.replace("main.", f"{main_table_alias}.")
        return group_by
    
    return None

def get_column_mappings(mapping_sheet):
    """Extract column mappings from mapping sheet"""
    mappings = []
    header_row = 9  # Row where column mapping starts (updated for new layout)
    
    # Find the header row
    for row in range(1, mapping_sheet.max_row + 1):
        if mapping_sheet.cell(row=row, column=1).value == 'S.NO':
            header_row = row
            break
    
    # Find the JOIN_TABLES section to determine where column mappings end
    join_section_row = None
    for row in range(1, mapping_sheet.max_row + 1):
        if mapping_sheet.cell(row=row, column=1).value == 'JOIN_TABLES':
            join_section_row = row
            break
    
    # If JOIN_TABLES section not found, use all rows
    end_row = join_section_row - 1 if join_section_row else mapping_sheet.max_row
    
    for row in range(header_row + 1, end_row + 1):
        target_col = mapping_sheet.cell(row=row, column=2).value
        source_col = mapping_sheet.cell(row=row, column=3).value
        logic = mapping_sheet.cell(row=row, column=4).value
        
        if target_col:
            mappings.append({
                'target_column': target_col,
                'source_column': source_col,
                'logic': logic
            })
    
    return mappings

def get_audit_columns():
    """Get standard audit columns and their logic"""
    return [
        {
            'column': 'DW_CREATED_DATE',
            'logic': "CURRENT_TIMESTAMP()"
        },
        {
            'column': 'DW_CREATED_BY',
            'logic': "'DBT_BUILD_AUTOMATION'"
        },
        {
            'column': 'DW_MODIFIED_DATE',
            'logic': "CURRENT_TIMESTAMP()"
        },
        {
            'column': 'DW_MODIFIED_BY',
            'logic': "'DBT_BUILD_AUTOMATION'"
        }
    ]

def get_source_type(mapping_sheet):
    """Get source type from mapping sheet"""
    for row in range(1, mapping_sheet.max_row + 1):
        if mapping_sheet.cell(row=row, column=1).value == 'SOURCE_TYPE':
            return mapping_sheet.cell(row=row, column=2).value
    return None

def get_source_name(mapping_sheet):
    """Get source name from mapping sheet"""
    for row in range(1, mapping_sheet.max_row + 1):
        if mapping_sheet.cell(row=row, column=1).value == 'SOURCE_NAME':
            return mapping_sheet.cell(row=row, column=2).value
    return None

def get_materialization(mapping_sheet):
    """Get materialization from mapping sheet"""
    for row in range(1, mapping_sheet.max_row + 1):
        if mapping_sheet.cell(row=row, column=1).value == 'MATERIALIZATION':
            return mapping_sheet.cell(row=row, column=2).value
    return 'incremental'  # Default to incremental if not specified

def extract_unique_keys_from_ddl(ddl_path):
    """Extract unique key constraints from DDL file"""
    try:
        with open(ddl_path, 'r') as file:
            ddl_content = file.read()
            
        # Look for UNIQUE KEY constraints
        unique_key_pattern = r'CONSTRAINT\s+\w+\s+UNIQUE\s+KEY\s*\(([^)]+)\)'
        unique_keys = []
        
        for match in re.finditer(unique_key_pattern, ddl_content, re.IGNORECASE):
            columns = match.group(1).split(',')
            # Clean up column names (remove quotes, trim whitespace)
            columns = [col.strip().strip('"').strip('`') for col in columns]
            unique_keys.extend(columns)
            
        return unique_keys
    except Exception:
        return []

def generate_model_content(config):
    """Generate model content from config"""
    # Extract source information
    source_type = config['Source']['Type'].lower()
    source_db = config['Source']['Database']
    source_schema = config['Source']['Schema']
    source_table = config['Source']['Table Name']
    source_name = config['Source']['Name']
    
    # Build the FROM clause based on source type
    if source_type == 'source' or source_type == 'src':
        from_clause = f"{{{{ source('{source_name}', '{source_table}') }}}}"
    else:
        # For ref type, combine schema and table
        ref_path = f"{source_schema}.{source_table}"
        from_clause = f"{{{{ ref('{ref_path}') }}}}"
    
    # Build model content
    model_content = f"""{{{{ config(
    materialized='{config['Target']['materialization']}',
    schema='{config['Target']['Schema']}'
)}}}}

-- Model: {config['Target']['Schema']}.{config['Target']['Table Name']}
-- Source: {source_db}.{source_schema}.{source_table}

SELECT
"""

    # Add column mappings
    for column in config['Columns']:
        target_col = column['Target Column']
        logic = column['Logic']
        
        # Skip unwanted columns
        if target_col in ["List (Y,N)", "Table Type", "ref"] or "=" in logic:
            continue
        
        # Handle special cases for column names with spaces or special characters
        quoted_target = target_col
        if ' ' in target_col or '(' in target_col or ')' in target_col:
            # For columns with spaces or special characters, use quotes
            quoted_target = f'"{target_col}"'
        
        # Use the logic exactly as written without adding table aliases
        model_content += f"    {logic} as {quoted_target},\n"

    # Remove trailing comma and add FROM clause
    model_content = model_content.rstrip(',\n')
    model_content += f"\nFROM {from_clause}"
    
    return model_content
