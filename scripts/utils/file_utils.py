import re
import logging

def extract_table_name(ddl_path):
    """Extract table name from DDL file"""
    with open(ddl_path, 'r') as f:
        ddl_content = f.read()
    
    table_pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\"?(\w+)\"?\.)?"?(\w+)"?'
    match = re.search(table_pattern, ddl_content, re.IGNORECASE)
    
    if match:
        schema = match.group(1) or ''
        table = match.group(2)
        return f"{schema}.{table}".upper() if schema else table.upper()
    
    return "Unknown_Table"


def parse_ddl_file(ddl_path):
    """Parse DDL file to extract column names, data types, and default values"""
    try:
        with open(ddl_path, 'r') as file:
            ddl_content = file.read()

        logging.info(f"Parsing DDL file: {ddl_path}")

        # Find the create table statement
        create_table_pattern = r'CREATE\s+TABLE\s+.*?\((.*?)\)[^)]*$'
        match = re.search(create_table_pattern, ddl_content, re.DOTALL | re.IGNORECASE)

        if not match:
            raise Exception("Could not find CREATE TABLE statement in DDL file")

        columns_text = match.group(1)
        # Split by commas, but be careful with commas inside parentheses (for functions, etc.)
        column_definitions = []
        current_def = ""
        paren_level = 0

        for char in columns_text:
            if char == '(':
                paren_level += 1
                current_def += char
            elif char == ')':
                paren_level -= 1
                current_def += char
            elif char == ',' and paren_level == 0:
                column_definitions.append(current_def.strip())
                current_def = ""
            else:
                current_def += char

        # Add the last definition if it exists
        if current_def.strip():
            column_definitions.append(current_def.strip())

        columns = []
        unique_keys = []
        primary_keys = []

        # First pass: extract all column names
        all_column_names = []
        for definition in column_definitions:
            if not definition:
                continue

            # Skip constraints for now
            if re.match(r'^\s*(PRIMARY|FOREIGN|CONSTRAINT|UNIQUE|CHECK|INDEX)', definition, re.IGNORECASE):
                continue

            # Extract column name - the first word before any whitespace
            match = re.match(r'^\s*([^\s\(]+)', definition)
            if match:
                column_name = match.group(1).strip('"').strip('`')
                all_column_names.append(column_name)

        # Second pass: process columns and constraints
        for definition in column_definitions:
            if not definition:
                continue

            # Log the definition for debugging
            logging.debug(f"Processing definition: {definition}")

            # Handle primary key constraints
            if re.match(r'^\s*PRIMARY\s+KEY', definition, re.IGNORECASE):
                # Extract primary key columns
                pk_pattern = r'PRIMARY\s+KEY\s*\(([^)]+)\)'
                pk_match = re.search(pk_pattern, definition, re.IGNORECASE)
                if pk_match:
                    pk_columns = [col.strip().strip('"').strip('`') for col in pk_match.group(1).split(',')]
                    # Add primary key columns to primary keys list
                    primary_keys.extend(pk_columns)
                    logging.info(f"Found PRIMARY KEY: {pk_columns}")
                continue

            # Handle standalone UNIQUE constraints (not attached to a column)
            if re.match(r'^\s*UNIQUE', definition, re.IGNORECASE):
                # Extract unique key columns
                unique_pattern = r'UNIQUE\s*\(([^)]+)\)'
                unique_match = re.search(unique_pattern, definition, re.IGNORECASE)
                if unique_match:
                    key_columns = [col.strip().strip('"').strip('`') for col in unique_match.group(1).split(',')]
                    unique_keys.extend(key_columns)
                    logging.info(f"Found standalone UNIQUE constraint: {key_columns}")
                continue

            # Handle unique constraints with CONSTRAINT keyword
            if re.match(r'^\s*CONSTRAINT', definition, re.IGNORECASE) and 'UNIQUE' in definition.upper():
                # Extract unique key columns
                unique_key_pattern = r'CONSTRAINT\s+\w+\s+UNIQUE\s+(?:KEY\s+)?\(([^)]+)\)'
                unique_match = re.search(unique_key_pattern, definition, re.IGNORECASE)
                if unique_match:
                    key_columns = [col.strip().strip('"').strip('`') for col in unique_match.group(1).split(',')]
                    unique_keys.extend(key_columns)
                    logging.info(f"Found CONSTRAINT UNIQUE: {key_columns}")
                continue

            # Skip other constraints
            if re.match(r'^\s*CONSTRAINT|CHECK|INDEX|FOREIGN', definition, re.IGNORECASE):
                continue

            # Skip default timestamp values or other non-column entries
            if 'CAST(CURRENT_TIMESTAMP' in definition.upper() or not definition.strip():
                continue

            # Extract column name and rest of definition
            # The column name is the first word before any whitespace
            match = re.match(r'^\s*([^\s\(]+)(.*)', definition)
            if match:
                column_name = match.group(1).strip('"').strip('`')
                full_definition = match.group(2).strip()

                # Check if this column has a UNIQUE constraint
                if ' UNIQUE' in full_definition.upper():
                    unique_keys.append(column_name)
                    logging.info(f"Found column with UNIQUE constraint: {column_name}")
                    # Remove the UNIQUE keyword from the definition to avoid confusion
                    full_definition = full_definition.replace(' UNIQUE', '').replace(' unique', '')

                # Add to columns list
                columns.append((column_name, full_definition))

        # Look for unique index definitions outside the CREATE TABLE statement
        unique_index_pattern = r'CREATE\s+UNIQUE\s+INDEX\s+\w+\s+ON\s+\w+\s*\(([^)]+)\)'
        for unique_index_match in re.finditer(unique_index_pattern, ddl_content, re.IGNORECASE):
            index_columns = [col.strip().strip('"').strip('`') for col in unique_index_match.group(1).split(',')]
            unique_keys.extend(index_columns)
            logging.info(f"Found UNIQUE INDEX: {index_columns}")

        # Remove duplicates from unique keys and primary keys
        unique_keys = list(dict.fromkeys(unique_keys))
        primary_keys = list(dict.fromkeys(primary_keys))

        # Verify that all keys are valid column names
        valid_unique_keys = [key for key in unique_keys if key in all_column_names]
        valid_primary_keys = [key for key in primary_keys if key in all_column_names]

        logging.info(
            f"Parsed DDL file: Found {len(columns)} columns, {len(valid_unique_keys)} unique keys, and {len(valid_primary_keys)} primary keys")
        logging.info(f"Unique keys: {valid_unique_keys}")
        logging.info(f"Primary keys: {valid_primary_keys}")

        return columns, valid_unique_keys
    except Exception as e:
        logging.error(f"Error parsing DDL file: {str(e)}")
        raise