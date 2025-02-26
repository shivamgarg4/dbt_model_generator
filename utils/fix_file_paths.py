import os
import json
import re
from datetime import datetime

def find_similar_file(target_path):
    """Find a file with a similar name pattern but different timestamp"""
    # Extract directory and filename pattern
    directory = os.path.dirname(target_path)
    filename = os.path.basename(target_path)
    
    # Extract base name and timestamp
    match = re.match(r'(.+)_(\d{8}_\d{6})\.(.+)', filename)
    if not match:
        return None
    
    base_name, timestamp, extension = match.groups()
    pattern_to_match = f"{base_name}_*.{extension}"
    
    # Check if directory exists
    if not os.path.exists(directory):
        print(f"Directory does not exist: {directory}")
        return None
    
    # List files in directory
    try:
        files = os.listdir(directory)
        for file in files:
            if file.startswith(base_name) and file.endswith(f".{extension}"):
                return os.path.join(directory, file)
    except Exception as e:
        print(f"Error listing directory: {str(e)}")
    
    return None

def check_file_exists(file_path):
    """Check if a file exists and suggest alternatives if not"""
    if os.path.exists(file_path):
        print(f"✓ File exists: {file_path}")
        return file_path
    
    print(f"✗ File does not exist: {file_path}")
    
    # Try to find a similar file
    similar_file = find_similar_file(file_path)
    if similar_file:
        print(f"  Found similar file: {similar_file}")
        return similar_file
    
    print("  No similar file found")
    return None

def fix_file_history():
    """Check and fix file paths in file_history.json"""
    history_file = 'file_history.json'
    
    if not os.path.exists(history_file):
        print(f"History file not found: {history_file}")
        return
    
    try:
        with open(history_file, 'r') as f:
            history = json.load(f)
        
        # Check mapping files
        updated_mapping_files = []
        for file_path in history.get('mapping_files', []):
            existing_path = check_file_exists(file_path)
            if existing_path:
                updated_mapping_files.append(existing_path)
        
        # Check DDL files
        updated_ddl_files = []
        for file_path in history.get('ddl_files', []):
            existing_path = check_file_exists(file_path)
            if existing_path:
                updated_ddl_files.append(existing_path)
        
        # Update history
        history['mapping_files'] = updated_mapping_files
        history['ddl_files'] = updated_ddl_files
        
        # Save updated history
        with open(history_file, 'w') as f:
            json.dump(history, f, indent=4)
        
        print(f"\nUpdated {history_file} with valid file paths")
    
    except Exception as e:
        print(f"Error fixing file history: {str(e)}")

def check_system_date():
    """Check if system date appears to be incorrect"""
    current_year = datetime.now().year
    if current_year != 2023:
        print(f"\n⚠️ WARNING: System year appears to be incorrect: {current_year}")
        print("   This may cause issues with file timestamps.")
        print("   Consider updating your system date to the correct year (2023).")

if __name__ == "__main__":
    print("=== File Path Checker ===")
    print(f"Current directory: {os.getcwd()}")
    
    # Check system date
    check_system_date()
    
    print("\nChecking file_history.json...")
    fix_file_history()
    
    print("\nDone!") 