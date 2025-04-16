
import os
import json
import logging

def ensure_directory_exists(directory_path):
    """Ensure that a directory exists, creating it if necessary"""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logging.info(f"Created directory: {directory_path}")
    return directory_path

def load_json_file(file_path):
    """Load and parse a JSON file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading JSON file {file_path}: {str(e)}")
        return None

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
