import json
import os
import shutil
import logging
from datetime import datetime

# File type mapping with source and client directories
FILE_TYPES = {
    'dags': {
        'source_subdir': os.path.join(os.getcwd(), 'dags/'),
        'client_subdir': ''
    },
    'dbt': {
        'source_subdir': os.path.join(os.getcwd(), 'jobs/'),
        'client_subdir': ''
    },
    'ddl': {
        'source_subdir': os.path.join(os.getcwd(), 'snowflake/ddl'),
        'client_subdir': ''
    },
    'dml': {
        'source_subdir': os.path.join(os.getcwd(), 'snowflake/dml'),
        'client_subdir': ''
    }
}

def collect_files(directory, file_names, base_dir, prefix):
    file_paths = []
    # Handle empty or None file_names
    if not file_names:
        return file_paths

    # Convert file_names to a set for better lookup performance
    file_names_set = {os.path.basename(name.strip()) for name in file_names if name.strip()}

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file in file_names_set:
                relative_path = os.path.relpath(os.path.join(root, file), base_dir).replace("\\", "/")
                file_paths.append(f"{prefix}/{relative_path}")
    return file_paths

def generate_deployment_file(dag_files=None, dbt_jobs=None, ddl_files=None, dml_files=None):
    deployment_data = {
        "deployment_verbose_mode": True,
        "domain_release_components": {
            "dags": {
                "dags_source_files": [{"source_files_path": file, "destination_sub_folder": "", "class_or_util": ""} for
                                      file in (dag_files or [])]
            },
            "data_mesh_secrets_configuration": {},
            "dbt_jobs": {
                "dbt_account_id": 31949,
                "jobs": dbt_jobs if dbt_jobs else []
            },
            "database_scripts": {
                "ddl_folders": [{"folder_path": file} for file in (ddl_files or [])],
                "dml_folders": [{"folder_path": file} for file in (dml_files or [])]
            },
            "domain_secrets_configuration": {},
            "domain_files_copy_to_s3": [],
            "domain_cf_configuration": {},
            "lambda_functions_to_update": []
        }
    }

    return json.dumps(deployment_data, indent=4)

def process_file_names(file_names):
    """Process file names into a list, handling various input formats"""
    if not file_names:
        return []

    # If it's already a list
    if isinstance(file_names, list):
        names = file_names
    # If it's a string, split by either comma or semicolon
    elif isinstance(file_names, str):
        # Split by semicolon first, then by comma if no semicolons found
        if ';' in file_names:
            names = [name.strip() for name in file_names.split(';') if name.strip()]
        else:
            names = [name.strip() for name in file_names.split(',') if name.strip()]
    else:
        return []

    # Process each name to get the basename
    return [os.path.basename(name.strip()) for name in names if name.strip()]

def source_and_move_files(base_client_dir, file_names, file_type):
    """
    Source files from current project and move them to client directory

    :param base_client_dir: Base directory in client project
    :param file_names: List of file names to source
    :param file_type: Type of files (dags, dbt, ddl, dml)
    :return: List of successfully moved file paths
    """


    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s: %(message)s',
        filename='file_movement.log'
    )

    if file_type not in FILE_TYPES:
        logging.error(f"Unsupported file type: {file_type}")
        raise ValueError(f"Unsupported file type: {file_type}")

    source_dir = FILE_TYPES[file_type]['source_subdir']
    client_subdir = FILE_TYPES[file_type]['client_subdir']

    logging.info(f"Moving {file_type} files from {source_dir} to {base_client_dir}/{client_subdir}")

    # Construct full client directory path
    full_client_dir = os.path.join(base_client_dir, client_subdir)

    # Create client directory if it doesn't exist
    os.makedirs(full_client_dir, exist_ok=True)

    # Track moved files
    moved_files = []

    # Source and move files
    for file_name in file_names:
        source_path = os.path.join(source_dir, file_name)
        dest_path = os.path.join(full_client_dir, file_name)

        try:
            if os.path.exists(source_path):
                shutil.copy2(source_path, dest_path)
                logging.info(f"Moved {file_name} to {dest_path}")
                moved_files.append(dest_path)
            else:
                logging.warning(f"Warning: {source_path} does not exist")
        except Exception as e:
            logging.error(f"Error moving {file_name}: {e}")

    return moved_files

def extract_last_directory(file_dict):
    result = {}

    # Process each category
    for category in ['dags', 'dbt', 'ddl', 'dml']:
        if file_dict[category]:  # If the category has any files
            # Take the first file path from the category
            file_path = file_dict[category][0]
            # Handle both forward and backward slashes
            normalized_path = file_path.replace('\\', '/')
            # Split the path and get the second to last component (before filename)
            directories = normalized_path.split('/')
            # Store the last directory before the filename for this category
            result[category] = directories[-2]
        else:
            # If category is empty, store None or you could use an empty string ''
            result[category] = None

    return result

def build_target_paths(prefixes):
    target_paths = {}

    # Handle dags path
    dags_prefix = prefixes.get('dags', '')
    target_paths['dags'] = f"airflow/dags/{dags_prefix}" if dags_prefix else "airflow/dags"

    # Handle dbt path
    dbt_prefix = prefixes.get('dbt', '')
    target_paths['dbt'] = f"dbt/jobs/{dbt_prefix}" if dbt_prefix else "dbt/jobs"

    # Handle ddl path
    ddl_prefix = prefixes.get('ddl', '')
    target_paths['ddl'] = f"snowflake/rgm/ddl/{ddl_prefix}" if ddl_prefix else "snowflake/rgm/ddl"

    # Handle dml path
    dml_prefix = prefixes.get('dml', '')
    target_paths['dml'] = f"snowflake/rgm/dml/{dml_prefix}" if dml_prefix else "snowflake/rgm/dml"

    return target_paths

def json_creator(base_dir_dags, base_dir_dbt, base_dir_sql,
                 dag_file_names, dbt_file_names, ddl_file_names, dml_file_names):
    """
    Create deployment JSON after moving generated files to client project directories.

    Args:
        base_dir_dags (str): Base directory for DAG files
        base_dir_dbt (str): Base directory for DBT files
        base_dir_sql (str): Base directory for SQL files
        dag_file_names (list): List of DAG file names
        dbt_file_names (list): List of DBT job file names
        ddl_file_names (list): List of DDL file names
        dml_file_names (list): List of DML file names

    Returns:
        dict: Dictionary containing moved files and deployment file path
    """


    """Create JSON deployment configuration"""
    print("Collecting files...")
    print(f"Processing directories:")
    print(f"DAGs directory: {base_dir_dags}")
    print(f"DBT directory: {base_dir_dbt}")
    print(f"SQL directory: {base_dir_sql}")

    # Process all file names
    dag_file_names = process_file_names(dag_file_names)
    dbt_file_names = process_file_names(dbt_file_names)
    ddl_file_names = process_file_names(ddl_file_names)
    dml_file_names = process_file_names(dml_file_names)

    print(f"\nProcessing files:")
    print(f"DAG files: {', '.join(dag_file_names)}")
    print(f"DBT files: {', '.join(dbt_file_names)}")
    print(f"DDL files: {', '.join(ddl_file_names)}")
    print(f"DML files: {', '.join(dml_file_names)}")

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s: %(message)s',
        filename='json_creator.log'
    )

    try:
        # Move files for different types
        moved_files = {
            'dags': source_and_move_files(base_dir_dags, dag_file_names, 'dags'),
            'dbt': source_and_move_files(base_dir_dbt, dbt_file_names, 'dbt'),
            'ddl': source_and_move_files(base_dir_sql, ddl_file_names, 'ddl'),
            'dml': source_and_move_files(base_dir_sql, dml_file_names, 'dml')
        }

        print(f"\nMoved files: {moved_files}")

        prefixes=extract_last_directory(moved_files)
        target_paths = build_target_paths(prefixes)

        # Now collect the files from their new locations
        dags_files = collect_files(base_dir_dags, dag_file_names, base_dir_dags, target_paths['dags'])

        logging.info(f"Collected DAG files: {dags_files}")

        # Process DBT jobs
        dbt_jobs = []
        if dbt_file_names:
            dbt_files = collect_files(base_dir_dbt, dbt_file_names, base_dir_dbt, target_paths['dbt'])
            logging.info(f"Collected DBT files: {dbt_files}")
            dbt_jobs = [
                {
                    "dbt_environment_name": "RGM_DBT_ENV",
                    "dbt_job_files_directory": dbt_file
                }
                for dbt_file in dbt_files
            ]

        # Collect SQL files
        ddl_files = collect_files(base_dir_sql, ddl_file_names, base_dir_sql, target_paths['ddl'])
        dml_files = collect_files(base_dir_sql, dml_file_names, base_dir_sql, target_paths['dml'])

        logging.info(f"Collected DDL files: {ddl_files}")
        logging.info(f"Collected DML files: {dml_files}")

        # Generate deployment JSON
        deployment_json = generate_deployment_file(
            dag_files=dags_files,
            dbt_jobs=dbt_jobs,
            ddl_files=ddl_files,
            dml_files=dml_files
        )

        # Write deployment file to client project directory
        deployment_file_path = os.path.join(base_dir_dags, 'deployment_file.json')
        with open(deployment_file_path, 'w') as file:
            file.write(deployment_json)

        logging.info(f"Deployment file generated: {deployment_file_path}")
        print(f"\nDeployment file generated: {deployment_file_path}")
        print(f"Found {len(dags_files)} DAG files, {len(dbt_jobs)} DBT jobs, "
              f"{len(ddl_files)} DDL files, and {len(dml_files)} DML files.")

        return {
            'moved_files': moved_files,
            'collected_files': {
                'dags': dags_files,
                'dbt': dbt_files if dbt_file_names else [],
                'ddl': ddl_files,
                'dml': dml_files
            },
            'deployment_file': deployment_file_path
        }

    except Exception as e:
        logging.error(f"Error in json_creator: {e}")
        raise
