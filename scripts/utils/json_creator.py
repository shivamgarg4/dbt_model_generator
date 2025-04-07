import os
import json


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



def json_creator(base_dir_dags, base_dir_dbt, base_dir_sql, dag_file_names, dbt_file_names, ddl_file_names,
                 dml_file_names):
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

    # Collect all files
    dag_files = collect_files(base_dir_dags, dag_file_names, base_dir_dags, "airflow/dags")

    # Process DBT jobs
    dbt_jobs = []
    if dbt_file_names:
        # Collect all DBT files at once
        dbt_files = collect_files(base_dir_dbt, dbt_file_names, base_dir_dbt, "dbt/jobs")
        # Create job entries for each found file
        dbt_jobs = [
            {
                "dbt_environment_name": "RGM_DBT_ENV",
                "dbt_job_files_directory": dbt_file
            }
            for dbt_file in dbt_files
        ]

    # Collect SQL files
    ddl_files = collect_files(base_dir_sql, ddl_file_names, base_dir_sql, "snowflake/rgm")
    dml_files = collect_files(base_dir_sql, dml_file_names, base_dir_sql, "snowflake/rgm")

    # Generate and save deployment file
    deployment_json = generate_deployment_file(
        dag_files=dag_files,
        dbt_jobs=dbt_jobs,
        ddl_files=ddl_files,
        dml_files=dml_files
    )

    with open('deployment_file.json', 'w') as file:
        file.write(deployment_json)

    print("\nDeployment file generated successfully.")
    print(f"Found {len(dag_files)} DAG files, {len(dbt_jobs)} DBT jobs, "
          f"{len(ddl_files)} DDL files, and {len(dml_files)} DML files.")
