import os
import yaml
import json

def create_dbt_job_file(config_file, output_dir='jobs'):
    """Create a dbt job file from the configuration"""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)

        # Get target schema and table name
        schema_name = config['Target']['Schema']
        table_name = config['Target']['Table Name']

        # Create job name using schema and table name
        job_name = f"{schema_name}_{table_name}"

        # Create the job file content with schema.model format in build statement
        job_content = f'''dbt build --select {schema_name}.{table_name}'''

        # Create jobs directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Create the job file with SCHEMA_MODEL.dbt format
        job_file_path = os.path.join(output_dir, f"{job_name}.dbt")

        with open(job_file_path, 'w') as f:
            f.write(job_content)

        return job_file_path

    except Exception as e:
        raise Exception(f"Failed to create dbt job file: {str(e)}")
