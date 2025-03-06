import os
import yaml
import json

def create_dbt_job_file(config_file, output_dir='jobs', merge_dbt_job_additon_flg=False, merge_macro_file_path=None, insert_dbt_job_additon_flg=False, insert_macro_file_path=None):
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
        job_content = f'''dbt build --select {schema_name}.{table_name}\n'''

        # Add merge macro file path if merge_dbt_job_additon_flg is True
        if merge_dbt_job_additon_flg:
            job_content += f"dbt run-operation {os.path.splitext(os.path.basename(merge_macro_file_path))[0]}\n"

        # Add insert macro file path if insert_dbt_job_additon_flg is True
        if insert_dbt_job_additon_flg:
            job_content += f"dbt run-operation {os.path.splitext(os.path.basename(insert_macro_file_path))[0]}\n"

        # Create jobs directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Create the job file with SCHEMA_MODEL.dbt format
        job_file_path = os.path.join(output_dir, f"{job_name}.dbt")

        with open(job_file_path, 'w') as f:
            f.write(job_content)

        return job_file_path

    except Exception as e:
        raise Exception(f"Failed to create dbt job file: {str(e)}")
