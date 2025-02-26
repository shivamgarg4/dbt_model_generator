from scripts.excel_to_json import convert_excel_to_json
from scripts.dbt_model_generator import create_dbt_model_from_json
from scripts.dbt_job_generator import create_dbt_job_file
from scripts.dag_generators import (
    create_dataset_dependency_dag,
    create_cron_dag,
    create_sns_dag
)
import datetime
import json
import os


def main():
    try:
        # Create necessary directories if they don't exist
        for dir_path in ['config', 'dags', 'jobs']:
            os.makedirs(dir_path, exist_ok=True)

        excel_file_path = '../data/DBT_Build_Automation_LND.xlsx'
        json_output_path = generate_json_output_path("DBT_Build_Automation")

        # Convert Excel to JSON
        convert_excel_to_json(excel_file_path, json_output_path)
        
        # Create DBT model
        target_model_name = create_dbt_model_from_json(json_output_path)
        
        # Create DBT job file
        dbt_job_file_path = f'jobs/{target_model_name}.dbt'
        create_dbt_job_file(dbt_job_file_path, target_model_name)

        # Generate appropriate DAG based on configuration
        config = load_json_config(json_output_path)
        dag_output_path = f'dags/{target_model_name}.py'
        generate_dag(config, json_output_path, dag_output_path)

    except Exception as e:
        print(f"An error occurred: {e}")


def generate_json_output_path(project_name):
    """Generate a descriptive JSON file name based on the project name and current date/time."""
    date_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f'config/{project_name}_config_{date_str}.json'


def load_json_config(json_output_path):
    """Load the JSON configuration from the given path."""
    with open(json_output_path, 'r') as json_file:
        return json.load(json_file)


def generate_dag(config, json_output_path, dag_output_path):
    """Generate the Airflow DAG based on the JSON configuration."""
    dag_details = config.get("DAG", {})
    dag_type = dag_details.get("DAG Type", "").upper()

    dag_generators = {
        "DATASET DEPENDENCY": create_dataset_dependency_dag,
        "CRON": create_cron_dag,
        "SNS": create_sns_dag
    }

    generator = dag_generators.get(dag_type)
    if not generator:
        raise ValueError(f"Unsupported DAG Type: {dag_type}")
    
    generator(json_output_path, dag_output_path)


if __name__ == "__main__":
    main()