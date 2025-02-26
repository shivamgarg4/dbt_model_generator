import json
import os

def create_dataset_dependency_dag(config_file, output_path):
    """Create a dataset dependency DAG file"""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)

        # Extract required information
        schema_name = config['Target']['Schema']
        table_name = config['Target']['Table Name']
        materialization = config['Target'].get('materialization', 'incremental')
        
        # Get dependencies
        dep_schemas = config['DAG'].get('Dependency Schema', [])
        dep_objects = config['DAG'].get('Dependency Object', [])

        # Create the DAG file content
        dag_content = f'''
from airflow import Dataset
from common.classes.dag_utility import DAG_Helper, workspace_name, workspace_env

SCHEMA_NAME, MODEL_TYPE, MODEL_NAME = '{schema_name}', '{materialization}', '{table_name}'
DBT_JOB_NAME = SCHEMA_NAME + '_' + MODEL_NAME

DAG_SCHEDULE = [
'''

        # Add each dependency as a Dataset
        for schema, obj in zip(dep_schemas, dep_objects):
            dag_content += f"    Dataset(f'{{workspace_name.upper()}}_{{workspace_env.upper()}}_{schema}_{obj}')"
            # Add comma if not the last item
            if obj != dep_objects[-1]:
                dag_content += ","
            dag_content += "\n"

        # Complete the DAG definition
        dag_content += ''']

dag_helper = DAG_Helper()
dag = dag_helper.generate_DAG(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, schedule=DAG_SCHEDULE)

with dag:
    dbt_airflow_task = dag_helper.generate_dbt_python_task(DBT_JOB_NAME)
'''

        # Write the DAG file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            f.write(dag_content)

    except Exception as e:
        raise Exception(f"Failed to create dataset dependency DAG: {str(e)}") 