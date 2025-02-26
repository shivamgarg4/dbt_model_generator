import json
import os
import logging

def create_dataset_dependency_dag(json_path, dag_output_path):
    try:
        config = load_json_config(json_path)
        target_details = config.get("Target")
        dependency_details = config.get("DAG", {})

        SCHEMA_NAME = target_details.get("Schema") or target_details.get("Table Name").split('.')[0]
        MODEL_NAME = target_details.get("Table Name")
        MODEL_TYPE = determine_model_type(MODEL_NAME, SCHEMA_NAME)

        # Get dependency schemas and objects, with better error handling
        dependency_schemas = ensure_list(dependency_details.get("Dependency Schema", []))
        dependency_objects = ensure_list(dependency_details.get("Dependency Object", []))

        # Log what we found
        print(f"Found {len(dependency_schemas)} dependency schemas and {len(dependency_objects)} dependency objects")
        
        # If no dependencies are provided, use a default placeholder
        if not dependency_schemas or not dependency_objects or len(dependency_schemas) == 0 or len(dependency_objects) == 0:
            print("No dependencies found, using placeholder dependency")
            dependency_schemas = ["DW"]
            dependency_objects = ["PLACEHOLDER"]
        
        # Ensure the lists have the same length
        if len(dependency_schemas) != len(dependency_objects):
            print(f"Warning: Mismatched dependency counts - schemas: {len(dependency_schemas)}, objects: {len(dependency_objects)}")
            # Use the shorter length
            min_length = min(len(dependency_schemas), len(dependency_objects))
            dependency_schemas = dependency_schemas[:min_length]
            dependency_objects = dependency_objects[:min_length]
            print(f"Using only the first {min_length} dependencies")

        dag_schedules = generate_dag_schedules(dependency_schemas, dependency_objects)

        dag_code = generate_dag_code(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, dag_schedules)

        save_dag_code(dag_output_path, dag_code, SCHEMA_NAME, MODEL_NAME)

        print(f"Dataset Dependency DAG has been saved to {dag_output_path}")
    except Exception as e:
        print(f"An error occurred while generating the Dataset Dependency DAG: {e}")
        import traceback
        print(traceback.format_exc())

def load_json_config(json_path):
    with open(json_path, 'r') as json_file:
        return json.load(json_file)

def ensure_list(value):
    if value is None:
        return []
    if not isinstance(value, list):
        return [value]
    return value

def generate_dag_schedules(dependency_schemas, dependency_objects):
    return [
        f"Dataset(f'{{workspace_name.upper()}}_{{workspace_env.upper()}}_{schema}_{obj}')"
        for schema, obj in zip(dependency_schemas, dependency_objects)
    ]

def generate_dag_code(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, dag_schedules):
    # If no schedules, add a comment explaining the issue
    if not dag_schedules:
        dag_schedules_str = "# No dependencies were provided. Please add dependencies in the Config sheet."
    else:
        dag_schedules_str = ", ".join(dag_schedules)
    
    return f"""
from airflow import Dataset
from common.classes.dag_utility import DAG_Helper, workspace_name, workspace_env

SCHEMA_NAME, MODEL_TYPE, MODEL_NAME = '{SCHEMA_NAME}', '{MODEL_TYPE}', '{MODEL_NAME}'
DBT_JOB_NAME = SCHEMA_NAME + '_' + MODEL_NAME

DAG_SCHEDULE = [
    {dag_schedules_str}
]

dag_helper = DAG_Helper()
dag = dag_helper.generate_DAG(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, schedule=DAG_SCHEDULE)

with dag:
    dbt_airflow_task = dag_helper.generate_dbt_python_task(DBT_JOB_NAME)
"""

def save_dag_code(dag_output_path, dag_code, SCHEMA_NAME, MODEL_NAME):
    os.makedirs(os.path.dirname(dag_output_path), exist_ok=True)
    dag_output_path = os.path.join(os.path.dirname(dag_output_path), f"{SCHEMA_NAME}_{MODEL_NAME}.py")
    with open(dag_output_path, 'w') as dag_file:
        dag_file.write(dag_code)

def determine_model_type(model_name, default_schema):
    if model_name.startswith('F'):
        return 'FACT'
    elif model_name.startswith('D_'):
        return 'DIM'
    elif model_name.startswith('X_'):
        return 'XREF'
    elif model_name.startswith('L_'):
        return 'LKP'
    elif model_name.startswith('R'):
        return 'REL'
    else:
        return default_schema
