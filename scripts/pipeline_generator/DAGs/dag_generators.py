import json
import os
import logging
from typing import List, Tuple
#Shivam

def create_cron_dag(json_path, dag_file_path, schedule_interval, dependencies, schema_name, table_name):
    """
    Create a cron-scheduled DAG file.
    """
    try:
        MODEL_NAME = table_name
        SCHEMA_NAME = schema_name
        MODEL_TYPE = determine_model_type(MODEL_NAME, SCHEMA_NAME)
        DBT_JOB_NAME = f"{SCHEMA_NAME}_{MODEL_NAME}"

        # Use the schedule_interval as cron values
        cron_values = schedule_interval or "0 */4 * * *"

        print(f"Using cron schedule: {cron_values}")

        dag_code = generate_cron_dag_code(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, cron_values)

        save_dag_code(dag_file_path, dag_code, SCHEMA_NAME, MODEL_NAME)

        print(f"CRON DAG has been saved to {dag_file_path}")
        return dag_file_path

    except Exception as e:
        print(f"An error occurred while generating the CRON DAG: {e}")
        import traceback
        print(traceback.format_exc())
        raise


def create_sns_dag(json_path, dag_file_path, schedule_interval, dependencies, schema_name, table_name):
    """
    Create an SNS-triggered DAG file.
    """
    try:
        SCHEMA_NAME = schema_name
        MODEL_NAME = table_name
        MODEL_TYPE = determine_model_type(MODEL_NAME, SCHEMA_NAME)
        DBT_JOB_NAME = f"{SCHEMA_NAME}_{MODEL_NAME}"

        # For SNS DAGs, DOMAIN_NAME and DP_NAME follow the same pattern
        DOMAIN_NAME = SCHEMA_NAME
        DP_NAME = MODEL_NAME

        print(f"Generating SNS DAG with DOMAIN_NAME={DOMAIN_NAME}, MODEL_TYPE={MODEL_TYPE}, DP_NAME={DP_NAME}")

        dag_code = generate_sns_dag_code(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, DOMAIN_NAME, DP_NAME)

        save_dag_code(dag_file_path, dag_code, SCHEMA_NAME, MODEL_NAME)

        print(f"SNS DAG has been saved to {dag_file_path}")
        return dag_file_path

    except Exception as e:
        print(f"An error occurred while generating the SNS DAG: {e}")
        import traceback
        print(traceback.format_exc())
        raise


def create_dataset_dag(json_path, dag_file_path, schedule_interval, dependencies, schema_name, table_name):
    """
    Create a dataset-dependent DAG file.
    """
    try:
        MODEL_NAME = table_name
        SCHEMA_NAME = schema_name
        MODEL_TYPE = determine_model_type(MODEL_NAME, SCHEMA_NAME)
        DBT_JOB_NAME = f"{SCHEMA_NAME}_{MODEL_NAME}"

        # Process dependencies to extract schemas and objects
        dependency_schemas, dependency_objects = parse_dependencies(dependencies)

        print(f"Found {len(dependency_schemas)} dependency schemas and {len(dependency_objects)} dependency objects")

        # Use default placeholder if no valid dependencies
        if not dependency_schemas or not dependency_objects:
            print("No dependencies found, using placeholder dependency")
            dependency_schemas = ["DW"]
            dependency_objects = ["PLACEHOLDER"]

        # Generate DAG schedules
        dag_schedules = generate_dag_schedules(dependency_schemas, dependency_objects)

        print(f"Generating Dataset DAG for {MODEL_NAME} with dependencies: {dependencies}")

        dag_code = generate_dataset_dag_code(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, DBT_JOB_NAME, dag_schedules)

        save_dag_code(dag_file_path, dag_code, SCHEMA_NAME, MODEL_NAME)

        print(f"Dataset DAG has been saved to {dag_file_path}")
        return dag_file_path

    except Exception as e:
        print(f"An error occurred while generating the Dataset DAG: {e}")
        import traceback
        print(traceback.format_exc())
        raise


def generate_cron_dag_code(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, cron_values):
    """Generate the DAG code for cron-scheduled DAGs."""
    return f"""
from airflow import Dataset
from common.classes.dag_utility import DAG_Helper, workspace_name, workspace_env

SCHEMA_NAME, MODEL_TYPE, MODEL_NAME = '{SCHEMA_NAME}', '{MODEL_TYPE}', '{MODEL_NAME}'
DBT_JOB_NAME = SCHEMA_NAME + '_' + MODEL_NAME

dag_helper = DAG_Helper()
dag = dag_helper.generate_DAG(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, schedule='{cron_values}')

with dag:
    dbt_airflow_task = dag_helper.generate_dbt_python_task(DBT_JOB_NAME)
"""


def generate_sns_dag_code(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, DOMAIN_NAME, DP_NAME):
    """Generate the DAG code for SNS-triggered DAGs."""
    return f"""
from common.classes.dag_utility import DAG_Helper

DOMAIN_NAME, MODEL_TYPE, DP_NAME = '{DOMAIN_NAME}', 'SNS_DPND', '{MODEL_NAME}'
DBT_JOB_NAME = DOMAIN_NAME + '_' + DP_NAME

dag_helper = DAG_Helper()
dag = dag_helper.generate_DAG(MODEL_TYPE + '_' + DOMAIN_NAME, MODEL_TYPE, DP_NAME, schedule=None)

with dag:
    dbt_airflow_task = dag_helper.generate_dbt_python_task(DBT_JOB_NAME)
"""


def generate_dataset_dag_code(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, DBT_JOB_NAME, dag_schedules):
    """Generate the DAG code for dataset-dependent DAGs."""
    schedules_str = f"[{', '.join(dag_schedules)}]" if dag_schedules else "[]"

    return f"""
from airflow import Dataset
from common.classes.dag_utility import DAG_Helper, workspace_name, workspace_env

SCHEMA_NAME, MODEL_TYPE, MODEL_NAME = '{SCHEMA_NAME}', '{MODEL_TYPE}', '{MODEL_NAME}'
DBT_JOB_NAME = SCHEMA_NAME + '_' + MODEL_NAME

# Define dataset dependencies
datasets = {schedules_str}

dag_helper = DAG_Helper()
dag = dag_helper.generate_DAG(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, schedule=datasets)

with dag:
    dbt_airflow_task = dag_helper.generate_dbt_python_task(DBT_JOB_NAME)
"""


def format_dataset_dependencies(dependencies):
    """Format dependencies for dataset DAG."""
    if not dependencies:
        return "[]"

    formatted_deps = [f"'{dep}'" for dep in dependencies]
    return f"[{', '.join(formatted_deps)}]"


def save_dag_code(dag_output_path, dag_code, SCHEMA_NAME, MODEL_NAME):
    os.makedirs(os.path.dirname(dag_output_path), exist_ok=True)
    dag_output_path = os.path.join(os.path.dirname(dag_output_path), f"{SCHEMA_NAME}_{MODEL_NAME}.py")
    with open(dag_output_path, 'w') as dag_file:
        dag_file.write(dag_code)


def determine_model_type(model_name, default_schema):
    """Determine the model type based on the model name prefix."""
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


def parse_dependencies(dependencies) -> Tuple[List[str], List[str]]:
    """
    Parse dependencies to extract schema and object names.
    Handles both dictionary format and direct dependency list.
    Expected formats:
    - [{'Schema': 'DW', 'Table': 'D_CUSTOMER'}, {'Schema': 'DW', 'Table': 'D_CUSTOMER_2'}]
    - Direct list format
    Returns: (schemas, objects)
    """
    if not dependencies:
        return [], []

    # Ensure we have a list to work with
    deps_list = ensure_list(dependencies)
    schemas = []
    objects = []

    try:
        # Handle dictionary format
        if deps_list and isinstance(deps_list[0], dict):
            for dep in deps_list:
                if isinstance(dep, dict) and 'Schema' in dep and 'Table' in dep:
                    schemas.append(dep['Schema'])
                    objects.append(dep['Table'])
                else:
                    print(f"Warning: Invalid dependency dictionary format: {dep}")
        # Handle direct list format
        else:
            for dep in deps_list:
                if isinstance(dep, str):
                    if '.' in dep:
                        schema, obj = dep.split('.')
                        schemas.append(schema)
                        objects.append(obj)
                    else:
                        # Handle case where dependency is just a table name
                        schemas.append('DW')  # Default schema
                        objects.append(dep)
                else:
                    print(f"Warning: Invalid dependency format: {dep}")

    except Exception as e:
        print(f"Error parsing dependencies: {e}")
        return ['DW'], ['PLACEHOLDER']  # Fallback to default values

    # If no valid dependencies were found, use default values
    if not schemas or not objects:
        return ['DW'], ['PLACEHOLDER']

    return schemas, objects


def ensure_list(value) -> List:
    """Convert input to list if it isn't already."""
    if value is None:
        return []
    if not isinstance(value, list):
        return [value]
    return value


def generate_dag_schedules(dependency_schemas: List[str], dependency_objects: List[str]) -> List[str]:
    """Generate DAG schedule strings for dataset dependencies."""
    return [
        f"Dataset(f'{{workspace_name.upper()}}_{{workspace_env.upper()}}_{schema}_{obj}')"
        for schema, obj in zip(dependency_schemas, dependency_objects)
    ]
