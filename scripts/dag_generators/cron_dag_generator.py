import json
import os

def create_cron_dag(json_path, dag_output_path):
    try:
        config = load_json_config(json_path)
        target_details, dag_details = config.get("Target"), config.get("DAG")

        MODEL_NAME = target_details.get("Table Name")
        SCHEMA_NAME = target_details.get("Schema") or MODEL_NAME.split('.')[0]
        MODEL_TYPE = determine_model_type(MODEL_NAME, SCHEMA_NAME)
        DBT_JOB_NAME = f"{SCHEMA_NAME}_{MODEL_NAME}"
        
        # Look for Schedule first, then fall back to Cron Values if Schedule is not present
        cron_values = dag_details.get("Schedule") or dag_details.get("Cron Values") or "0 */4 * * *"
        
        # Log the cron schedule being used
        print(f"Using cron schedule: {cron_values}")

        dag_code = generate_dag_code(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, DBT_JOB_NAME, cron_values)

        save_dag_code(dag_output_path, dag_code, SCHEMA_NAME, MODEL_NAME)

        print(f"CRON DAG has been saved to {dag_output_path}")
    except Exception as e:
        print(f"An error occurred while generating the CRON DAG: {e}")


def load_json_config(json_path):
    """Load the JSON configuration from the given path."""
    with open(json_path, 'r') as json_file:
        return json.load(json_file)


def generate_dag_code(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, DBT_JOB_NAME, cron_values):
    """Generate the DAG code based on the provided parameters."""
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


def save_dag_code(dag_output_path, dag_code, SCHEMA_NAME, MODEL_NAME):
    """Ensure the directory exists and save the DAG code to a Python file."""
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
