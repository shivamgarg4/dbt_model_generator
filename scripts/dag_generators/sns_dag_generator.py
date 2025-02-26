import json
import os
import logging


def create_sns_dag(json_path, dag_output_path):
    try:
        config = load_json_config(json_path)
        target_details, source_details = config.get("Target"), config.get("Source")
        dag_details = config.get("DAG", {})

        SCHEMA_NAME = target_details.get("Schema") or target_details.get("Table Name").split('.')[0]
        MODEL_NAME = target_details.get("Table Name")
        MODEL_TYPE = determine_model_type(MODEL_NAME, SCHEMA_NAME)
        DBT_JOB_NAME = f"{SCHEMA_NAME}_{MODEL_NAME}"

        # Get domain name and DP view from source details or DAG details
        DOMAIN_NAME = source_details.get("Schema") or SCHEMA_NAME
        DP_NAME = source_details.get("DP View") or MODEL_NAME
        
        # Log what we're using
        print(f"Generating SNS DAG with DOMAIN_NAME={DOMAIN_NAME}, MODEL_TYPE={MODEL_TYPE}, DP_NAME={DP_NAME}")

        dag_code = generate_dag_code(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, DBT_JOB_NAME, DOMAIN_NAME, DP_NAME)

        save_dag_code(dag_output_path, dag_code, SCHEMA_NAME, MODEL_NAME)

        print(f"SNS DAG has been saved to {dag_output_path}")
    except Exception as e:
        print(f"An error occurred while generating the SNS DAG: {e}")
        import traceback
        print(traceback.format_exc())


def load_json_config(json_path):
    """Load the JSON configuration from the given path."""
    with open(json_path, 'r') as json_file:
        return json.load(json_file)


def generate_dag_code(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, DBT_JOB_NAME, DOMAIN_NAME, DP_NAME):
    """Generate the DAG code based on the provided parameters."""
    return f"""
from common.classes.dag_utility import DAG_Helper
 
DOMAIN_NAME, MODEL_TYPE, DP_NAME = '{DOMAIN_NAME}', 'SNS_DPND', '{MODEL_NAME}'
DBT_JOB_NAME = DOMAIN_NAME + '_' + DP_NAME
 
dag_helper = DAG_Helper()
dag = dag_helper.generate_DAG(MODEL_TYPE + '_' + DOMAIN_NAME, MODEL_TYPE, DP_NAME, schedule=None)
 
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
