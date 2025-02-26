
from airflow import Dataset
from common.classes.dag_utility import DAG_Helper, workspace_name, workspace_env

SCHEMA_NAME, MODEL_TYPE, MODEL_NAME = 'DW', 'DIM', 'D_OPCO'
DBT_JOB_NAME = SCHEMA_NAME + '_' + MODEL_NAME

DAG_SCHEDULE = [
    Dataset(f'{workspace_name.upper()}_{workspace_env.upper()}_DW_D_CUSTOMER')
]

dag_helper = DAG_Helper()
dag = dag_helper.generate_DAG(SCHEMA_NAME, MODEL_TYPE, MODEL_NAME, schedule=DAG_SCHEDULE)

with dag:
    dbt_airflow_task = dag_helper.generate_dbt_python_task(DBT_JOB_NAME)
