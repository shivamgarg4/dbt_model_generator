"""
Scripts package for DBT Model Generator.
Contains utility scripts for generating models, jobs, and DAGs.
"""

from .excel_to_json import _extract_config_from_sheet, _extract_mappings_from_sheet
from .dbt_job_generator import create_dbt_job_file
from .dbt_model_generator import (
    create_dbt_model_from_json,
    extract_join_clauses,
    extract_where_condition,
    extract_group_by,
    get_materialization,
    should_exclude_from_minus,
    format_sql_content
)
from .insert_sql_generator import insert_sql_generator
from .merge_sql_generator import (
    generate_merge_sql_for_pipelines,
    merge_sql_generator
)
from .model_mapper import ModelMapper
from .dag_generators import (
    create_cron_dag,
    create_sns_dag,
    create_dataset_dependency_dag
)
from .utils import (
    get_snowflake_connection,
    get_cursor,
    get_config_from_sheet,
    get_table_info_from_sheet,
    extract_table_name,
    parse_ddl_file
)

__all__ = [
    # Airflow DAG generators
    'create_cron_dag',
    'create_sns_dag',
    'create_dataset_dependency_dag',

    # Excel to JSON converters
    '_extract_mappings_from_sheet',
    '_extract_config_from_sheet',

    # DBT related functions
    'extract_join_clauses',
    'extract_where_condition',
    'extract_group_by',
    'get_materialization',
    'should_exclude_from_minus',

    # SQL generators
    'insert_sql_generator',
    'merge_sql_generator',
    'generate_merge_sql_for_pipelines',
    'format_sql_content',
    'create_dbt_model_from_json',

    # Job file generator
    'create_dbt_job_file',

    # Model mapper
    'ModelMapper',

    # Utilities
    'get_snowflake_connection',
    'get_cursor',
    'get_config_from_sheet',
    'get_table_info_from_sheet',
    'extract_table_name',
    'parse_ddl_file'
]

# Version information
__version__ = '1.0.0'
