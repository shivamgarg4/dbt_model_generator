"""
DBT Pipeline Generator package.
Handles pipeline configuration, model generation, source mapping, and DAG creation.
"""

from .init_pipeline_generator import (
    read_pipeline_sheet,
    write_pipeline_details,
    create_mapping_sheet,
    create_config_sheet,
    process_pipeline_sheet,
    process_filled_workbook_to_json,
    process_filled_sheet,
    generate_dbt_models,
    generate_dag_file,
    generate_dbt_jobs
)

from .model_generator_pipelines import (
    generate_lnd_dbt_model_file,
    create_dp_view_file,
    format_columns,
    replace_audit_columns,
    read_snowflake_config
)

from .source_column_mapper import (
    SourceColumnMapper,
    update_source_mappings
)

from .dag_generators import (
    create_cron_dag,
    create_sns_dag,
    create_dataset_dag,
    generate_cron_dag_code,
    generate_sns_dag_code,
    generate_dataset_dag_code,
    format_dataset_dependencies,
    save_dag_code,
    determine_model_type,
    parse_dependencies,
    ensure_list,
    generate_dag_schedules
)

__all__ = [
    # Pipeline sheet operations
    'read_pipeline_sheet',
    'write_pipeline_details',
    'create_mapping_sheet',
    'create_config_sheet',
    'process_pipeline_sheet',
    'process_filled_workbook_to_json',
    'process_filled_sheet',

    # Model generation
    'generate_dbt_models',
    'generate_dag_file',
    'generate_dbt_jobs',
    'generate_lnd_dbt_model_file',
    'create_dp_view_file',

    # Column utilities
    'format_columns',
    'replace_audit_columns',

    # Configuration and mapping
    'read_snowflake_config',
    'SourceColumnMapper',
    'update_source_mappings',

    # DAG creation and utilities
    'create_cron_dag',
    'create_sns_dag',
    'create_dataset_dag',
    'generate_cron_dag_code',
    'generate_sns_dag_code',
    'generate_dataset_dag_code',
    'format_dataset_dependencies',
    'save_dag_code',
    'determine_model_type',
    'parse_dependencies',
    'ensure_list',
    'generate_dag_schedules'
]

# Version information
__version__ = '1.0.0'
