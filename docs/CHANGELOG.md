# Changelog

All notable changes to the DBT Model Generator project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure setup
- Basic directory organization:
  - `config/` for configuration files
  - `data/` for sample data files
  - `dags/` for generated Airflow DAGs
  - `jobs/` for generated DBT job files
  - `mappings/` for mapping Excel files
  - `models/` for generated DBT models
  - `scripts/` for core functionality
- Core script files:
  - `dag_generators.py` for DAG generation
  - `dbt_job_generator.py` for DBT job generation
  - `dbt_model_generator.py` for DBT model generation
  - `excel_to_json.py` for Excel conversion
  - `model_mapper.py` for mapping functionality
  - `utils.py` for utility functions
- Main application file (`dag_generator_app.py`)
- Support for multiple DAG types:
  - CRON-based scheduling
  - Dataset dependency triggers
  - SNS event-driven execution
- Basic documentation in README.md

### Planned
- SQL DDL parser implementation
- Excel mapping template generation
- Snowflake integration for auto-filling column details
- DBT model generation core functionality
- Job configuration generation
- Airflow DAG generation for different types
- Enhanced error handling and validation
- User interface improvements
- Comprehensive testing suite
- Extended documentation

## [0.1.0] - 2025-02-27
### Initial Release
- Project scaffolding
- Basic directory structure
- Core script templates
- Initial documentation
