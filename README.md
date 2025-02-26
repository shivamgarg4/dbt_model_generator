# DBT Model Generator

This application automates the generation of DBT models, jobs, and Airflow DAGs from SQL DDL files and mapping templates. It provides a user-friendly GUI to streamline the process of creating and managing data models.

## Features

- **Generate Mapping Templates**: Create Excel mapping templates from SQL DDL files
- **Fill Model Mappings**: Connect to Snowflake to automatically fill in column details
- **Generate DBT Models**: Create DBT models from mapping files
- **Generate Airflow DAGs**: Create Airflow DAGs for scheduling DBT jobs
- **Support for Multiple DAG Types**: Support for CRON, Dataset Dependency, and SNS DAG types

## Project Structure

```
dbt_model_generator/
├── config/                  # Configuration files
├── data/                    # Sample data files
├── dags/                    # Generated Airflow DAGs
├── jobs/                    # Generated DBT job files
├── mappings/                # Mapping Excel files
├── models/                  # Generated DBT models
├── scripts/                 # Core functionality scripts
│   ├── dag_generators.py    # DAG generation scripts
│   ├── dbt_job_generator.py # DBT job generation scripts
│   ├── dbt_model_generator.py # DBT model generation scripts
│   ├── excel_to_json.py     # Excel to JSON conversion
│   ├── model_mapper.py      # Model mapping functionality
│   └── utils.py             # Utility functions
├── dag_generator_app.py     # Main application file
├── README.md                # Documentation
└── requirements.txt         # Dependencies
```

## Setup

1. Ensure you have Python 3.8+ installed.
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Running the Application

```bash
python dag_generator_app.py
```

### Workflow

1. **Generate Mapping Template**:
   - Go to the "Mapping Tools" tab
   - Select a DDL file using the "Browse" button
   - Click "Generate Mapping" to create an Excel template
   - Fill in the SOURCE_TABLE field in the generated Excel file (format: DATABASE.SCHEMA.TABLE)

2. **Fill Model Mapping**:
   - Select the completed mapping file
   - Click "Fill Model Mapping" to connect to Snowflake and populate column details

3. **Generate Files**:
   - Go to the "DAG Generator" tab
   - Select the completed mapping file
   - Click "Generate Files" to create DBT model, job, and DAG files

### Mapping File Format

The mapping Excel file contains two sheets:

1. **Mapping Sheet**:
   - TARGET_TABLE: The target table name
   - SOURCE_TABLE: The source table name (format: DATABASE.SCHEMA.TABLE)
   - SOURCE_TYPE: The source type (source or ref)
   - SOURCE_NAME: The source name
   - Column mappings: Target columns, source columns, and transformation logic
   - JOIN_TABLES: Optional JOIN clauses
   - WHERE_CONDITIONS: Optional WHERE conditions
   - GROUP BY: Optional GROUP BY clauses

2. **Config Sheet**:
   - DAG Type: DATASET DEPENDENCY, CRON, or SNS
   - Schedule Interval: Cron expression for scheduling
   - Dependencies: For dataset dependency DAGs
   - Snowflake Configuration: Connection details

## Troubleshooting

- **File Not Found**: If you encounter file not found errors, check the file paths in the application.
- **Snowflake Connection**: Ensure your Snowflake credentials are correct in the Config sheet.
- **Missing SOURCE_TABLE**: Make sure to fill in the SOURCE_TABLE field in the mapping file before using "Fill Model Mapping".

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
