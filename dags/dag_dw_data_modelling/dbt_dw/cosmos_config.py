from cosmos.config import ProfileConfig, ProjectConfig
from pathlib import Path

DBT_CONFIG = ProfileConfig(
    profile_name='gcp_bigquery',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/dags/dag_dw_data_modelling/dbt_dw/profiles/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/dags/dag_dw_data_modelling/dbt_dw/',
)