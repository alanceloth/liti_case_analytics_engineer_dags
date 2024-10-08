from airflow.decorators import dag
from datetime import datetime
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ProjectConfig, RenderConfig
from cosmos.constants import LoadMode
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.bash_operator import BashOperator

from airflow.models.baseoperator import chain

# Definindo as configurações do dbt
from dag_dw_data_modelling.dbt_dw.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG

# Definindo a DAG com o decorador @dag
@dag(
    start_date=datetime(2024, 9, 1),
    schedule=None,
    catchup=False,
    tags=['etl', 'dw', 'data_modelling','dbt', 'medallion'],
    default_args={
        'owner': 'Alan Lanceloth'
    }
)
def etl_medallion_architecture():
    bucket_name = 'data-lake-bucket-liti-case-analytics-engineer'

    # # Tarefa para rodar dbt deps antes das transformações
    # dbt_deps = BashOperator(
    #     task_id='dbt_deps',
    #     bash_command=f'cd {DBT_PROJECT_CONFIG.dbt_project_path} && dbt deps',
    #     retries=3,
    # )

    # TaskGroup para a transformação dos dados do GCS para a camada Bronze
    transform_bronze = DbtTaskGroup(
        group_id='transform_bronze',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/bronze']
        )
    )

    # TaskGroup para a transformação dos dados da camada Bronze para a Silver
    transform_silver = DbtTaskGroup(
        group_id='transform_silver',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/silver']
        )
    )

    # TaskGroup para a transformação dos dados da camada Silver para a Gold
    transform_gold = DbtTaskGroup(
        group_id='transform_gold',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/gold']
        )
    )

    # Encadeando as tarefas
    chain(
        #dbt_deps,
        transform_bronze,
        transform_silver,
        transform_gold
    )

etl_medallion_architecture()
