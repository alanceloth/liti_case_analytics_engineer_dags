# dags/sync_dags.py

import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

def sync_dags():
    # Configurações
    gcs_bucket = 'airflow-dags-bucket-liti-case-analytics-engineer' 
    gcs_prefix = 'dags/'
    local_dag_folder = '/usr/local/airflow/dags/'

    # Garantir que o diretório local existe
    if not os.path.exists(local_dag_folder):
        logging.error(f"Diretório local para DAGs não existe: {local_dag_folder}")
        raise FileNotFoundError(f"Diretório local para DAGs não existe: {local_dag_folder}")

    # Inicializa o hook do GCS
    gcs = GCSHook(gcp_conn_id='google_cloud_default')

    # Lista os blobs (arquivos) no bucket
    blobs = gcs.list(bucket_name=gcs_bucket, prefix=gcs_prefix)

    # Log para inspecionar o conteúdo de 'blobs'
    logging.info(f"Blobs retornados: {blobs}")

    for blob in blobs:
        # 'blob' é uma string representando o caminho do objeto no GCS
        file_name = os.path.basename(blob)

        # Opcional: Ignorar arquivos de sistema ou outros arquivos específicos
        # Exemplo: Ignorar arquivos que começam com '.', como '.DS_Store'
        if file_name.startswith('.'):
            logging.info(f"Ignorando arquivo de sistema oculto: {relative_path}")
            continue

        # Caminho completo local mantendo a estrutura de pastas
        local_path = os.path.join(local_dag_folder, relative_path)
        local_dir = os.path.dirname(local_path)

        # Garantir que o diretório local para o arquivo existe
        if not os.path.exists(local_dir):
            try:
                os.makedirs(local_dir, exist_ok=True)
                logging.info(f"Criado diretório local: {local_dir}")
            except Exception as e:
                logging.error(f"Falha ao criar diretório local {local_dir}: {e}")
                raise

        # Baixa o arquivo do GCS
        try:
            gcs.download(bucket_name=gcs_bucket, object_name=blob, filename=local_path)
            logging.info(f"Arquivo baixado com sucesso: {relative_path}")
        except Exception as e:
            logging.error(f"Falha ao baixar o arquivo {relative_path}: {e}")
            raise

def cleanup_dags():
    # Opcional: Implementar limpeza de DAGs antigas ou não mais necessárias
    pass

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='sync_dags',
    default_args=default_args,
    description='DAG para sincronizar todo o conteúdo do GCS para o Airflow mantendo a estrutura de pastas',
    schedule_interval='@daily',  # Defina conforme sua necessidade
    catchup=False,
) as dag:

    sync_task = PythonOperator(
        task_id='sync_dags_from_gcs',
        python_callable=sync_dags,
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_old_dags',
        python_callable=cleanup_dags,
    )

    sync_task >> cleanup_task
