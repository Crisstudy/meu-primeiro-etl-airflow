from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import shutil
import os
os.environ['AIRFLOW_HOME'] = '/workspaces/meu-primeiro-etl-airflow'

default_args = {
    'owner': 'CrisStudy',
    'retries': 2, # Requisito 5: Retry
    'retry_delay': timedelta(minutes=5),
}

import sqlite3
import pandas as pd

def transformacao_pandas():
    # Caminhos organizados (Modelo do Professor)
    caminho_csv = '/workspaces/meu-primeiro-etl-airflow/dados/etl/results/resultado_final.csv'
    caminho_db = '/workspaces/meu-primeiro-etl-airflow/dados/etl/results/meu_banco.db'
    
    # Requisito 3: Transformação
    df_vendas = pd.DataFrame({'id': [1, 2], 'valor': [100.0, 200.0]})
    df_clientes = pd.DataFrame({'id': [1, 2], 'nome': ['Cris', 'Saraiva']})
    df_final = pd.merge(df_vendas, df_clientes, on='id')
    
    # Salva o CSV (para manter o modelo das pastas)
    df_final.to_csv(caminho_csv, index=False)
    
    # Requisito 4: Carga direta no SQLite via Pandas
    conn = sqlite3.connect(caminho_db)
    df_final.to_sql('vendas_consolidadas', conn, if_exists='replace', index=False)
    conn.close()
    print("Dados carregados no banco com sucesso!")

# def transformacao_pandas():
#     #Requisito 3: Transformação e Join com Pandas
#     #imulando a leitura de dados extraídos
#     df_vendas = pd.DataFrame({'id': [1, 2], 'valor': [100, 200]})
#     df_clientes = pd.DataFrame({'id': [1, 2], 'nome': ['Cris', 'Saraiva']})     
#     df_final = pd.merge(df_vendas, df_clientes, on='id')
#     df_final.to_csv('dados/etl/results/resultado_final.csv', index=False)

with DAG(
    dag_id='pipeline_etl_pratica',
    default_args=default_args,
    start_date=datetime(2020, 1, 1),
    schedule_interval='@daily',
    catchup=False, # Requisito 5: Evitar múltiplas execuções
    template_searchpath='/workspaces/meu-primeiro-etl-airflow/dags'
) as dag:

    # Task 1: Extração (Simulada)
    extract = PythonOperator(task_id='extrair_s3', python_callable=lambda: print("Extraindo do S3..."))

    # Task 2: Transformação Pandas
    transform = PythonOperator(task_id='transformar_dados', python_callable=transformacao_pandas)

    # Task 3: Carga SQL (Requisito 4)
    #load = SQLExecuteQueryOperator(task_id='carregar_banco', sql='sql/insert_data.sql', conn_id='db_local')

    # Task 4: Cleanup (Requisito 6)'))
    cleanup = PythonOperator(task_id='limpeza_arquivos', python_callable=lambda: os.remove('dados/etl/results/resultado_final.csv'))

    extract >> transform >> cleanup