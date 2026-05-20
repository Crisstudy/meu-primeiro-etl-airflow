import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime  

os.environ['AIRFLOW_HOME'] = '/workspaces/meu-primeiro-etl-airflow'

def ler_arquivo():
    with open('data/train.csv', 'r') as file:
        data =file.readlines()
    return data

def contar_linhas(data=None):
    if data is None:
        return 0
    return len(data)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,1,1)
}

dag= DAG('contagem_linhas_dag', default_args=default_args, schedule_interval=None)

ler_arquivo_task= PythonOperator(
    task_id='ler_arquivo',
    python_callable=ler_arquivo,
    dag=dag
)

contar_linhas_task= PythonOperator(
    task_id='contar_linhas',
    python_callable=contar_linhas,
    provide_context=True,
    dag=dag                        
)

ler_arquivo_task >> contar_linhas_task