# Importe das bibliotecas com as funções necessárias para o DAG de retreino do modelo de machine learning.
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import random

#Definne as regras de segurança da Dag
default_args = {
    'owner': 'CrisStudy',
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}
# Funções que executam as tarefas reais.

# simula a chegada de novos dados.
def carregar_dados_diarios():
    print("Coletando dados diários de transaçoes e comportamento de Clientes...")
    print("Sucesso: Dados carregados na camada bronze para avaliação.")

# Calcula a precisão do modelo atual. (pode simular dados bons ou ruins)
def avaliar_modelo_atual(**kwargs):
    print("Calculando métricas de performance dos novos dados...")
    precisao_atual = random.uniform(0.4, 0.9)
    print(f"Métricas avaliadas - Precisão Atual: {precisao_atual * 100:.2f}%")
    # Guarda o valor da precisão para a próxima tarefa ler
    kwargs['ti'].xcom_push(key='metric_precision', value=precisao_atual)

# Toma a precisão se vai para o retreino ou não
def avaliar_decisao_retreino(**kwargs):
    ti = kwargs['ti']
    precisao = ti.xcom_pull(key='metric_precision', task_ids='avaliar_modelo')

    if precisao < 0.60:
        print(f"Alerta: precisao de {precisao * 100:.2f}% abaixo de 60%. Retreinando!")
        return 'Melhorar_base_treinamento'
    else:
        print(f"Precisão de {precisao * 100:.2f}% satisfatória. Não será necessário retreinar.")
        return 'fim'
    
# Junta os dados novos aos antigos se o modelo estiver ruim 
def melhorar_base_dados():
    print("Adcionando os novos dados diarios ao conjunto de dados histórico...")
    print("Garantindo a remoção de duplicatas e evitar a contaminação de dados.")

# Estrutura da Dag que transforma as funções Python em tarefas oficial do Airflow
with DAG(
    'pipeline_retreino_automatizado_ml',
    default_args=default_args,
    description='DAG de retreino automatizado do modelo de machine learning',
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    task_carga = PythonOperator(
        task_id = 'carregar_dados_reais',
        python_callable=carregar_dados_diarios
    )

    task_aval= PythonOperator(
        task_id = 'avaliar_modelo',
        python_callable=avaliar_modelo_atual
    )

    task_decisao = PythonOperator(
        task_id= 'decisao_de_retreino',
        python_callable=avaliar_decisao_retreino
    )

    task_melhoria = PythonOperator(
        task_id = 'Melhorar_base_treinamento',
        python_callable=melhorar_base_dados
    )

# Chama a outra DAG automaticamente
    task_trigger = TriggerDagRunOperator(
        task_id='retreinar_modelo_trigger',
        trigger_dag_id='pipeline_metricas_machine_learning_v1'
    )

    task_saudavel = PythonOperator(
        task_id = 'modelo_esta_saudavel',
        python_callable=lambda: print("fim do monitoramento. Modelo saudável")
    )

    # Dependências entre as tarefas

    # O fluxo principal se divide na decisão
    task_carga >> task_aval >> task_decisao

    # Caminho A: Se precisar de retreino
    task_decisao >> task_melhoria >> task_trigger

    # Caminho B: Se o modelo estiver bom
    task_decisao >> task_saudavel