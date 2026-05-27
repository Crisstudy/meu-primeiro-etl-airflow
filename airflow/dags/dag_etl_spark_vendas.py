import matplotlib.pyplot as plt
import pandas as pd
import os

def gerar_relatorio_com_grafico():
    # 1. Caminhos dos arquivos na Camada Gold
    caminho_csv = "/workspaces/meu-primeiro-etl-airflow/datalake/gold/faturamento_diario.csv"
    caminho_grafico = "/workspaces/meu-primeiro-etl-airflow/datalake/gold/grafico_faturamento.png"
    caminho_documento = "/workspaces/meu-primeiro-etl-airflow/datalake/gold/relatorio_gerencial.md"
    
    # 2. Carrega os dados gerados pelo Spark para criar o gráfico
    df = pd.read_csv(caminho_csv)
    
    # 3. Construção do Gráfico Gerencial via Matplotlib
    plt.figure(figsize=(8, 4))
    plt.plot(df['Data'], df['Faturamento_R$'], marker='o', color='#1f77b4', linewidth=2)
    plt.title('Relatório Gerencial - Faturamento Diário (Camada Gold)', fontsize=12, fontweight='bold', pad=15)
    plt.xlabel('Data')
    plt.ylabel('Faturamento (R$)')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    
    # Salva o gráfico como imagem física no Data Lake
    plt.savefig(caminho_grafico, dpi=100)
    plt.close()
    
    print(f"Sucesso: Gráfico gerencial salvo em {caminho_grafico}")

# Força o Airflow a rodar o EmailOperator em modo de teste (ignora o envio real)
os.environ["AIRFLOW__SMTP__DRY_RUN"] = "True"
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime

# Garante que o Airflow saiba onde estamos trabalhando
os.environ['AIRFLOW_HOME'] = '/workspaces/meu-primeiro-etl-airflow'

# Definição dos caminhos dos relatórios gerados pelo Spark que vamos enviar
CAMINHO_RELATORIO_DIA = '/workspaces/meu-primeiro-etl-airflow/datalake/gold/vendas_por_dia/dados.csv'
CAMINHO_ESTOQUE_BAIXO = '/workspaces/meu-primeiro-etl-airflow/datalake/gold/estoque_baixo/dados.csv'

def simular_envio_email():
    print("--- SIMULAÇÃO DE DISPARO DE E-MAIL ---")
    print("Destinatário: cristinafigueiredosaraiva@gmail.com")
    print("Assunto: Relatório Final de Vendas - Camada Gold (Airflow + Spark)")
    print("Status: Relatório enviado com sucesso via simulação local!")
    print("--------------------------------------")

with DAG(
    'atv_etl_spark_e_airflow_v1',
    default_args={'owner': 'CrisStudy'},
    schedule=None,  # Execução manual via Trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['Spark', 'ETL', 'Faculdade']
) as dag:

    # Task 1: Executa o script do Spark que você acabou de testar no terminal
    rodar_spark_etl = BashOperator(
        task_id='executar_pyspark_pipeline',
        bash_command='python /workspaces/meu-primeiro-etl-airflow/processar_com_spark.py'
    )

    # Task 2: Envia o relatório final gerado na camada Gold por e-mail (Requisito 5)
   # Task 2: Simula o envio do relatório final gerado na camada Gold por e-mail
    enviar_relatorio_email = PythonOperator(
        task_id='enviar_relatorio_gold',
        python_callable=simular_envio_email
    )

    # Definindo a ordem das tarefas: Primeiro o Spark processa, depois o e-mail envia!
    rodar_spark_etl >> enviar_relatorio_email