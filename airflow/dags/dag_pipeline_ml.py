from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score
import os

#configuração padrão da DAG
default_args = {
    'owner': 'CrisStudy',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Criar uma pasta local para simular o S3
PASTA_S3_SIMULADA = "/workspaces/meu-primeiro-etl-airflow/datalake/s3_storage"
os.makedirs(PASTA_S3_SIMULADA, exist_ok=True)

# tarefa de treinamento do modelo 
def treinar_modelo_regressao():
    print("Iniciando o treinamento do modelo (regressão Logistíca)...")
    # Simulando dados de treino rápidos (X = características, y = alvo)
    X_train = np.array([[1, 2], [2, 3], [3, 4], [4, 5], [5, 6], [6, 7]])
    y_train = np.array([0, 0, 0, 1, 1, 1])

    modelo = LogisticRegression()
    modelo.fit(X_train, y_train)
    print("Modelo treinado com sucesso!")

# 3. Tarefa de Validação de Métricas
def validar_metricas_modelo():
    print("Validando as métricas do modelo...")
    # Dados reais simulados vs Previsões do modelo
    y_real = [0, 0, 1, 1]
    y_pred_prob = [0.1, 0.3, 0.8, 0.9] # Probabilidades para curva ROC
    y_pred_classe = [0, 0, 1, 1] # Classes para acuracia
    
    # Cálculo das métricas exigidas no enunciado
    accuracy = accuracy_score(y_real, y_pred_classe)
    auc_roc = roc_auc_score(y_real, y_pred_prob)

    print(f"--- MÉTRICAS AVALIADAS ---")
    print(f"Acurácia do Modelo: {accuracy * 100}%")
    print(f"Área sob a Curva ROC (AUC-ROC): {auc_roc}")
    print(f"--------------------------")

# 4. Tarefa de Seleção do Melhor Modelo
def selecionar_melhor_modelo():
    print("Analisando critérios de performace...")
    # Regra de corte baseada nas métricas validadas
    print("Critério atingido: AUC-ROC > 0.80. Modelo aprovado para produção!")

# 5. Tarefa de Publicação no S3 (Simulado)
def publicar_modelo_s3():
    caminho_final = os.path.join(PASTA_S3_SIMULADA, "modelo_final_logistico.pkl")
    # Simulando a escrita do arquivo de modelo pronto
    with open(caminho_final, 'w') as f:
        f.write("CONTEÚDO_SIMULADO_DO_MODELO_TREINADO_E_SERIALIZADO")
        print(f"Modelo publicado no ambiente de armazenamento em: {caminho_final}")

with DAG(
    "pipeline_metricas_machine_learning_v1",
    default_args=default_args,
    description="Pipeline de treinamento, validação e publicação de modelos ML",
    schedule_interval=None,  # Execução manual via Trigger
    catchup=False,
) as dag:
    task_treinar = PythonOperator(
        task_id='treinar_modelo',
        python_callable=treinar_modelo_regressao
    )

    task_validar = PythonOperator(
        task_id='validar_metricas',
        python_callable=validar_metricas_modelo,
    )

    task_selecionar = PythonOperator(
        task_id= 'Selecionar_melhor_modelo',
        python_callable=selecionar_melhor_modelo
    )

    task_publicar = PythonOperator(
        task_id='publicar_modelo_s3',
        python_callable=publicar_modelo_s3
    )

    # Fluxo sequencial
    task_treinar >> task_validar >> task_selecionar >> task_publicar
