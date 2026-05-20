from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import joblib
import os
from sklearn.metrics import accuracy_score

def realizar_inferencia_e_metricas():
    # Caminhos
    caminho_modelo = '/workspaces/meu-primeiro-etl-airflow/models/titanic_model.pkl'
    caminho_teste_x = '/workspaces/meu-primeiro-etl-airflow/data/X_test.csv'
    caminho_teste_y = '/workspaces/meu-primeiro-etl-airflow/data/y_test.csv'
    caminho_output = '/workspaces/meu-primeiro-etl-airflow/data/previsoes_titanic.csv'

    # 1. Carregar Modelo e Dados
    model = joblib.load(caminho_modelo)
    X_test = pd.read_csv(caminho_teste_x)
    y_true = pd.read_csv(caminho_teste_y)

    # 2. Realizar Previsões (Inferência)
    y_pred = model.predict(X_test)

    # 3. Calcular Acurácia (Requisito 4 da atividade)
    acuracia = accuracy_score(y_true, y_pred)
    
    # 4. Salvar Resultados
    resultados = X_test.copy()
    resultados['Real'] = y_true
    resultados['Previsao'] = y_pred
    resultados.to_csv(caminho_output, index=False)

    # Log de métricas para o Airflow
    print(f"Métrica de Avaliação - Acurácia do Modelo: {acuracia:.4f}")

with DAG(
    'atv7_inferencia_titanic_v1',
    default_args={'owner': 'CrisStudy'},
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ML', 'Inferencia']
) as dag:

    tarefa_predicao = PythonOperator(
        task_id='gerar_previsoes_e_metricas',
        python_callable=realizar_inferencia_e_metricas
    )