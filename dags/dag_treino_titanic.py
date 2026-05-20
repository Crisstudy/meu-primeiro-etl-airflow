import os
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import joblib

# Configuração de Ambiente
os.environ['AIRFLOW_HOME'] = '/workspaces/meu-primeiro-etl-airflow'

def treinar_modelo_titanic():
    caminho_dados = '/workspaces/meu-primeiro-etl-airflow/data/train.csv'
    caminho_modelos = '/workspaces/meu-primeiro-etl-airflow/models/'
    
    if not os.path.exists(caminho_modelos):
        os.makedirs(caminho_modelos)

    # 1. Carregar Dataset
    if not os.path.exists(caminho_dados):
        raise FileNotFoundError(f"O arquivo {caminho_dados} não foi encontrado!")
        
    df = pd.read_csv(caminho_dados)
    
    # 2. Pré-processamento Simples
    cols = ['Survived', 'Pclass', 'Sex', 'Age', 'SibSp', 'Parch']
    df = df[cols].dropna()
    df['Sex'] = df['Sex'].map({'male': 0, 'female': 1})
    
    X = df.drop('Survived', axis=1)
    y = df['Survived']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 3. Treinar Modelo
    model = RandomForestClassifier(n_estimators=100)
    model.fit(X_train, y_train)
    
    # 4. Salvar
    joblib.dump(model, os.path.join(caminho_modelos, 'titanic_model.pkl'))
    X_test.to_csv('/workspaces/meu-primeiro-etl-airflow/data/X_test.csv', index=False)
    y_test.to_csv('/workspaces/meu-primeiro-etl-airflow/data/y_test.csv', index=False)
    print("Sucesso: Modelo e dados de teste salvos.")

# Definição explícita da DAG
with DAG(
    'atv7_treino_titanic_v1', # Mudei o ID para forçar o Airflow a atualizar
    default_args={
        'owner': 'CrisStudy',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Pipeline de Treinamento Titanic - Atividade 7',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ML', 'Titanic'],
) as dag_objeto:

    tarefa_treino = PythonOperator(
        task_id='executar_treino_random_forest',
        python_callable=treinar_modelo_titanic
    )

    tarefa_treino