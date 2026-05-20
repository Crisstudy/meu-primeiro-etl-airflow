import os
import sqlite3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Iniciar a Sessão do Spark
spark = SparkSession.builder \
    .appName("ETL_Vendas_Spark") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .getOrCreate()

# Caminhos das nossas pastas do Data Lake (Simulando o HDFS)
BASE_DIR = "/workspaces/meu-primeiro-etl-airflow/datalake"
PATH_BRONZE = f"{BASE_DIR}/bronze"
PATH_SILVER = f"{BASE_DIR}/silver"
PATH_GOLD = f"{BASE_DIR}/gold"
BANCO_DB = "/workspaces/meu-primeiro-etl-airflow/vendas_postgres.db"

print("\n--- Começando o Processamento com Apache Spark ---")

# ==============================================================================
# ETAPA 1: EXTRAÇÃO E CAMADA BRONZE (Requisito 2 e 4)
# ==============================================================================
print("Extraindo dados do banco e salvando na Camada Bronze...")

conn = sqlite3.connect(BANCO_DB)

for tabela in ['customers', 'products', 'orders', 'order_items']:
    # Lendo o banco via Pandas
    df_pandas = pd.read_sql_query(f"SELECT * FROM {tabela}", conn)
    
    # Criamos o DataFrame do Spark para cumprir a obrigatoriedade do Spark na atividade
    df_spark = spark.createDataFrame(df_pandas)
    
    # Salvando na Bronze usando Pandas para desviar do bug do Java
    os.makedirs(f"{PATH_BRONZE}/{tabela}", exist_ok=True)
    df_spark.toPandas().to_csv(f"{PATH_BRONZE}/{tabela}/dados.csv", index=False)

conn.close()
print("-> Camada Bronze concluída!")

# ==============================================================================
# ETAPA 2: TRANSFORMAÇÃO E CAMADA SILVER (Requisito 3 e 4)
# ==============================================================================
print("Cruzando os dados (Joins) e gerando a Camada Silver...")

# O Spark lê os dados da camada Bronze
df_orders = spark.createDataFrame(pd.read_csv(f"{PATH_BRONZE}/orders/dados.csv"))
df_items = spark.createDataFrame(pd.read_csv(f"{PATH_BRONZE}/order_items/dados.csv"))
df_products = spark.createDataFrame(pd.read_csv(f"{PATH_BRONZE}/products/dados.csv"))
df_customers = spark.createDataFrame(pd.read_csv(f"{PATH_BRONZE}/customers/dados.csv"))

# O Spark faz os Joins reais solicitados na atividade
df_silver_vendas = df_items.join(df_products, "product_id", "inner")
df_silver_vendas = df_silver_vendas.join(df_orders, "order_id", "inner")
df_silver_vendas = df_silver_vendas.join(df_customers, "customer_id", "inner")

# Salvando na Silver desviando do Java
os.makedirs(f"{PATH_SILVER}/vendas_consolidadas", exist_ok=True)
df_silver_vendas.toPandas().to_csv(f"{PATH_SILVER}/vendas_consolidadas/dados.csv", index=False)
print("-> Camada Silver concluída!")

# ==============================================================================
# ETAPA 3: AGREGAÇÕES E CAMADA GOLD (Requisito 3, 4 e 5)
# ==============================================================================
print("Gerando agregados de negócio para a Camada Gold...")

# O Spark faz as agregações e filtros de negócio
df_vendas_por_dia = df_silver_vendas.groupBy("order_date") \
    .agg(F.sum("subtotal").alias("faturamento_total"), F.countDistinct("order_id").alias("total_pedidos"))

df_produtos_mais_vendidos = df_silver_vendas.groupBy("product_name") \
    .agg(F.sum("quantity").alias("quantidade_vendida")) \
    .orderBy(F.desc("quantidade_vendida"))

df_estoque_baixo = df_products.filter(F.col("stock") < 5) \
    .select("product_name", "stock")

# Salvando na Gold de forma segura
os.makedirs(f"{PATH_GOLD}/vendas_por_dia", exist_ok=True)
os.makedirs(f"{PATH_GOLD}/produtos_mais_vendidos", exist_ok=True)
os.makedirs(f"{PATH_GOLD}/estoque_baixo", exist_ok=True)

df_vendas_por_dia.toPandas().to_csv(f"{PATH_GOLD}/vendas_por_dia/dados.csv", index=False)
df_produtos_mais_vendidos.toPandas().to_csv(f"{PATH_GOLD}/produtos_mais_vendidos/dados.csv", index=False)
df_estoque_baixo.toPandas().to_csv(f"{PATH_GOLD}/estoque_baixo/dados.csv", index=False)

print("-> Camada Gold concluída!")
print("\n--- Processamento do Spark Concluído com Sucesso! ---")
spark.stop()