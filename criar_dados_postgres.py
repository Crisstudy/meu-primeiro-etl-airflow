import sqlite3
import pandas as pd

def inicializar_banco_de_dados():
    # Vamos usar o SQLite porque ele imita o PostgreSQL localmente sem precisar de senhas
    conn = sqlite3.connect('vendas_postgres.db')
    cursor = conn.cursor()

    # 1. Criar Tabela de Clientes
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            city TEXT
        )
    ''')

    # 2. Criar Tabela de Produtos
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            price REAL,
            stock INTEGER
        )
    ''')

    # 3. Criar Tabela de Pedidos
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_date TEXT,
            status TEXT
        )
    ''')

    # 4. Criar Tabela de Itens do Pedido
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            subtotal REAL
        )
    ''')

    # Inserir dados fictícios de teste
    cursor.execute("INSERT OR IGNORE INTO customers VALUES (1, 'Cris Figueiredo', 'Anapolis'), (2, 'Maria Silva', 'Goiania')")
    cursor.execute("INSERT OR IGNORE INTO products VALUES (101, 'Notebook', 3500.00, 2), (102, 'Mouse Wireless', 150.00, 25), (103, 'Teclado Mecanico', 300.00, 4)")
    cursor.execute("INSERT OR IGNORE INTO orders VALUES (1001, 1, '2026-05-19', 'COMPLETED'), (1002, 2, '2026-05-20', 'COMPLETED')")
    cursor.execute("INSERT OR IGNORE INTO order_items VALUES (1, 1001, 101, 1, 3500.00), (2, 1001, 102, 2, 300.00), (3, 1002, 103, 1, 300.00)")

    conn.commit()
    conn.close()
    print("Banco de dados simulado com sucesso!")

if __name__ == "__main__":
    inicializar_banco_de_dados()