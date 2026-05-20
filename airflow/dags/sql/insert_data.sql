-- Criar a tabela caso ela não exista
CREATE TABLE IF NOT EXISTS vendas_consolidadas (
    id INTEGER PRIMARY KEY,
    valor REAL,
    nome TEXT
);

DELETE FROM vendas_consolidadas;

-- Inserir dados de exemplo (ou os dados que seu ETL gerou)
INSERT OR REPLACE INTO vendas_consolidadas (id, valor, nome)
VALUES (1, 100.0, 'Cris'), (2, 200.0, 'Saraiva');