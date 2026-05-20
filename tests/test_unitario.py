import pytest
from airflow.example_dags.dag_contagem_linhas import ler_arquivo, contar_linhas

def test_ler_arquivo():
    data =ler_arquivo()
    assert type(data) is list


def test_contar_linhas():
    data = ['linha1\n', 'linha2\n']
    result = contar_linhas(data)
    assert result == 2