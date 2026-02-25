# import polars as pl 
import pandas as pd 
from datetime import datetime


try:
    inicio = datetime.now()
    print('Lendo arquivo parquet')
    df_bolsa_familia = pd.read_parquet('./aula13/bolsa_familia.parquet') # Necessario o fastparquet para rodar em pandas!! pip install fastparquet

    print(df_bolsa_familia.head())
    fim = datetime.now()

    print(f'Tempo total de execução: {fim - inicio}')


except Exception as e:
    print(f'Erro ao obter dados parquet...{e}')