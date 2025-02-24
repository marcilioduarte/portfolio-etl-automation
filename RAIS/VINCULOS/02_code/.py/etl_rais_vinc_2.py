# %% [markdown]
# **O QUE ESSE CÓDIGO FAZ?**
# 
# Esse código transfere a Rais Vínculos (anos de 2010 em diante) do MongoDB e para o PostgreSQL no Dbeaver.

# %% [markdown]
# # **1. IMPORTANDO LIBS** 

# %%
from datetime import datetime
from datetime import date
import json
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession#, functions as F
from pyspark.sql.functions import col, when, asc
from sqlalchemy import create_engine
import time
import urllib

# %% [markdown]
# ## **2.1 CRIANDO FUNÇÕES** 

# %%
def load_config(file_path=r'..\config.json'):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

# %%
def etl_mongodb_to_postgresql(mongo_collection, ano, uf):
    config = load_config()

    # Criando sessão no spark
    spark = SparkSession.builder \
        .appName("ETL_MongoDB_to_PostgreSQL") \
        .enableHiveSupport() \
        .getOrCreate()

    # Lendo dados do MongoDB
    df = spark.read.format("mongo").option("uri", f"mongodb://{config['mongodb']['user_name']}:{config['mongodb']['password']}@{config['mongodb']['host']}:{config['mongodb']['port']}/{config['mongodb']['db_name']}.{mongo_collection}").load()

    # Aplicando tratamentos nos dados
    # Filtrar pela UF
    df = df.drop("_id")
    df = df.filter(col("uf") == uf)

    # Substituindo valores indesejados e tratando valores nulos
    unwanted_values = ["0000-1", "000-1", "00-1", "0-1", "{ñ", "{ñ class}", "{ñ c", "{ñ clas", "{ñ cl}"]
    for col_name in df.columns:
        if col_name == 'uf':
            # Tratando valores nulos na coluna 'uf'
            df = df.withColumn(col_name, when(col(col_name).isNull(), "NI").otherwise(col(col_name)))
        else:
            # Substituindo valores indesejados em outras colunas
            df = df.withColumn(col_name, when(col(col_name).isin(unwanted_values), "-1").otherwise(col(col_name)))

    df = df.withColumnRenamed('tipo_estab41', 'tipo_estab') \
       .withColumnRenamed('tipo_estab42', 'tipo_estab_dsc')

    data_types = {
    "bairros_fortaleza": "short",
    "bairros_rj": "short",
    "causa_afastamento_1": "short",
    "causa_afastamento_2": "short",
    "causa_afastamento_3": "short",
    "motivo_desligamento": "short",
    "cbo_ocupacao_2002": "int",
    "cnae_2_0_classe": "int",
    "cnae_95_classe": "int",
    "distritos_sp": "short",
    "vinculo_ativo_31_12": "short",
    "faixa_etaria": "short",
    "faixa_hora_contrat": "short",
    "faixa_remun_dezem_sm": "short",
    "faixa_remun_media_sm": "short",
    "faixa_tempo_emprego": "short",
    "escolaridade_apos_2005": "short",
    "qtd_hora_contr": "short",
    "idade": "short",
    "ind_cei_vinculado": "short",
    "ind_simples": "short",
    "mes_admissao": "short",
    "mes_desligamento": "short",
    "mun_trab": "int",
    "municipio": "int",
    "nacionalidade": "short",
    "natureza_juridica": "short",
    "ind_portador_defic": "short",
    "qtd_dias_afastamento": "short",
    "raca_cor": "short",
    "regioes_adm_df": "short",
    "vl_remun_dezembro_nom": "decimal(15, 2)",
    "vl_remun_dezembro_sm": "decimal(15, 2)",
    "vl_remun_media_nom": "decimal(15, 2)",
    "vl_remun_media_sm": "decimal(15, 2)",
    "cnae_2_0_subclasse": "int",
    "sexo_trabalhador": "short",
    "tamanho_estabelecimento": "short",
    "tempo_emprego": "short",
    "tipo_admissao": "short",
    "tipo_estab": "short",
    "tipo_estab_dsc": "string",
    "tipo_defic": "short",
    "tipo_vinculo": "short",
    "ibge_subsetor": "short",
    "vl_rem_janeiro_cc": "decimal(15, 2)",
    "vl_rem_fevereiro_cc": "decimal(15, 2)",
    "vl_rem_marco_cc": "decimal(15, 2)",
    "vl_rem_abril_cc": "decimal(15, 2)",
    "vl_rem_maio_cc": "decimal(15, 2)",
    "vl_rem_junho_cc": "decimal(15, 2)",
    "vl_rem_julho_cc": "decimal(15, 2)",
    "vl_rem_agosto_cc": "decimal(15, 2)",
    "vl_rem_setembro_cc": "decimal(15, 2)",
    "vl_rem_outubro_cc": "decimal(15, 2)",
    "vl_rem_novembro_cc": "decimal(15, 2)",
    "ano_chegada_brasil": "short",
    "ind_trab_intermitente": "short",
    "ind_trab_parcial": "short",
    "tipo_salario": "short",
    "vl_salario_contratual": "decimal(15, 2)",
    "uf": "string",
    "ano": "short",
    "curr_date": "date",
    "ds_owner": "string"
    }
            
    for col_name, data_type in data_types.items():
        df = df.withColumn(col_name, df[col_name].cast(data_type))
    
    # Gravar dados nas partições corretas no PostgreSQL
    url = f'{config['postgresql']['pg_url']}'
    properties = {
        "user": f"{config['postgresql']['pg_user']}",
        "password": f"{config['postgresql']['pg_password']}",
        "driver": "org.postgresql.Driver"
    }
    table_name = f'stg_rais.tb_rais_vinculos_{ano}'

    # Dividir o DataFrame em partições e aplicar a função write_to_postgresql em cada partição
    df.write.jdbc(url=url, table=table_name, mode='append', properties=properties)

    # Encerrar a sessão do Spark após o loop
    spark.stop()


# %% [markdown]
# # **2. EXECUTANDO** 

# %%
inicio_geral = datetime.now()
anos = []
while True:
    try:
        ano_inicial = int(input('Digite o ANO INICIAL para o qual você deseja fazer o ETL da RAIS: '))
        ano_final = int(input('Digite o ÚLTIMO ANO para o qual você deseja fazer o ETL da RAIS: '))
        if len(str(ano_inicial)) == 4 and str(ano_inicial).startswith("20") and len(str(ano_final)) == 4 and str(ano_final).startswith("20"):
            for i in range(ano_inicial, ano_final+1):
                anos.append(str(i))
            print("\nA lista final dos anos de referência para o ETL dos dados contém os seguintes anos: ", anos)
            break
        else:
            print("Erro: Por favor, insira anos válidos com quatro dígitos começando com '20', ex: 2022.")
    except ValueError:
        print("Erro: Por favor, insira um número inteiro, ex: 2022.")
print('\nPassando agora para a transferência do MongoDB para o PostgreSQL no Dbeaver...\n')


# %%
uf_list = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"]
inicio_geral = datetime.now()
for ano in anos:
    inicio_ano = datetime.now()
    print(f'Iniciando loop com o ano de {str(ano)} em {inicio_ano}...\n')
    mongo_collection = f"RAIS_VINCULOS_{str(ano)}"
    for uf in uf_list:
        try:        
            print(f"Executando ETL para o ano {str(ano)} e UF {uf}")
            etl_mongodb_to_postgresql(mongo_collection, ano, uf)
            print(f"ETL para o ano {str(ano)} e UF {uf} concluído")
            time.sleep(10)
        except Exception as e:
            print(f"Erro durante a execução do ETL para o ano {str(ano)} e UF {uf}: {e}")
            time.sleep(10)
            print("Continuando com o próximo ano/UF")
    fim_ano = datetime.now()     
    tempo_total_ano = fim_ano - inicio_ano
    print(f'\nLoop de {str(ano)} finalizado com sucesso. O tempo total foi {tempo_total_ano}.\n')
    time.sleep(10)
fim_geral = datetime.now()
tempo_total_geral = fim_geral - inicio_geral
print(f'A execução completa da carga durou {tempo_total_geral}.')


