# %% [markdown]
# **O QUE ESSE CÓDIGO FAZ?**
# 
# Esse código lê o dicionário que construímos da tabela (01_doc\dicionario_rais_vinc_sebrae.xlsx) e importa as abas de dimensão pro MongoDB e para o PostgreSQL no Dbeaver.
# 
# OBS: o código só precisará ser rodado novamente se houver mudança no layout da tabela (ex: acréscimo de colunas/categorias). 

# %% [markdown]
# # **1. IMPORTANDO LIBS** 

# %%
from datetime import datetime
from datetime import date
import json
import pandas as pd
import psycopg2
import pymongo as p


# %% [markdown]
# # **2. CRIANDO FUNÇÕES** 

# %%
def load_config(file_path=r'..\config.json'):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

# %%
# Função para fazer upload no mongodb seguindo o padrão das demais funções do tipo que já utilizamos

def upload_mongodb(df, arquivo):

    config = load_config()
    
    print("------- Conectando com o mongodb ------------------")     
    

    client = p.MongoClient(f"f"mongodb://{config['mongodb']['user_name']}:{config['mongodb']['password']}@{config['mongodb']['host']}:{config['mongodb']['port']}/{config['mongodb']['db_name']}")
    db = client.SEBRAE
    type(client)

    print(client.list_database_names())
    print("------- Carregando os dados para o mongodb ------------------")    
    print(f"Lendo o  arquivo: {arquivo} e convertendo para dict pandas...\n")
    data = df.to_dict(orient="records")
    db = client["SEBRAE"]
    col = db[arquivo]

    print("\nIniciando a gravação no mongodb...\n ")
    col.insert_many(data)
    print(f"{arquivo} gravado com sucesso!")
    print("\n------- fim  da carga dos dados para o mongodb ------------------\n")       


# %%
def transfer_to_postgres(data, collection_name):
    print("------- Conectando com o PostgreSQL ------------------")     

    config = load_config()

    # Conectando ao PostgreSQL
    conn = psycopg2.connect(
        host=config['postgresql']['pg_host'],
        port=config['postgresql']['pg_port'],
        dbname=config['postgresql']['pg_db_name'],
        user=config['postgresql']['pg_user'],
        password=config['postgresql']['pg_password']
    )

    # Criando um cursor
    cur = conn.cursor()

    # Verificando se a tabela existe
    cur.execute(f"SELECT to_regclass('stg_rais.{collection_name}');")
    table_exists = cur.fetchone()[0]

    # Se a tabela não existir, criamos ela
    if not table_exists:
        print(f"A tabela {collection_name} não existe, criando...")
        
        # Obtendo os nomes das colunas da collection do MongoDB
        columns = data[0].keys()
        
        # Determinando o tipo de cada coluna
        column_types = []
        for column in columns:
            unique_values = set(record[column] for record in data)
            max_length = max(len(str(value)) for value in unique_values)
            if isinstance(data[0][column], int):
                if max(record[column] for record in data) < 32767:  # smallint range
                    column_types.append(f"{column} smallint")
                elif max(record[column] for record in data) < 2147483647:  # int range
                    column_types.append(f"{column} int")
                else:
                    column_types.append(f"{column} bigint")
            else:
                column_types.append(f"{column} varchar({max_length})")
        
        # Criando a tabela no PostgreSQL com as mesmas colunas
        columns_query = ", ".join(column_types)
        create_table_query = f"CREATE TABLE stg_rais.{collection_name} ({columns_query});"
        cur.execute(create_table_query)

    # Inserindo os dados na tabela
    print("------- Transferindo os dados para o PostgreSQL ------------------")  
    for record in data:
        columns = ', '.join(record.keys())
        placeholders = ', '.join(['%s'] * len(record))
        insert_query = f"INSERT INTO stg_rais.{collection_name} ({columns}) VALUES ({placeholders})"
        cur.execute(insert_query, list(record.values()))

    # Commit das alterações e fechamento da conexão
    conn.commit()
    cur.close()
    conn.close()

    print(f"{collection_name} transferido com sucesso!")


# %% [markdown]
# # **3. EXECUTANDO**

# %% [markdown]
# ## **3.1 DEFININDO CAMINHO DA PLANILHA DO DICIONÁRIO, LENDO ELA E IMPRIMINDO LISTA COM AS ABAS EXISTENTES** 

# %%
# Definindo caminho e imprimindo uma lista com todas as sheets da planilha
ds_owner = input('Insira seu nome e sobrenome (Ex: Marcilio Duarte)')
path = input('Insira o caminho do arquivo excel onde estão as dimensões/tabelas (dicionário): ')
xls = pd.ExcelFile(path)


# %%
sheets = xls.sheet_names[1:-17]
print(sheets)

# %% [markdown]
# ## **3.2 RODANDO** 

# %%
for i in sheets:
    df = pd.read_excel(path, sheet_name=i)
    df['curr_date'] = str(date.today())
    df['ds_owner'] = ds_owner
    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    coll_name = str.upper('tb_rais_vinc'+i[2:])
    upload_mongodb(df=df, arquivo=coll_name)
    # Suponha que df seja o seu DataFrame
    df = df.to_dict('records')
    transfer_to_postgres(data=df, collection_name=coll_name)


