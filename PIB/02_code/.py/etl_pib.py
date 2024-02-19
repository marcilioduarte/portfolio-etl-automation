# %% [markdown]
# **O QUE ESSE CÓDIGO FAZ?**
# 
# Esse código faz o download, o tratamento e o carregamento dos dados do PIB para o postgresql no Dbeaver.  
# 
# **O QUE É O PIB?**
# 
# O PIB é a soma de todos os bens e serviços finais produzidos por um país, estado ou cidade, geralmente em um ano. Todos os países calculam o seu PIB nas suas respectivas moedas.
# 
# O PIB mede apenas os bens e serviços finais para evitar dupla contagem. Se um país produz R$ 100 de trigo, R$ 200 de farinha de trigo e R$ 300 de pão, por exemplo, seu PIB será de R$ 300, pois os valores da farinha e do trigo já estão embutidos no valor do pão.
# 
# Os bens e serviços finais que compõem o PIB são medidos no preço em que chegam ao consumidor. Dessa forma, levam em consideração também os impostos sobre os produtos comercializados.
# 
# O PIB não é o total da riqueza existente em um país. Esse é um equívoco muito comum, pois dá a sensação de que o PIB seria um estoque de valor que existe na economia, como uma espécie de tesouro nacional.
# 
# Na realidade, o PIB é um indicador de fluxo de novos bens e serviços finais produzidos durante um período. Se um país não produzir nada em um ano, o seu PIB será nulo.
# 
# Portanto, O PIB é apenas um indicador síntese de uma economia. Ele ajuda a compreender um país, mas não expressa importantes fatores, como distribuição de renda, qualidade de vida, educação e saúde. Um país tanto pode ter um PIB pequeno e ostentar um altíssimo padrão de vida, como registrar um PIB alto e apresentar um padrão de vida relativamente baixo.
# 
# **COMO ELE É MENSURADO PELO IBGE?**
# 
# Para o cálculo do PIB, são utilizados diversos dados; alguns produzidos pelo IBGE, outros provenientes de fontes externas. Essas são algumas das peças que compõem o quebra-cabeça do PIB:
# 
# * Balanço de Pagamentos (Banco Central)
# * Declaração de Informações Econômico-Fiscais da Pessoa Jurídica - DIPJ (Secretaria da Receita Federal)
# * Índice de Preços ao Produtor Amplo - IPA (FGV)
# * Índice Nacional de Preços ao Consumidor Amplo - IPCA (IBGE)
# * Produção Agrícola Municipal - PAM - (IBGE)
# * Pesquisa Anual de Comércio - PAC (IBGE)
# * Pesquisa Anual de Serviços - PAS (IBGE)
# * Pesquisa de Orçamentos Familiares - POF (IBGE)
# * Pesquisa Industrial Anual - Empresa - PIA-Empresa (IBGE)
# * Pesquisa Industrial Mensal - Produção Física - PIM-PF (IBGE)
# * Pesquisa Mensal de Comércio - PMC (IBGE)
# * Pesquisa Mensal de Serviços - PMS (IBGE)
# 
# **ANÁLISES QUE PODEM SER FEITAS A PARTIR DO PIB**
# A partir da performance do PIB, pode-se fazer várias análises, tais como:
# 
# * Traçar a evolução do PIB no tempo, comparando seu desempenho ano a ano;
# * Fazer comparações internacionais sobre o tamanho das economias dos diversos países;
# * Analisar o PIB per capita (divisão do PIB pelo número de habitantes), que mede quanto do PIB caberia a cada indivíduo de um país se todos recebessem partes iguais, entre outros estudos.
# 
# ***Fonte das informações:*** https://www.ibge.gov.br/explica/pib.php
# 

# %% [markdown]
# # **1. IMPORTANDO LIBS**:

# %%
print("Importando bibliotecas necessárias...")

import os #manipulação de arquivos/diretórios
from os import walk #manipulação de arquivos/diretórios
from ftplib import FTP #conexão ftp
import requests_ftp #requisições ftp
import pandas as pd #manipulação de dados no python 
import numpy as np #manipulação de dados num.
from datetime import date #manipulação de dados do tipo data/hora
from datetime import datetime #manipulação de dados do tipo data/hora
from psycopg2 import sql
from psycopg2 import extras
import zipfile
import glob
from sqlalchemy import create_engine

print("\nBibliotecas importadas, bora para a execução do script!!")

# %% [markdown]
# # **2. CRIANDO FUNÇÕES**

# %%
def download_ftp_ibge(ftp_server, directory, local_directory, ano):
    # Verificar se o diretório local existe, se não, criar
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    # Conectando ao FTP
    with FTP(ftp_server) as ftp:
        ftp.login()
        ftp.cwd(directory)
        files = ftp.nlst()

        # Lista dos arquivos que queremos baixar
        target_files = ['base_de_dados_2002_2009_xls.zip', f'base_de_dados_2010_{ano}_xlsx.zip']

        
        # Definindo limite de tentativas para 50
        for target in target_files:
            success = False
            attempts = 0

            while not success and attempts < 50:
                try:
                    if target in files:
                        print(f"Baixando {target}...")
                        local_file = os.path.join(local_directory, target)

                        # Usar 'with' para garantir que o arquivo seja fechado
                        with open(local_file, 'wb') as fp:
                            ftp.retrbinary('RETR ' + target, fp.write)

                        print(f"Extraindo {target}...")
                        with zipfile.ZipFile(local_file, 'r') as zip_ref:
                            zip_ref.extractall(local_directory)

                        success = True
                except Exception as e:
                    print(f"Erro: {e}")
                    attempts += 1

# %%
# Criando função de tratamento dos dados
def tratamento_pib(df, ds_owner): 

    # Dropando colunas que não iremos utilizar
    df.drop(columns=['Região Metropolitana', 'Código da Mesorregião',
       'Nome da Mesorregião', 'Código da Microrregião', 'Nome da Microrregião',
       'Código da Região Geográfica Imediata',
       'Nome da Região Geográfica Imediata',
       'Município da Região Geográfica Imediata',
       'Código da Região Geográfica Intermediária',
       'Nome da Região Geográfica Intermediária',
       'Município da Região Geográfica Intermediária',
       'Código Concentração Urbana', 'Nome Concentração Urbana',
       'Tipo Concentração Urbana', 'Código Arranjo Populacional',
       'Nome Arranjo Populacional', 'Hierarquia Urbana',
       'Hierarquia Urbana (principais categorias)', 'Código da Região Rural',
       'Nome da Região Rural',
       'Região rural (segundo classificação do núcleo)', 'Amazônia Legal',
       'Semiárido', 'Cidade-Região de São Paulo'], inplace=True)
    
    # Criando dicionário para renomear colunas
    dict = {
        'Ano'                                                                                                                                                 : 'ano',
        'Código da Grande Região'                                                                                                                             : 'codigo_grande_regiao',
        'Nome da Grande Região'                                                                                                                               : 'nome_grande_regiao',
        'Código da Unidade da Federação'                                                                                                                      : 'codigo_uf',
        'Sigla da Unidade da Federação'                                                                                                                       : 'sigla_uf',
        'Nome da Unidade da Federação'                                                                                                                        : 'nome_uf',
        'Código do Município'                                                                                                                                 : 'id_municipio',
        'Nome do Município'                                                                                                                                   : 'nome_municipio',
        'Valor adicionado bruto da Agropecuária, \na preços correntes\n(R$ 1.000)'                                                                            : 'vab_agropecuaria',
        'Valor adicionado bruto da Indústria,\na preços correntes\n(R$ 1.000)'                                                                                : 'vab_industria',
        'Valor adicionado bruto dos Serviços,\na preços correntes \n- exceto Administração, defesa, educação e saúde públicas e seguridade social\n(R$ 1.000)': 'vab_servicos',
        'Valor adicionado bruto da Administração, defesa, educação e saúde públicas e seguridade social, \na preços correntes\n(R$ 1.000)'                    : 'vab_servicos_publicos',
        'Valor adicionado bruto total, \na preços correntes\n(R$ 1.000)'                                                                                      : 'vab_total',
        'Impostos, líquidos de subsídios, sobre produtos, \na preços correntes\n(R$ 1.000)'                                                                   : 'impostos_liquidos_sobre_produtos',
        'Produto Interno Bruto, \na preços correntes\n(R$ 1.000)'                                                                                             : 'pib_corrente',
        'Produto Interno Bruto per capita, \na preços correntes\n(R$ 1,00)'                                                                                   : 'pib_per_capita_corrente',
        'Atividade com maior valor adicionado bruto'                                                                                                          : 'atividade_principal_vab',
        'Atividade com segundo maior valor adicionado bruto'                                                                                                  : 'atividade_2_vab',
        'Atividade com terceiro maior valor adicionado bruto'                                                                                                 : 'atividade_3_vab'
        }
    df.rename(columns=dict, inplace = True)
    df.sort_values(by = ['ano', 'sigla_uf', 'id_municipio'], inplace= True)
    df['curr_date'] = str(date.today())
    df['ds_owner']  = ds_owner
    for i in df.columns:
        if df[i].dtype == 'object':
            df[i] = df[i].apply(lambda x: x.strip())
    return df

# %%
# Criando função para deletar arquivos que serão armazenados temporariamente em pasta local

def delete_files(local_directory):
    for filename in os.listdir(local_directory):
        if any(extension in filename for extension in ['xls.zip', 'xlsx.zip', '.xls', '.xlsx']):
            os.remove(os.path.join(local_directory, filename))
            print(f"Arquivo {filename} removido com sucesso.")

# %%
def upload_postgresql(df):
    print("------- Conectando com o PostgreSQL ------------------")     

    # Crie um engine SQLalchemy
    engine = create_engine('postgresql://sbr_user01:sebrae541@10.12.5.101:5432/sebrae')

    try:
        # Use 'to_sql' para fazer upload do DataFrame para o PostgreSQL
        print(df)
        df.to_sql('pib_ibge', engine, if_exists='replace', index=False, schema='stg_pib_ibge')
        print("Upload concluído com sucesso!")
        
        # Verifique se os dados foram carregados corretamente
        with engine.raw_connection().cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM stg_pib_ibge.pib_ibge")
            row_count = cursor.fetchone()[0]
            print(f"A tabela agora contém {row_count} linhas.")
    except Exception as e:
        print(f"Erro: {e}")


# %% [markdown]
# # **3. RODANDO CÓDIGO**

# %%
# Interagindo com usuário do Script para coletar informações que serão necessárias para o download
while True:
    try:
        ds_owner = input('Digite seu nome e sobrenome? Ex: "Marcilio Duarte"')
        if not ds_owner:
            raise ValueError("O nome não pode estar vazio.")
        else:
            print(f'Obrigado, {ds_owner}!')
        dload_dir = input('Insira o caminho no qual os dados serão temporariamente armazenados antes de os enviarmos ao nosso banco de dados Mongodb. A título de exemplo, veja como está no computador do Marcilio: "E:\\Users\\c2705\\PROJETOS\\AUTOMATIZACAO\\PIB\\04_temp_data"\n')
        if not dload_dir:
            raise ValueError("O caminho não pode estar vazio.")
        else:
            print("\nObrigado! Os arquivos serão armazenados temporariamente neste diretório da sua máquina: "+ dload_dir+ ".\n")
        ano = input('Agora insira o ano para o qual você deseja baixar os dados do PIB. Ex: 2021')
        if not ano:
            raise ValueError("O ano não pode estar vazio.")
        else:
            print("Ok! Vamos baixar os dados de", ano + ".\n")
        # Adicione aqui o restante do seu código
        break
    except ValueError as e:
        print(f"Erro: {e}")

# %%
# Definindo objetos que iremos precisar para rodar as funções

ftp_server = 'ftp.ibge.gov.br'
directory = f'Pib_Municipios/{ano}/base'
local_directory = dload_dir
ano = ano
all_data = pd.DataFrame()

# Executando função de download
download_ftp_ibge(ftp_server=ftp_server, directory=directory, local_directory=local_directory, ano=ano)

# No diretório local de armazenamento temporário, vamos rodar um foor loop com as demais etapas:
for file in os.listdir(local_directory):
    if file == 'PIB dos Municípios - base de dados 2002-2009.xls':
        print(f"Carregando {file} em um DataFrame...")
        try:
            df = pd.read_excel(local_directory + r'\PIB dos Municípios - base de dados 2002-2009.xls')
        except Exception as e:
            print(f"Erro ao ler o arquivo {file}: {e}")
            continue
                    
        # Verificar se o DataFrame está vazio
        if df.empty:
            print(f"O DataFrame está vazio após a leitura do arquivo {file}.")
            continue
        df = tratamento_pib(df, ds_owner = ds_owner)
        print(df.head())
        # Concatenar o DataFrame ao DataFrame vazio para posteriormente concatenar o próximo
        all_data = pd.concat([all_data, df])
        print(all_data.head())
    elif file == f'PIB dos Municípios - base de dados 2010-{ano}.xlsx':
        print(f"Carregando {file} em um DataFrame...")
        try:
            df = pd.read_excel(local_directory + rf"\PIB dos Municípios - base de dados 2010-{ano}.xlsx")
        except Exception as e:
            print(f"Erro ao ler o arquivo {file}: {e}")
            continue
        
        # Verificar se o DataFrame está vazio
        if df.empty:
            print(f"O DataFrame está vazio após a leitura do arquivo {file}.")
            continue
        df = tratamento_pib(df, ds_owner = ds_owner)
        print(df.head)
        # Concatenando dados
        all_data = pd.concat([all_data, df])
        print(all_data.head)

# Deletando arquivos da pasta temporária        
delete_files(local_directory=local_directory)

# Fazendo upload dos dados no postgresql
upload_postgresql(df = all_data)