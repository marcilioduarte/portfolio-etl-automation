# %% [markdown]
# **O QUE ESSE CÓDIGO FAZ?**
# 
# Esse código faz o download e o carregamento da Rais Vínculos (anos de 2010 em diante) do FTP para o MongoDB.

# %% [markdown]
# # **1. IMPORTANDO LIBS** 

# %%
# Importando as bibliotecas utilizadas no script
from ftplib import FTP
import os
import py7zr
import pandas as pd
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, when, substring
import re
import requests
import json
from datetime import datetime


# %% [markdown]
# ## **2. CRIANDO FUNÇÕES** 

# %%
def load_config(file_path=r'..\config.json'):
    """
    Carrega o arquivo de configuração com os acessos ao banco de dados no script.

    Args:
        file_path (str): caminho pré-definido de onde o arquivo está armazenado
    Returns:
        config: variável com os conteúdos do json de configuração.
    """
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

# %%
def download_and_extract_files(ftp_host, ftp_path, ftp_file, ano, path_tmp):
    """
    Conecta a uma base de dados FTP, faz o download e extrai os arquivos necessários imediatamente após o download.

    Args:
        ftp_host (str): Endereço do servidor FTP.
        ftp_path (str): Caminho base no FTP.
        ftp_file (str): Subdiretório com os arquivos no FTP.
        ano (str): Ano para acessar o diretório correspondente.
        path_tmp (str): Caminho temporário local para salvar os arquivos.

    Returns:
        list: Lista de arquivos .txt extraídos.
    """
    extracted_files = []
    try:
        # Conectar ao servidor FTP
        ftp = FTP(ftp_host, timeout=1000000)
        ftp.login()
        ftp.cwd(ftp_path)
        ftp.cwd(ftp_file)
        ftp.cwd(str(ano))

        # Listar arquivos disponíveis
        files = ftp.nlst()
        print(f"Arquivos encontrados no FTP: {files}\n")

        # Processar cada arquivo individualmente
        for i in files:
            if all(exclusion not in i for exclusion in ['ESTB', 'ESTAB', 'Estb', 'Legado', 'Leia']):
                local_file_path = os.path.join(path_tmp, i)

                # Download do arquivo
                if not os.path.isfile(local_file_path):
                    print(f"Baixando o arquivo: {i}")
                    try:
                        os.makedirs(path_tmp, exist_ok=True)
                        with open(local_file_path, "wb") as file:
                            ftp.retrbinary(f"RETR {i}", file.write)
                        print(f"Download concluído: {i}\n")
                    except Exception as err:
                        print(f"Erro ao baixar o arquivo {i}: {err}")
                        continue
                else:
                    print(f"O arquivo {i} já existe localmente em: {local_file_path}")

                # Extração do arquivo
                try:
                    with py7zr.SevenZipFile(local_file_path, 'r') as archive:
                        archive.extractall(path_tmp)
                    print(f"Arquivo {i} extraído com sucesso para: {path_tmp}\n")

                    # Remover arquivo compactado
                    os.remove(local_file_path)

                    # Adicionar o arquivo extraído à lista
                    file_to_spark = i[:-3] + '.txt'
                    extracted_files.append(file_to_spark)
                    print(f"Arquivo pronto para uso: {file_to_spark}\n")
                except Exception as err:
                    print(f"Erro ao processar o arquivo {i}: {err}\n")
    except Exception as e:
        print(f"Erro ao conectar ou processar o FTP: {e}")

    return extracted_files


# %%
def process_and_load_file(file_path, ano, mongo_url_build, mongo_url_write, mongo_collection, ds_owner, spark=None):
    """
    Processa um arquivo .txt usando Spark e carrega os dados no MongoDB.

    Args:
        file_path (str): Caminho completo do arquivo .txt a ser processado.
        ano (int): Ano de referência do arquivo.
        mongo_url_build (str): URL de conexão para inicializar o spark conectado ao MongoDB.
        mongo_url_write (str): URL de conexão para carregar os dados com o spark no MongoDB.
        mongo_collection (str): Nome da coleção MongoDB onde os dados serão carregados.
        spark (SparkSession): Sessão Spark (opcional). Será criada se não for fornecida.
    """
            
    # Inicializa SparkSession se não fornecida
    if not spark:
        spark = SparkSession.builder \
            .appName("APP_RAIS_VINCULOS") \
            .config("spark.executor.memory", "8g") \
            .config("spark.executor.cores", "4") \
            .config("spark.executor.instances", "4") \
            .config("spark.driver.memory", "4g") \
            .config("spark.mongodb.input.uri", mongo_url_build) \
            .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "128") \
            .config("spark.mongodb.input.collection", mongo_collection) \
            .config("spark.network.timeout", "10000") \
            .getOrCreate()
    try:
        #Início do processamento
        process_start = datetime.now()
        print(f"\n[INFO] Início do processamento do arquivo {file_path} às {process_start}")

        # Lê o arquivo .txt
        df_rais = spark.read \
            .option("encoding", "ISO-8859-1") \
            .option("header", "true") \
            .option("sep", ";") \
            .csv(file_path)

        # Lista de colunas com nomes formatados
        rais_columns_list = [c.lower() for c in df_rais.schema.names]
        campos_formatados = [
            re.sub(r'[^\w]', '_', ''.join(char for char in unicodedata.normalize('NFKD', c) if not unicodedata.combining(char)).lower())
            .replace('__', '_').strip('_')
            for c in rais_columns_list
        ]

        # Renomeia colunas para nomes formatados
        for old, new in zip(df_rais.columns, campos_formatados):
            df_rais = df_rais.withColumnRenamed(old, new)

        # Adiciona e converte colunas
        df_rais = df_rais.withColumn("codigo_uf", substring(col("municipio"), 1, 2))
        # Mapeamento de códigos para siglas das UFs
        uf_mapping = {
            "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA",
            "16": "AP", "17": "TO", "21": "MA", "22": "PI", "23": "CE",
            "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE",
            "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
            "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT",
            "52": "GO", "53": "DF", "99": "NI"
        }

        # Adicionando a coluna com os dois primeiros dígitos do campo `municipio`
        df_rais = df_rais.withColumn("codigo_uf", substring(col("municipio"), 1, 2))

        # Convertendo o código da UF para a sigla correspondente usando múltiplos `when`
        df_rais = df_rais.withColumn(
            "uf",
            when(col("codigo_uf") == "11", "RO")
            .when(col("codigo_uf") == "12", "AC")
            .when(col("codigo_uf") == "13", "AM")
            .when(col("codigo_uf") == "14", "RR")
            .when(col("codigo_uf") == "15", "PA")
            .when(col("codigo_uf") == "16", "AP")
            .when(col("codigo_uf") == "17", "TO")
            .when(col("codigo_uf") == "21", "MA")
            .when(col("codigo_uf") == "22", "PI")
            .when(col("codigo_uf") == "23", "CE")
            .when(col("codigo_uf") == "24", "RN")
            .when(col("codigo_uf") == "25", "PB")
            .when(col("codigo_uf") == "26", "PE")
            .when(col("codigo_uf") == "27", "AL")
            .when(col("codigo_uf") == "28", "SE")
            .when(col("codigo_uf") == "29", "BA")
            .when(col("codigo_uf") == "31", "MG")
            .when(col("codigo_uf") == "32", "ES")
            .when(col("codigo_uf") == "33", "RJ")
            .when(col("codigo_uf") == "35", "SP")
            .when(col("codigo_uf") == "41", "PR")
            .when(col("codigo_uf") == "42", "SC")
            .when(col("codigo_uf") == "43", "RS")
            .when(col("codigo_uf") == "50", "MS")
            .when(col("codigo_uf") == "51", "MT")
            .when(col("codigo_uf") == "52", "GO")
            .when(col("codigo_uf") == "53", "DF")
            .otherwise("NI")  # Para tratar casos onde o código não é encontrado
        )

        # Remover a coluna intermediária 'codigo_uf', se não for mais necessária
        df_rais = df_rais.drop("codigo_uf")


        # Tratamento baseado no ano
        if ano < 2015:
                    print("menor do que 2015")
                    df_rais = df_rais.withColumn('bairros_sp', f.translate('bairros_sp','{ñ c','-1').cast(IntegerType()))\
                        .withColumn('bairros_fortaleza', f.translate('bairros_fortaleza','{ñ c','-1').cast(IntegerType()))\
                        .withColumn('bairros_rj', f.translate('bairros_rj','{ñ c','-1').cast(IntegerType()))\
                        .withColumn('distritos_sp', f.translate('distritos_sp','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('vl_remun_dezembro_nom', f.regexp_replace('vl_remun_dezembro_nom',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_dezembro_sm', f.regexp_replace('vl_remun_dezembro_sm',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_media_nom', f.regexp_replace('vl_remun_media_nom',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_media_sm', f.regexp_replace('vl_remun_media_sm',',','.').cast(DoubleType()))\
                        .withColumn('tempo_emprego', f.regexp_replace('tempo_emprego',',','.'))\
                        .withColumn('vl_rem_janeiro_cc', lit(0).cast(DoubleType()))\
                        .withColumn('vl_rem_fevereiro_cc', lit(0).cast(DoubleType()))\
                        .withColumn('vl_rem_marco_cc', lit(0).cast(DoubleType()))\
                        .withColumn('vl_rem_abril_cc', lit(0).cast(DoubleType()))\
                        .withColumn('vl_rem_abril_cc', lit(0).cast(DoubleType()))\
                        .withColumn('vl_rem_maio_cc', lit(0).cast(DoubleType()))\
                        .withColumn('vl_rem_junho_cc', lit(0).cast(DoubleType()))\
                        .withColumn('vl_rem_julho_cc', lit(0).cast(DoubleType()))\
                        .withColumn('vl_rem_agosto_cc', lit(0).cast(DoubleType()))\
                        .withColumn('vl_rem_setembro_cc', lit(0).cast(DoubleType()))\
                        .withColumn('vl_rem_outubro_cc', lit(0).cast(DoubleType()))\
                        .withColumn('vl_rem_novembro_cc', lit(0).cast(DoubleType()))\
                        .withColumn('vinculo_ativo_31_12', col('vinculo_ativo_31_12').cast(IntegerType()))\
                        .withColumn('qtd_hora_contr', col('qtd_hora_contr').cast(IntegerType()))\
                        .withColumn('idade', col('idade').cast(IntegerType()))\
                        .withColumn('mes_admissao', col('mes_admissao').cast(IntegerType()))\
                        .withColumn('mes_desligamento', col('mes_desligamento').cast(IntegerType()))\
                        .withColumn('qtd_dias_afastamento', col('qtd_dias_afastamento').cast(IntegerType()))\
                        .withColumn('ano_chegada_brasil', lit('1900'))\
                        .withColumn('ind_trab_parcial', lit(-1))\
                        .withColumn('ind_trab_intermitente', lit(-1))\
                        .withColumn('tipo_salario', lit(-1))\
                        .withColumn('ibge_subsetor', lit(-1))\
                        .withColumn('vl_salario_contratual', lit(0).cast(DoubleType()))\
                        .withColumn('ano', f.lit(ano).cast(IntegerType()))\
                        .withColumn("curr_date",to_date(date_format(current_timestamp(), 'yyyy-MM-dd')))\
                        .withColumn("ds_owner",lit(ds_owner))\
                        .withColumn('nacionalidade', f.translate('nacionalidade','{ñ','-1').cast(IntegerType()))
        elif ano == 2015:
            print("menor do que 2016")
            df_rais = df_rais.withColumn('bairros_sp', f.translate('bairros_sp','{ñ c','-1').cast(IntegerType()))\
                            .withColumn('bairros_fortaleza', f.translate('bairros_fortaleza','{ñ c','-1').cast(IntegerType()))\
                            .withColumn('bairros_rj', f.translate('bairros_rj','{ñ c','-1').cast(IntegerType()))\
                            .withColumn('distritos_sp', f.translate('distritos_sp','{ñ class}','-1').cast(IntegerType()))\
                            .withColumn('vl_remun_dezembro_nom', f.regexp_replace('vl_remun_dezembro_nom',',','.').cast(DoubleType()))\
                            .withColumn('vl_remun_dezembro_sm', f.regexp_replace('vl_remun_dezembro_sm',',','.').cast(DoubleType()))\
                            .withColumn('vl_remun_media_nom', f.regexp_replace('vl_remun_media_nom',',','.').cast(DoubleType()))\
                            .withColumn('vl_remun_media_sm', f.regexp_replace('vl_remun_media_sm',',','.').cast(DoubleType()))\
                            .withColumn('tempo_emprego', f.regexp_replace('tempo_emprego',',','.'))\
                            .withColumn('vl_rem_janeiro_cc', f.regexp_replace('vl_rem_janeiro_cc',',','.').cast(DoubleType()))\
                            .withColumn('vl_rem_fevereiro_cc', f.regexp_replace('vl_rem_fevereiro_cc',',','.').cast(DoubleType()))\
                            .withColumn('vl_rem_marco_cc', f.regexp_replace('vl_rem_marco_cc',',','.').cast(DoubleType()))\
                            .withColumn('vl_rem_abril_cc', f.regexp_replace('vl_rem_abril_cc',',','.').cast(DoubleType()))\
                            .withColumn('vl_rem_abril_cc', f.regexp_replace('vl_rem_abril_cc',',','.').cast(DoubleType()))\
                            .withColumn('vl_rem_maio_cc', f.regexp_replace('vl_rem_maio_cc',',','.').cast(DoubleType()))\
                            .withColumn('vl_rem_junho_cc', f.regexp_replace('vl_rem_junho_cc',',','.').cast(DoubleType()))\
                            .withColumn('vl_rem_julho_cc', f.regexp_replace('vl_rem_julho_cc',',','.').cast(DoubleType()))\
                            .withColumn('vl_rem_agosto_cc', f.regexp_replace('vl_rem_agosto_cc',',','.').cast(DoubleType()))\
                            .withColumn('vl_rem_setembro_cc', f.regexp_replace('vl_rem_setembro_cc',',','.').cast(DoubleType()))\
                            .withColumn('vl_rem_outubro_cc', f.regexp_replace('vl_rem_outubro_cc',',','.').cast(DoubleType()))\
                            .withColumn('vl_rem_novembro_cc', f.regexp_replace('vl_rem_novembro_cc',',','.').cast(DoubleType()))\
                            .withColumn('vinculo_ativo_31_12', col('vinculo_ativo_31_12').cast(IntegerType()))\
                            .withColumn('qtd_hora_contr', col('qtd_hora_contr').cast(IntegerType()))\
                            .withColumn('idade', col('idade').cast(IntegerType()))\
                            .withColumn('mes_admissao', col('mes_admissao').cast(IntegerType()))\
                            .withColumn('mes_desligamento', col('mes_desligamento').cast(IntegerType()))\
                            .withColumn('qtd_dias_afastamento', col('qtd_dias_afastamento').cast(IntegerType()))\
                            .withColumn('ano_chegada_brasil', lit('1900'))\
                            .withColumn('ind_trab_parcial', lit(-1))\
                            .withColumn('ind_trab_intermitente', lit(-1))\
                            .withColumn('tipo_salario', lit(-1))\
                            .withColumn('vl_salario_contratual', lit(0).cast(DoubleType()))\
                            .withColumn('ano', f.lit(ano).cast(IntegerType()))\
                            .withColumn("curr_date",to_date(date_format(current_timestamp(), 'yyyy-MM-dd')))\
                            .withColumn("ds_owner",lit(ds_owner))\
                            .withColumn('nacionalidade', f.translate('nacionalidade','{ñ','-1').cast(IntegerType()))    
        elif ano >= 2016 and ano < 2017:
            print("maior ou igual 2016 e menor do que  2017")
            df_rais = df_rais.withColumn('bairros_sp', f.translate('bairros_sp','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('bairros_fortaleza', f.translate('bairros_fortaleza','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('bairros_rj', f.translate('bairros_rj','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('distritos_sp', f.translate('distritos_sp','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('vl_remun_dezembro_nom', f.regexp_replace('vl_remun_dezembro_nom',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_dezembro_sm', f.regexp_replace('vl_remun_dezembro_sm',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_media_nom', f.regexp_replace('vl_remun_media_nom',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_media_sm', f.regexp_replace('vl_remun_media_sm',',','.').cast(DoubleType()))\
                        .withColumn('tempo_emprego', f.regexp_replace('tempo_emprego',',','.'))\
                        .withColumn('vl_rem_janeiro_cc', f.regexp_replace('vl_rem_janeiro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_fevereiro_cc', f.regexp_replace('vl_rem_fevereiro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_marco_cc', f.regexp_replace('vl_rem_marco_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_abril_cc', f.regexp_replace('vl_rem_abril_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_maio_cc', f.regexp_replace('vl_rem_maio_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_junho_cc', f.regexp_replace('vl_rem_junho_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_julho_cc', f.regexp_replace('vl_rem_julho_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_agosto_cc', f.regexp_replace('vl_rem_agosto_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_setembro_cc', f.regexp_replace('vl_rem_setembro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_outubro_cc', f.regexp_replace('vl_rem_outubro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_novembro_cc', f.regexp_replace('vl_rem_novembro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vinculo_ativo_31_12', col('vinculo_ativo_31_12').cast(IntegerType()))\
                        .withColumn('qtd_hora_contr', col('qtd_hora_contr').cast(IntegerType()))\
                        .withColumn('idade', col('idade').cast(IntegerType()))\
                        .withColumn('mes_admissao', col('mes_admissao').cast(IntegerType()))\
                        .withColumn('mes_desligamento', col('mes_desligamento').cast(IntegerType()))\
                        .withColumn('qtd_dias_afastamento', col('qtd_dias_afastamento').cast(IntegerType()))\
                        .withColumn('ind_trab_parcial', lit(-1))\
                        .withColumn('ind_trab_intermitente', lit(-1))\
                        .withColumn('tipo_salario', lit(-1))\
                        .withColumn('vl_salario_contratual', lit(0).cast(DoubleType()))\
                        .withColumn('ano', f.lit(ano).cast(IntegerType()))\
                        .withColumn("curr_date",to_date(date_format(current_timestamp(), 'yyyy-MM-dd')))\
                        .withColumn("ds_owner",lit(ds_owner))\
                        .withColumn('nacionalidade', f.translate('nacionalidade','{ñ','-1').cast(IntegerType()))
        elif ano == 2017:
            print("ano igual 2017")
            df_rais = df_rais.withColumn('bairros_sp', f.translate('bairros_sp','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('bairros_fortaleza', f.translate('bairros_fortaleza','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('bairros_rj', f.translate('bairros_rj','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('distritos_sp', f.translate('distritos_sp','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('vl_remun_dezembro_nom', f.regexp_replace('vl_remun_dezembro_nom',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_dezembro_sm', f.regexp_replace('vl_remun_dezembro_sm',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_media_nom', f.regexp_replace('vl_remun_media_nom',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_media_sm', f.regexp_replace('vl_remun_media_sm',',','.').cast(DoubleType()))\
                        .withColumn('tempo_emprego', f.regexp_replace('tempo_emprego',',','.'))\
                        .withColumn('vl_rem_janeiro_cc', f.regexp_replace('vl_rem_janeiro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_fevereiro_cc', f.regexp_replace('vl_rem_fevereiro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_marco_cc', f.regexp_replace('vl_rem_marco_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_abril_cc', f.regexp_replace('vl_rem_abril_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_maio_cc', f.regexp_replace('vl_rem_maio_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_junho_cc', f.regexp_replace('vl_rem_junho_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_julho_cc', f.regexp_replace('vl_rem_julho_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_agosto_cc', f.regexp_replace('vl_rem_agosto_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_setembro_cc', f.regexp_replace('vl_rem_setembro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_outubro_cc', f.regexp_replace('vl_rem_outubro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_novembro_cc', f.regexp_replace('vl_rem_novembro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vinculo_ativo_31_12', col('vinculo_ativo_31_12').cast(IntegerType()))\
                        .withColumn('qtd_hora_contr', col('qtd_hora_contr').cast(IntegerType()))\
                        .withColumn('idade', col('idade').cast(IntegerType()))\
                        .withColumn('mes_admissao', col('mes_admissao').cast(IntegerType()))\
                        .withColumn('mes_desligamento', col('mes_desligamento').cast(IntegerType()))\
                        .withColumn('qtd_dias_afastamento', col('qtd_dias_afastamento').cast(IntegerType()))\
                        .withColumn('tipo_salario', lit(-1))\
                        .withColumn('vl_salario_contratual', lit(0).cast(DoubleType()))\
                        .withColumn('ano', f.lit(ano).cast(IntegerType()))\
                        .withColumn("curr_date",to_date(date_format(current_timestamp(), 'yyyy-MM-dd')))\
                        .withColumn("ds_owner",lit(ds_owner))\
                        .withColumn('nacionalidade', f.translate('nacionalidade','{ñ','-1').cast(IntegerType()))
        elif ano == 2018:
            print("igual 2018")
            df_rais = df_rais.withColumn('bairros_sp', f.translate('bairros_sp','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('bairros_fortaleza', f.translate('bairros_fortaleza','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('bairros_rj', f.translate('bairros_rj','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('distritos_sp', f.translate('distritos_sp','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('vl_remun_dezembro_nom', f.regexp_replace('vl_remun_dezembro_nom',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_dezembro_sm', f.regexp_replace('vl_remun_dezembro_sm',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_media_nom', f.regexp_replace('vl_remun_media_nom',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_media_sm', f.regexp_replace('vl_remun_media_sm',',','.').cast(DoubleType()))\
                        .withColumn('tempo_emprego', f.regexp_replace('tempo_emprego',',','.'))\
                        .withColumn('vl_rem_janeiro_cc', f.regexp_replace('vl_rem_janeiro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_fevereiro_cc', f.regexp_replace('vl_rem_fevereiro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_marco_cc', f.regexp_replace('vl_rem_marco_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_abril_cc', f.regexp_replace('vl_rem_abril_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_maio_cc', f.regexp_replace('vl_rem_maio_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_junho_cc', f.regexp_replace('vl_rem_junho_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_julho_cc', f.regexp_replace('vl_rem_julho_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_agosto_cc', f.regexp_replace('vl_rem_agosto_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_setembro_cc', f.regexp_replace('vl_rem_setembro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_outubro_cc', f.regexp_replace('vl_rem_outubro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_novembro_cc', f.regexp_replace('vl_rem_novembro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_salario_contratual', f.regexp_replace('vl_salario_contratual',',','.').cast(DoubleType()))\
                        .withColumn('vinculo_ativo_31_12', col('vinculo_ativo_31_12').cast(IntegerType()))\
                        .withColumn('qtd_hora_contr', col('qtd_hora_contr').cast(IntegerType()))\
                        .withColumn('idade', col('idade').cast(IntegerType()))\
                        .withColumn('mes_admissao', col('mes_admissao').cast(IntegerType()))\
                        .withColumn('mes_desligamento', col('mes_desligamento').cast(IntegerType()))\
                        .withColumn('qtd_dias_afastamento', col('qtd_dias_afastamento').cast(IntegerType()))\
                        .withColumn('ano', f.lit(ano).cast(IntegerType()))\
                        .withColumn("curr_date",to_date(date_format(current_timestamp(), 'yyyy-MM-dd')))\
                        .withColumn("ds_owner",lit(ds_owner))\
                        .withColumn('nacionalidade', f.translate('nacionalidade','{ñ','-1').cast(IntegerType()))
        elif ano == 2019:
            print("igual 2019")
            df_rais = df_rais.withColumn('bairros_sp', f.translate('bairros_sp','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('bairros_fortaleza', f.translate('bairros_fortaleza','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('bairros_rj', f.translate('bairros_rj','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('distritos_sp', f.translate('distritos_sp','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('vl_remun_dezembro_nom', f.regexp_replace('vl_remun_dezembro_nom',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_dezembro_sm', f.regexp_replace('vl_remun_dezembro_sm',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_media_nom', f.regexp_replace('vl_remun_media_nom',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_media_sm', f.regexp_replace('vl_remun_media_sm',',','.').cast(DoubleType()))\
                        .withColumn('tempo_emprego', f.regexp_replace('tempo_emprego',',','.'))\
                        .withColumn('vl_rem_janeiro_cc', f.regexp_replace('vl_rem_janeiro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_fevereiro_cc', f.regexp_replace('vl_rem_fevereiro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_marco_cc', f.regexp_replace('vl_rem_marco_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_abril_cc', f.regexp_replace('vl_rem_abril_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_maio_cc', f.regexp_replace('vl_rem_maio_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_junho_cc', f.regexp_replace('vl_rem_junho_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_julho_cc', f.regexp_replace('vl_rem_julho_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_agosto_cc', f.regexp_replace('vl_rem_agosto_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_setembro_cc', f.regexp_replace('vl_rem_setembro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_outubro_cc', f.regexp_replace('vl_rem_outubro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_novembro_cc', f.regexp_replace('vl_rem_novembro_cc',',','.').cast(DoubleType()))\
                        .withColumn('vinculo_ativo_31_12', col('vinculo_ativo_31_12').cast(IntegerType()))\
                        .withColumn('qtd_hora_contr', col('qtd_hora_contr').cast(IntegerType()))\
                        .withColumn('idade', col('idade').cast(IntegerType()))\
                        .withColumn('mes_admissao', col('mes_admissao').cast(IntegerType()))\
                        .withColumn('mes_desligamento', col('mes_desligamento').cast(IntegerType()))\
                        .withColumn('qtd_dias_afastamento', col('qtd_dias_afastamento').cast(IntegerType()))\
                        .withColumn('vl_salario_contratual', lit(0).cast(DoubleType()))\
                        .withColumn('tipo_salario', lit(-1))\
                        .withColumn('ano', f.lit(ano).cast(IntegerType()))\
                        .withColumn("curr_date",to_date(date_format(current_timestamp(), 'yyyy-MM-dd')))\
                        .withColumn("ds_owner",lit(ds_owner))\
                        .withColumn('nacionalidade', f.translate('nacionalidade','{ñ','-1').cast(IntegerType()))
        elif  ano >= 2020:
            print("maior ou igual 2020")
            df_rais = df_rais.withColumn('bairros_sp', f.translate('bairros_sp','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('bairros_fortaleza', f.translate('bairros_fortaleza','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('bairros_rj', f.translate('bairros_rj','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('distritos_sp', f.translate('distritos_sp','{ñ class}','-1').cast(IntegerType()))\
                        .withColumn('vl_remun_dezembro_nom', f.regexp_replace('vl_remun_dezembro_nom',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_dezembro_sm', f.regexp_replace('vl_remun_dezembro_sm',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_media_nom', f.regexp_replace('vl_remun_media_nom',',','.').cast(DoubleType()))\
                        .withColumn('vl_remun_media_sm', f.regexp_replace('vl_remun_media_sm',',','.').cast(DoubleType()))\
                        .withColumn('tempo_emprego', f.regexp_replace('tempo_emprego',',','.'))\
                        .withColumn('vl_rem_janeiro_cc', f.regexp_replace('vl_rem_janeiro_sc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_fevereiro_cc', f.regexp_replace('vl_rem_fevereiro_sc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_marco_cc', f.regexp_replace('vl_rem_marco_sc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_abril_cc', f.regexp_replace('vl_rem_abril_sc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_maio_cc', f.regexp_replace('vl_rem_maio_sc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_junho_cc', f.regexp_replace('vl_rem_junho_sc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_julho_cc', f.regexp_replace('vl_rem_julho_sc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_agosto_cc', f.regexp_replace('vl_rem_agosto_sc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_setembro_cc', f.regexp_replace('vl_rem_setembro_sc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_outubro_cc', f.regexp_replace('vl_rem_outubro_sc',',','.').cast(DoubleType()))\
                        .withColumn('vl_rem_novembro_cc', f.regexp_replace('vl_rem_novembro_sc',',','.').cast(DoubleType()))\
                        .withColumn('vinculo_ativo_31_12', col('vinculo_ativo_31_12').cast(IntegerType()))\
                        .withColumn('qtd_hora_contr', col('qtd_hora_contr').cast(IntegerType()))\
                        .withColumn('idade', col('idade').cast(IntegerType()))\
                        .withColumn('mes_admissao', col('mes_admissao').cast(IntegerType()))\
                        .withColumn('mes_desligamento', col('mes_desligamento').cast(IntegerType()))\
                        .withColumn('qtd_dias_afastamento', col('qtd_dias_afastamento').cast(IntegerType()))\
                        .withColumn('tipo_salario', lit(-1))\
                        .withColumn('vl_salario_contratual', lit(0).cast(DoubleType()))\
                        .withColumn('ano', f.lit(ano).cast(IntegerType()))\
                        .withColumn("curr_date",to_date(date_format(current_timestamp(), 'yyyy-MM-dd')))\
                        .withColumn("ds_owner",lit(ds_owner))\
                        .withColumn('nacionalidade', f.translate('nacionalidade','{ñ','-1').cast(IntegerType()))
        else:
            print("Sem arquivos")

        # Exibe o esquema do DataFrame após transformação
        df_rais.printSchema()

        # Fim do processamento
        process_end = datetime.now()
        print(f"[INFO] Fim do processamento do arquivo {file_path} às {process_end}")
        print(f"[INFO] Tempo total de processamento: {process_end - process_start}")

        # Carrega os dados no MongoDB
        print(f"Iniciando o carregamento do arquivo {file_path} no MongoDB...")
        start_time = datetime.now()
        df_rais.write.format("mongo") \
            .mode("append") \
            .option("uri", mongo_url_write) \
            .option("database", "SEBRAE") \
            .option("collection", mongo_collection).save()
        end_time = datetime.now() 

        print(f"Carregamento concluído para o arquivo {file_path}. Tempo total: {end_time - start_time}")

    except Exception as e:
        print(f"[ERROR] Erro ao processar o arquivo {file_path}: {e}")
        
    finally:
        # Fechando a sessao spark
        print("Fechando a sessão do Spark")
        spark.stop() 


# %%
def cleanup_files(file_to_spark_list, path_tmp):
    """
    Apaga os arquivos .txt processados.

    Args:
        file_to_spark_list (list): Lista de arquivos .txt processados.
        path_tmp (str): Caminho temporário onde os arquivos estão localizados.
    """
    print("\n[INFO] Iniciando a limpeza dos arquivos .txt...")

    for file_to_spark in file_to_spark_list:
        try:
            file_path = os.path.join(path_tmp, file_to_spark)
            if os.path.isfile(file_path):  # Verifica se o arquivo existe
                os.remove(file_path)
                print(f"[INFO] Arquivo {file_to_spark} apagado com sucesso.")
            else:
                print(f"[WARNING] Arquivo {file_to_spark} não encontrado para remoção.")
        except Exception as err:
            print(f"[ERROR] Erro ao remover o arquivo {file_to_spark}: {err}")


# %% [markdown]
# # **3. EXECUTANDO** 

# %%
inicio_geral = datetime.now()
anos = []
while True:
    try:
        ano_inicial = int(input('Digite o ANO INICIAL para o qual você deseja fazer o DOWNLOAD DO FTP e a CARGA INICIAL DA RAIS NO MONGODB: '))
        ano_final = int(input('Digite o ÚLTIMO ANO para o qual você deseja fazer o DOWNLOAD DO FTP e a CARGA INICIAL DA RAIS NO MONGODB: '))
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
# INICIANDO A SESSAO SPARK
config = load_config()
MONGO_URL_BUILD = f"mongodb://{config['mongodb']['user_name']}:{config['mongodb']['password']}@{config['mongodb']['host']}:{config['mongodb']['port']}/{config['mongodb']['db_name']}"
MONGO_URL_WRITE = f"mongodb://{config['mongodb']['user_name']}:{config['mongodb']['password']}@{config['mongodb']['host']}:{config['mongodb']['port']}/{config['mongodb']['db_name']}?serverSelectionTimeoutMS=5000&connectTimeoutMS=300000"
COLLECTION ="RAIS_VINCULOS"
# pastas e diretorios

path_tmp=r'..\04_temp_data'
ftp_host = "ftp.mtps.gov.br"
ftp_path = r"pdet/microdados"
ftp_file = 'RAIS'
ftp_user = "anonymous"
ftp_pass = ""

for ano in anos:
    files_to_spark = download_and_extract_files(ftp_host, ftp_path, ftp_file, ano, path_tmp)

    for file_to_spark in files_to_spark:

        process_and_load_file(
            file_path=os.path.join(path_tmp, file_to_spark),
            ano=int(ano),
            mongo_url_build=MONGO_URL_BUILD,
            mongo_url_write=MONGO_URL_WRITE,
            mongo_collection=f"{COLLECTION}_{ano}",
            ds_owner="MARCILIO DUARTE"
        )

        cleanup_files(files_to_spark, path_tmp)



