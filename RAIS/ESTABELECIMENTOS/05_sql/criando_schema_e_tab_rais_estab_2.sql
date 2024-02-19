-- Criar o schema
CREATE SCHEMA IF NOT EXISTS stg_rais;

-- Criar a tabela rais_estab_pub
CREATE TABLE stg_rais.tb_rais_estab_pub (
    ano int2,
    sigla_uf int2,
    id_municipio int4,
    qtd_vinc_ativos int4,
    qtd_vinc_clt int4,
    qtd_vinc_estat int4,
    nat_jur int2,
    tam_estab int2,
    tipo_estab int2,
    ind_ativ_ano int2,
    ind_cei_vinculado int2,
    ind_pat int2,
    ind_rais_negativa int2,
    ind_simples int2,
    cnae_1_classe int4,
    cnae_2_classe int4,
    cnae_2_subclasse int4,
    subsetor_ibge int4,
    cep int4,
    bairro_sp int2,
    bairro_fortaleza int2,
    bairro_rj int2,
    distrito_sp int2,
    regiao_adm_df int2,
    padrao_sebrae int2,
    porte_sebrae varchar(20),
    setor_sebrae varchar(50),
    setor_ibge varchar(1000),
    ds_owner varchar(50),
    curr_date varchar(20)
) PARTITION BY RANGE (ano);

-- Criar as partições
CREATE TABLE stg_rais.tb_rais_estab_pub_2010_2012 PARTITION OF stg_rais.tb_rais_estab_pub FOR VALUES FROM (2010) TO (2013);
CREATE TABLE stg_rais.tb_rais_estab_pub_2013_2015 PARTITION OF stg_rais.tb_rais_estab_pub FOR values FROM (2013) TO (2016);
CREATE TABLE stg_rais.tb_rais_estab_pub_2016_2018 PARTITION OF stg_rais.tb_rais_estab_pub FOR VALUES FROM (2016) TO (2019);
CREATE TABLE stg_rais.tb_rais_estab_pub_2019_2021 PARTITION OF stg_rais.tb_rais_estab_pub FOR VALUES FROM (2019) TO (2022);
CREATE TABLE stg_rais.tb_rais_estab_pub_2022_2024 PARTITION OF stg_rais.tb_rais_estab_pub FOR VALUES FROM (2022) TO (2025);
CREATE TABLE stg_rais.tb_rais_estab_pub_2025_2027 PARTITION OF stg_rais.tb_rais_estab_pub FOR VALUES FROM (2025) TO (2028);