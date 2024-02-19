-- CRIANDO SCHEMA E TB
-- O nome do usuário foi ocultado por motivos de segurança 

CREATE SCHEMA stg_pib_ibge AUTHORIZATION USUARIO;

CREATE TABLE stg_pib_ibge.pib_ibge (
	ano int8 NULL,
	codigo_grande_regiao int8 NULL,
	nome_grande_regiao text NULL,
	codigo_uf int8 NULL,
	sigla_uf text NULL,
	nome_uf text NULL,
	id_municipio int8 NULL,
	nome_municipio text NULL,
	vab_agropecuaria float8 NULL,
	vab_industria float8 NULL,
	vab_servicos float8 NULL,
	vab_servicos_publicos float8 NULL,
	vab_total float8 NULL,
	impostos_liquidos_sobre_produtos float8 NULL,
	pib_corrente float8 NULL,
	pib_per_capita_corrente float8 NULL,
	curr_date text NULL,
	ds_owner text NULL,
	atividade_principal_vab text NULL,
	atividade_2_vab text NULL,
	atividade_3_vab text NULL
);