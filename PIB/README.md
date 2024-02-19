# **O QUE É ESSE PROJETO?**

É o projeto de automatização do ETL do PIB para os anos de 2002 em diante.

# **COMO OS DIRETÓRIOS ESTÃO ORGANIZADOS?**
 
1. PIB\01_doc --> Documentação do projeto. I.e: layouts originais das bases, dicionário criado e futuramente o modelo lógico do dw (no que tange ao PIB) 

2. PIB\02_code --> Códigos em python do processo de extração, tratamento e carregamento no PostgreSQL(Dbeaver). Disponíveis em notebook e .py.

3. PIB\03_ktrs --> ETL da staging área para a dm final do PIB no dw. Aqui estão as transformações e o job que são utilizados no Pentaho.

4. PIB\04_temp_data --> diretório necessário para arquivos criados de forma temporária na execução do código.

5. PIB\05_sql  --> diretório com arquivos sql utilizados para criar schema e tabela da staging area.

# **O QUE O CÓDIGO FAZ?**

O download, tratamento e o carregamento dos dados no Dbeaver.

# **O QUE É O PIB?**

O PIB é a soma de todos os bens e serviços finais produzidos por um país, estado ou cidade, geralmente em um ano. Todos os países calculam o seu PIB nas suas respectivas moedas.

O PIB mede apenas os bens e serviços finais para evitar dupla contagem. Se um país produz R$ 100 de trigo, R$ 200 de farinha de trigo e R$ 300 de pão, por exemplo, seu PIB será de R$ 300, pois os valores da farinha e do trigo já estão embutidos no valor do pão.

Os bens e serviços finais que compõem o PIB são medidos no preço em que chegam ao consumidor. Dessa forma, levam em consideração também os impostos sobre os produtos comercializados.

O PIB não é o total da riqueza existente em um país. Esse é um equívoco muito comum, pois dá a sensação de que o PIB seria um estoque de valor que existe na economia, como uma espécie de tesouro nacional.

Na realidade, o PIB é um indicador de fluxo de novos bens e serviços finais produzidos durante um período. Se um país não produzir nada em um ano, o seu PIB será nulo.

Portanto, O PIB é apenas um indicador síntese de uma economia. Ele ajuda a compreender um país, mas não expressa importantes fatores, como distribuição de renda, qualidade de vida, educação e saúde. Um país tanto pode ter um PIB pequeno e ostentar um altíssimo padrão de vida, como registrar um PIB alto e apresentar um padrão de vida relativamente baixo.

# **COMO ELE É MENSURADO PELO IBGE?**

Para o cálculo do PIB, são utilizados diversos dados; alguns produzidos pelo IBGE, outros provenientes de fontes externas. Essas são algumas das peças que compõem o quebra-cabeça do PIB:

* Balanço de Pagamentos (Banco Central)
* Declaração de Informações Econômico-Fiscais da Pessoa Jurídica - DIPJ (Secretaria da Receita Federal)
* Índice de Preços ao Produtor Amplo - IPA (FGV)
* Índice Nacional de Preços ao Consumidor Amplo - IPCA (IBGE)
* Produção Agrícola Municipal - PAM - (IBGE)
* Pesquisa Anual de Comércio - PAC (IBGE)
* Pesquisa Anual de Serviços - PAS (IBGE)
* Pesquisa de Orçamentos Familiares - POF (IBGE)
* Pesquisa Industrial Anual - Empresa - PIA-Empresa (IBGE)
* Pesquisa Industrial Mensal - Produção Física - PIM-PF (IBGE)
* Pesquisa Mensal de Comércio - PMC (IBGE)
* Pesquisa Mensal de Serviços - PMS (IBGE)

# **ANÁLISES QUE PODEM SER FEITAS A PARTIR DO PIB**
A partir da performance do PIB, pode-se fazer várias análises, tais como:

* Traçar a evolução do PIB no tempo, comparando seu desempenho ano a ano;
* Fazer comparações internacionais sobre o tamanho das economias dos diversos países;
* Analisar o PIB per capita (divisão do PIB pelo número de habitantes), que mede quanto do PIB caberia a cada indivíduo de um país se todos recebessem partes iguais, entre outros estudos.

# ***Fonte das informações:*** https://www.ibge.gov.br/explica/pib.php