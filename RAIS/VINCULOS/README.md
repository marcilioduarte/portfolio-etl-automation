# **O QUE É ESSE PROJETO?**

É o projeto de automatização do ETL da RAIS VÍNCULOS para os anos de 2010 em diante.

# **COMO OS DIRETÓRIOS ESTÃO ORGANIZADOS?**

1. RAIS\VINCULOS\01_doc --> Documentação do projeto. I.e: layouts originais das bases, dicionário criado e futuramente o modelo lógico do dw 

2. RAIS\VINCULOS\02_code --> Códigos em  python do processo de extração, tratamento e carregamento no MongoDB e no Dbeaver.

3. RAIS\VINCULOS\03_ktrs --> ETL da staging área para a dm final da rais no dw. Aqui estão as transformações e o job que são utilizados no Pentaho.

4. RAIS\VINCULOS\04_temp_data --> Diretório necessário para arquivos criados de forma temporária na execução do código.

5. RAIS\VINCULOS\05_sql --> Diretório com arquivos utilizados para criar schema e tbs da staging.

# **O QUE É A RAIS?**

A RAIS é uma base de dados estatística gerada a partir da declaração da RAIS (Relação Anual de Informações Sociais) instituída para auxiliar a gestão governamental do setor do trabalho com base legal nos Decreto nº 76.900, de 23 de dezembro de 1975 e nº 10.854, de 10 de novembro de 2021. A partir da coleta, o governo visa:

1. o suprimento às necessidades de controle da atividade trabalhista no País,
2. o provimento de dados para a elaboração de estatísticas do trabalho,
3. a disponibilização de informações do mercado de trabalho às entidades governamentais,
4. embasar estudos técnicos de natureza estatística e atuarial;
5. suprir outras necessidades relacionadas ao controle de registros do FGTS, dos Sistemas de Arrecadação e de Concessão e Benefícios Previdenciários, e da identificação do trabalhador com direito ao abono salarial PIS/PASEP (APENAS RAIS IDENTIFICADA).

Atualmente, duas bases de dados são disponibilizadas anualmente de forma gratuita pela internet, a RAIS Trabalhadores (também conhecida como RAIS Vínculos) e a RAIS Estabelecimentos (período de 1985 - ano atual).

A base RAIS Trabalhadores é organizada em nível do vínculo, contém todos os vínculos declarados (ativos e não ativos em 31/12). Abaixo algumas informações que podem ser obtidas a partir do uso desta base:

Número de empregados em 31 de dezembro, segundo faixa etária, escolaridade e gênero por nível geográfico, setorial e ocupacional;
Número de empregados por tamanho de estabelecimento, segundo setor de atividade econômica;
Remuneração média dos empregos em 31 de dezembro, segundo ocupação e setor de atividade econômica por nível geográfico.

A base RAIS Estabelecimentos é organizada em nível do estabelecimento. Contém tanto os estabelecimentos com vínculo declarado quanto os estabelecimentos sem vínculo informado no exercício (RAIS negativa). Abaixo informações que podem ser obtidas a partir desta base:

Freqüência (quantidade) de estabelecimentos declarantes
Informações do Estoque (quantidade de empregos em 31/12)
Nível geográfico;
Nível setorial;
Natureza jurídica;
Tamanho do estabelecimento;
Indicador de RAIS Negativa.

Fonte das informações: http://acesso.mte.gov.br/portal-pdet/o-pdet/portifolio-de-produtos/bases-de-dados.htm e http://www.rais.gov.br/sitio/sobre.jsf   

Nota sobre mudanças na RAIS 2022:
https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/rais/rais-2022/nota-tecnica-rais-2022.pdf    