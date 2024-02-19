# **SOBRE O REPOSITÓRIO:**

Este é um repositório com projetos de automatização de processos de ETL de dados secundários que desenvolvi no meu trabalho no Sebrae Minas e que por se tratarem de dados públicos, estou compartilhando aqui como parte do meu portfolio :).

As ferramentas utilizadas foram Python, Mongodb, Pentaho e PostgreSQL (via Dbeaver). Python para fazer o download, tratamento e carregamento dos dados nas staging areas do MongoDB e do Dbeaver. E por falar em MongoDB e Dbeaver, eles foram utilizados como locais para armazenamento e disponibilização dos dados, e o Pentaho foi utilizado para criação dos Data Marts com as tabelas dimensão e fato utilizadas no consumo de BIs e etc via nosso DW.

Os arquivos ".ktr" utilizados no Pentaho foram ocultados por motivos de segurança. Porém, inseri prints das transformações e dos jobs criados para que fique mais claro como o processo foi realizado. OBS: os prints só não foram inseridos no caso das dimensões da RAIS, mas a lógica segue a mesa da aplicada na criação das dimensões do PIB.

Mais detalhes das metodologias podem ser encontrados nos arquivos "README" de cada repositório.
