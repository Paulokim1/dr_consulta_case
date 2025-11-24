# Dr. Consulta - Case Challenge

## Pipeline de Ingestão

Fonte de dados utilizada: https://dados.ons.org.br/dataset/balanco-energia-subsistema

Dicionário de dados: https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/balanco_energia_subsistema_ho/DicionarioDados_Balanco_Energia_Subsistema.pdf 

1. Script Python: Responsável pela leitura da fonte, validação de schema e preparação dos dados.
    * [pipeline](src/pipeline.py)

2. Códigos SQL
    * 2.1 Criação de tabelas
        * [GCP](sql/gcp/01_create_tables.sql)
        * [Local](sql/local/01_create_tables.sql)
    * 2.2 Inserção / Atualização
        * [GCP](sql/gcp/02_upsert_fact_energia.sql)
        * [Local](sql/local/02_upsert_fact_energia.sql)

### Execução
1. Pipeline Local:
    Execute o [notebook](main.ipynb) para realizar a extração, normalização e geração dos arquivos Parquet.
2. Carga no Banco de dados (DW):
    Utilize o [notebook load_gold](load_gold.ipynb)￼para carregar os dados tratados no banco analítico final, simulando o processo de ingestão para a camada final de consumo.
