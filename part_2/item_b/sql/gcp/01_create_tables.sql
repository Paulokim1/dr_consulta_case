-- Schema de staging (Silver -> Gold)
CREATE SCHEMA IF NOT EXISTS energia_stg;

-- Schema final (Gold)
CREATE SCHEMA IF NOT EXISTS energia_curated;

-- Tabela de staging
CREATE TABLE IF NOT EXISTS energia_stg.balanco_subsistema_stg (
  din_instante DATE NOT NULL,
  id_subsistema STRING NOT NULL,
  nom_subsistema STRING NOT NULL,
  geracao_hidraulica NUMERIC,
  geracao_termica   NUMERIC,
  geracao_eolica    NUMERIC,
  geracao_solar     NUMERIC,
  carga       NUMERIC NOT NULL,
  intercambio NUMERIC NOT NULL,
  dt_ingestao TIMESTAMP NOT NULL
)
PARTITION BY din_instante
CLUSTER BY id_subsistema;

-- Tabela final de consumo analítico (DW/Lakehouse)
CREATE TABLE IF NOT EXISTS energia_curated.fact_balanco_subsistema (
  dt_referencia DATE NOT NULL,
  id_subsistema STRING NOT NULL,
  nom_subsistema STRING NOT NULL,
  fonte STRING NOT NULL,

  geracao_hidraulica NUMERIC,
  geracao_termica   NUMERIC,
  geracao_eolica    NUMERIC,
  geracao_solar     NUMERIC,

  carga       NUMERIC NOT NULL,
  intercambio NUMERIC NOT NULL,

  dt_ingestao           TIMESTAMP NOT NULL,
  dt_ultima_atualizacao TIMESTAMP NOT NULL
)
PARTITION BY dt_referencia
CLUSTER BY id_subsistema;