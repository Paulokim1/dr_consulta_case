CREATE TABLE IF NOT EXISTS balanco_subsistema_stg (
  din_instante TEXT NOT NULL,
  id_subsistema TEXT NOT NULL,
  nom_subsistema TEXT NOT NULL,
  geracao_hidraulica REAL,
  geracao_termica REAL,
  geracao_eolica REAL,
  geracao_solar REAL,
  carga REAL NOT NULL,
  intercambio REAL NOT NULL,
  dt_ingestao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_balanco_subsistema (
  din_instante TEXT NOT NULL,
  id_subsistema TEXT NOT NULL,
  nom_subsistema TEXT NOT NULL,
  geracao_hidraulica REAL,
  geracao_termica REAL,
  geracao_eolica REAL,
  geracao_solar REAL,
  carga REAL NOT NULL,
  intercambio REAL NOT NULL,
  dt_ingestao TEXT NOT NULL,
  dt_ultima_atualizacao TEXT NOT NULL,
  
  PRIMARY KEY (din_instante, id_subsistema)
);