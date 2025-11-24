MERGE energia_curated.fact_balanco_subsistema AS tgt
USING energia_stg.balanco_subsistema_stg AS src
ON
  tgt.din_instante = src.din_instante
  AND tgt.id_subsistema = src.id_subsistema

-- Caso o registro vindo do staging exista na tabela de consumo, é porque houve uma atualização nos dados.
-- Então, atualizamos os campos necessários.
WHEN MATCHED THEN
  UPDATE SET
    tgt.nom_subsistema        = src.nom_subsistema,
    tgt.geracao_hidraulica    = src.geracao_hidraulica,
    tgt.geracao_termica       = src.geracao_termica,
    tgt.geracao_eolica        = src.geracao_eolica,
    tgt.geracao_solar         = src.geracao_solar,
    tgt.carga                 = src.carga,
    tgt.intercambio           = src.intercambio,
    tgt.dt_ingestao           = src.dt_ingestao,
    tgt.dt_ultima_atualizacao = CURRENT_TIMESTAMP()

-- Caso o registro vindo do staging não exista na tabela de consumo, inserimos um novo registro.
WHEN NOT MATCHED THEN
  INSERT (
    dt_referencia,
    id_subsistema,
    nom_subsistema,
    geracao_hidraulica,
    geracao_termica,
    geracao_eolica,
    geracao_solar,
    carga,
    intercambio,
    dt_ingestao,
    dt_ultima_atualizacao
  )
  VALUES (
    src.din_instante,
    src.id_subsistema,
    src.nom_subsistema,
    src.geracao_hidraulica,
    src.geracao_termica,
    src.geracao_eolica,
    src.geracao_solar,
    src.carga,
    src.intercambio,
    src.dt_ingestao,
    CURRENT_TIMESTAMP()
  );