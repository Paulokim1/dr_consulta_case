-- 1) Apaga registros da chave que já existem no GOLD
DELETE FROM fact_balanco_subsistema
WHERE (din_instante, id_subsistema) IN (
    SELECT din_instante, id_subsistema
    FROM balanco_subsistema_stg
);

-- 2) Insere novamente
INSERT INTO fact_balanco_subsistema (
    din_instante,
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
SELECT
    din_instante,
    id_subsistema,
    nom_subsistema,
    geracao_hidraulica,
    geracao_termica,
    geracao_eolica,
    geracao_solar,
    carga,
    intercambio,
    dt_ingestao,
    datetime('now')
FROM balanco_subsistema_stg;