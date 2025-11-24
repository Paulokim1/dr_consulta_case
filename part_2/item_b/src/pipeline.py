# ingestion/pipeline.py

from pathlib import Path

from src.config import CONFIG
from src.loaders import download_and_store_raw, save_parquet
from src.normalizers import normalize
from src.quality import data_quality, SchemaValidationError, DataQualityError
from src.logger import get_logger

logger = get_logger(__name__)


def build_paths(data_ref: str):
    """
    Constrói paths de Bronze (raw) e Silver (parquet) com base na data de referência.
    data_ref pode ser '2025' (usamos só ano para particionar).
    """
    year = data_ref

    raw_path = (
        Path(CONFIG.raw_dir)
        / "balanco_subsistema"
        / f"year={year}"
        / f"balanco_raw_{year}.csv"
    )

    curated_path = (
        Path(CONFIG.curated_dir)
        / "balanco_subsistema"
        / f"year={year}"
        / f"balanco_subsistema_{year}.parquet"
    )

    return raw_path, curated_path


def run_ingestion(url: str, data_ref: str):
    """
    Executa a ingestão do Balanço de Energia por Subsistema:
    - Bronze: salva CSV bruto
    - Silver: normaliza e salva Parquet
    """

    raw_path, curated_path = build_paths(data_ref)

    try:
        # Bronze: download + persistência raw
        df_raw = download_and_store_raw(url, raw_path)

        # Silver: normalização
        df_norm = normalize(df_raw)

        # Data Quality
        data_quality(df_norm)

        # Salva Parquet (Silver)
        save_parquet(df_norm, str(curated_path))

        logger.info(
            "Ingestão concluída com sucesso (Balanço de Energia por Subsistema)",
            extra={
                "details": {
                    "rows": len(df_norm),
                    "data_ref": data_ref,
                    "parquet_path": str(curated_path),
                }
            },
        )

    except (SchemaValidationError, DataQualityError) as e:
        logger.error(
            "Erro de schema/data quality na ingestão",
            extra={"details": {"error": str(e), "data_ref": data_ref}},
        )
        # Deixa a exceção subir para o Composer/Cloud Run marcar a task como FAILED
        raise

    except Exception as e:  # erro inesperado
        logger.error(
            "Erro inesperado na ingestão",
            extra={"details": {"error": str(e), "data_ref": data_ref}},
        )
        raise