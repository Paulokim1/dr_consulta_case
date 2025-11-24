# ingestion/quality.py

import pandas as pd
from src.logger import get_logger

logger = get_logger(__name__)


class SchemaValidationError(Exception):
    pass


class DataQualityError(Exception):
    pass


def validate_schema(df: pd.DataFrame, required_cols):
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise SchemaValidationError(f"Schema inválido. Faltando colunas: {missing}")


def data_quality(df: pd.DataFrame):
    """Regras simples de qualidade para o Balanço de Energia."""

    if df.empty:
        raise DataQualityError("DataFrame vazio após normalização.")

    # Carga não pode ser nula (dicionário diz 'Não permite nulo')
    if df["carga"].isna().any():
        raise DataQualityError("Valores nulos em 'carga'.")

    # Intercâmbio também não permite nulo
    if df["intercambio"].isna().any():
        raise DataQualityError("Valores nulos em 'intercambio'.")

    # Evitar dataset inteiro zerado (pode indicar erro na origem)
    if df["carga"].abs().sum() == 0:
        raise DataQualityError(
            "Soma absoluta de 'carga' == 0. "
            "Possível problema na origem (arquivo zerado)."
        )
    
    else:
        logger.info("Data quality checks passed.")