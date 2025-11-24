# ingestion/normalizers.py

from datetime import datetime
import pandas as pd

from src.schemas import BALANCO_REQUIRED_COLUMNS
from src.quality import validate_schema
from src.logger import get_logger

logger = get_logger(__name__)


def _to_float(series: pd.Series) -> pd.Series:
    """
    Converte colunas numéricas que podem vir com vírgula decimal.
    Permite nulos (NaN) para colunas de geração, conforme documentação.
    """
    return (
        series.astype(str)
        .str.replace(",", ".", regex=False)
        .replace({"": None})
        .astype(float)
    )


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza o dataset 'Balanço de Energia por Subsistema' do ONS
    para um schema analítico único.
    """
    # 1. Validação de schema
    validate_schema(df, BALANCO_REQUIRED_COLUMNS)

    # 2. Conversão básica
    df_out = pd.DataFrame({
        "din_instante": pd.to_datetime(df["din_instante"]),
        "id_subsistema": df["id_subsistema"].astype(str).str.strip(),
        "nom_subsistema": df["nom_subsistema"].astype(str).str.strip(),
    })

    # 3. Mapeamento de colunas numéricas → nomes normalizados
    metric_map = {
        "val_gerhidraulica": "geracao_hidraulica",
        "val_gertermica": "geracao_termica",
        "val_gereolica": "geracao_eolica",
        "val_gersolar": "geracao_solar",
        "val_carga": "carga",
        "val_intercambio": "intercambio",
    }

    # Conversão vetorizada + renomeação automática
    for raw_col, final_col in metric_map.items():
        df_out[final_col] = _to_float(df[raw_col])

    # 4. Auditoria
    df_out["dt_ingestao"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    logger.info(
        "Dataset normalizado (Balanço de Energia por Subsistema)",
        extra={
            "details": {
                "rows": len(df_out),
                "columns": list(df_out.columns),
            }
        },
    )

    return df_out