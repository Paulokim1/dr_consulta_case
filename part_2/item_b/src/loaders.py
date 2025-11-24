import io
from pathlib import Path
import requests
import pandas as pd

from src.config import CONFIG
from src.logger import get_logger

logger = get_logger(__name__)

def save_raw(content: str, path: str):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True) # Garante que o diretório existe
    p.write_text(content, encoding="utf-8")
    logger.info("Raw file saved", extra={"details": {"path": path}})

def download_and_store_raw(url: str, raw_path: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=CONFIG.http_timeout)
    resp.raise_for_status()
    save_raw(resp.text, str(raw_path))
    return pd.read_csv(io.StringIO(resp.text), sep=";")

def save_parquet(df: pd.DataFrame, path: str):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p, index=False)
    logger.info("Parquet saved", extra={"details": {"path": path, "rows": len(df)}})