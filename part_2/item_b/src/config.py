import os


class IngestionConfig:
    raw_dir: str = os.environ.get("RAW_DIR", "data/raw")
    curated_dir: str = os.environ.get("CURATED_DIR", "data/curated")
    http_timeout: int = int(os.environ.get("HTTP_TIMEOUT", "60"))

CONFIG = IngestionConfig()