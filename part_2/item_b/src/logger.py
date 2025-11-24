import json
import logging
import sys

class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "logger": record.name,
            "message": record.getMessage()
        }
        if hasattr(record, "details"):
            payload.update(record.details)
        return json.dumps(payload, ensure_ascii=False)

def get_logger(name: str):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return logger