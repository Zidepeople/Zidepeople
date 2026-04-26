import logging
import os


def _resolve_log_level() -> int:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    return getattr(logging, level_name, logging.INFO)


_LOG_LEVEL = _resolve_log_level()

root_logger = logging.getLogger()
root_logger.setLevel(_LOG_LEVEL)

logger = logging.getLogger(__name__)
logger.setLevel(_LOG_LEVEL)

try:
    from opencensus.ext.azure.log_exporter import AzureLogHandler

    handler = AzureLogHandler()
    handler.setLevel(_LOG_LEVEL)
    logger.addHandler(handler)
except Exception as exc:
    if not logger.handlers:
        fallback_handler = logging.StreamHandler()
        fallback_handler.setLevel(_LOG_LEVEL)
        logger.addHandler(fallback_handler)
    logger.warning("AzureLogHandler disabled: %s", exc)

def log_event(message):
    logger.info(message)
