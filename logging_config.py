import logging
from opencensus.ext.azure.log_exporter import AzureLogHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = AzureLogHandler()
logger.addHandler(handler)

def log_event(message):
    logger.info(message)
