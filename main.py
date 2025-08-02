import logging

from configs.logging_config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting the application")