import requests
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime


# Настройка логирования
logging.basicConfig(
    level=logging.INFO
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
    )
logger = logging.getLogger(__name__)

    

if __name__ == "__main__":
    