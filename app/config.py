"""
Конфигурация приложения.
Загружает переменные окружения из .env файла
"""
import os
from dotenv import load_dotenv

# Загрузка переменных из .env
load_dotenv()

# Параметры подключения к БД
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
DB_NAME = os.getenv('DB_NAME', 'exchange_rates_db')

# Параметры приложения
FETCH_INTERVAL = int(os.getenv('FETCH_INTERVAL', '5'))  # Интервал в минутах
API_TIMEOUT = int(os.getenv('API_TIMEOUT', '10'))  # Таймаут в секундах
API_URL = os.getenv('API_URL', 'https://www.cbr.ru/scripts/XML_daily.asp')
LOG_FILE = os.getenv('LOG_FILE', 'logs/error.log')

# Строка подключения к БД
DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'