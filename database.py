"""
Модуль для работы с базой данных PostgreSQL.
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import logging
from config import DATABASE_URL, LOG_FILE, LOG_LEVEL

# Настройка логирования
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Менеджер для работы с базой данных"""
    
    def __init__(self, db_url):
        self.db_url = db_url
        self.connection = None
    
    def connect(self) -> bool:
        """Подключение к базе данных"""
        try:
            self.connection = psycopg2.connect(self.db_url)
            logger.info("Успешное подключение к БД")
            return True
        
        except psycopg2.OperationalError as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            return False
        
        except Exception as e:
            logger.error(f"Неожиданная ошибка при подключении: {e}")
            return False
    
    def create_tables(self) -> bool:
        """Создание таблиц requests и responses"""
        try:
            with self.connection.cursor() as cursor:
                # Таблица requests - для хранения информации о запросах
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS requests (
                        id SERIAL PRIMARY KEY,
                        request_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        api_url VARCHAR(255) NOT NULL,
                        status VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Таблица responses - для хранения курсов валют (связана с requests)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS responses (
                        id SERIAL PRIMARY KEY,
                        request_id INTEGER NOT NULL REFERENCES requests(id) ON DELETE CASCADE,
                        currency_code VARCHAR(3) NOT NULL,
                        currency_name VARCHAR(100),
                        rate DECIMAL(10, 4) NOT NULL,
                        nominal INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                self.connection.commit()
                logger.info("Таблицы успешно созданы")
                return True
            
        except Exception as e:
            logger.error(f"Ошибка при создании таблиц: {e}")
            self.connection.rollback()
            return False