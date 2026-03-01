"""
Модуль для работы с базой данных PostgreSQL.
"""
import psycopg2
from datetime import datetime
import logging
from config import DATABASE_URL, LOG_FILE

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Менеджер для работы с базой данных"""
    
    def __init__(self, db_url) -> None:
        self.db_url = db_url
        self.connection = None
    
    def connect(self) -> bool:
        """Подключение к базе данных"""
        try:
            self.connection = psycopg2.connect(self.db_url)
            logger.info("Connection to database successful")
            return True
        
        except psycopg2.OperationalError as e:
            logger.error(f"Database connection error: {e}")
            return False
        
        except Exception as e:
            logger.error(f"Unexpected error while connecting: {e}")
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
                logger.info("Tables successfully created")
                return True
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            self.connection.rollback()
            return False
        

    def insert_request(self, api_url: str, status: str) -> int | None:
        """
        Вставка записи в таблицу requests
        Возвращает ID вставленной записи
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO requests (api_url, status, request_time)
                    VALUES (%s, %s, %s)
                    RETURNING id
                ''', (api_url, status, datetime.now()))
                
                request_id = cursor.fetchone()[0]
                self.connection.commit()
                return request_id
            
        except Exception as e:
            logger.error(f"Error inserting into requests: {e}")
            self.connection.rollback()
            return None

    def insert_responses(self, request_id: int, currencies: list[dict]) -> bool:
        """
        Вставка курсов валют в таблицу responses
        currencies - список словарей с данными о валютах
        """
        try:
            with self.connection.cursor() as cursor:
                for currency in currencies:
                    cursor.execute('''
                        INSERT INTO responses 
                        (request_id, currency_code, currency_name, rate, nominal)
                        VALUES (%s, %s, %s, %s, %s)
                    ''', (
                        request_id,
                        currency['code'],
                        currency['name'],
                        currency['rate'],
                        currency['nominal']
                    ))
                
                self.connection.commit()
                logger.info(f"Saved {len(currencies)} currencies for request_id {request_id}")
                return True
        except Exception as e:
            logger.error(f"Error inserting into responses: {e}")
            self.connection.rollback()
            return False

    
    def close(self) -> None:
        """Закрытие соединения с БД"""
        if self.connection:
            self.connection.close()
            logger.info("Connection to database closed")