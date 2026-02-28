"""
Модуль для работы с базой данных PostgreSQL.
"""
import psycopg2
from psycopg2.extras import RealDictCursor
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
            logger.error(f"Ошибка при вставке в requests: {e}")
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
                logger.info(f"Сохранено {len(currencies)} валют")
                return True
        except Exception as e:
            logger.error(f"Ошибка при вставке в responses: {e}")
            self.connection.rollback()
            return False
        

    def get_latest_rates(self, limit: int = 10):
        """Получение последних курсов валют"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute('''
                    SELECT 
                        r.id, 
                        r.request_time, 
                        resp.currency_code, 
                        resp.currency_name, 
                        resp.rate
                    FROM responses resp
                    JOIN requests r ON resp.request_id = r.id
                    ORDER BY r.request_time DESC, resp.id DESC
                    LIMIT %s
                ''', (limit))
                
                return cursor.fetchall()
            
        except Exception as e:
            logger.error(f"Ошибка при чтении из БД: {e}")
            return []
    
    def close(self):
        """Закрытие соединения с БД"""
        if self.connection:
            self.connection.close()
            logger.info("Соединение с БД закрыто")