"""
Основной скрипт для получения курсов валют с API ЦБР РФ.
Запрашивает данные каждые 5 минут и сохраняет в PostgreSQL
"""
import requests
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from config import API_URL, FETCH_INTERVAL, API_TIMEOUT, \
    DATABASE_URL, LOG_FILE
from database import DatabaseManager


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


class ExchangeRatesService:
    """Сервис для получения и сохранения курсов валют"""
    
    def __init__(self, db_manager) -> None:
        self.db = db_manager
        self.fetch_interval = FETCH_INTERVAL * 60  # Конвертирование минут в секунды

    def fetch_rates(self) -> list[dict[str, str]] | None:
        """
        Получение курсов валют с API ЦБР
        Возвращает список словарей с курсами
        """
        try:
            logger.info(f"Send request: {API_URL}")

            # Запрос к API с таймаутом
            response = requests.get(API_URL, timeout=API_TIMEOUT)
            response.raise_for_status()

            logger.info("Response received")

            # Парсинг XML
            rates = self._parse_xml(response.text)
            logger.info(f"Found {len(rates)} currencies")

            return rates
        
        except requests.exceptions.Timeout:
            logger.error(f"Server did not respond within {API_TIMEOUT} seconds")
            return None
        
        except requests.exceptions.ConnectionError as e:
            logger.error(f"CONNECTION ERROR: {e}")
            return None
        
        except requests.exceptions.RequestException as e:
            logger.error(f"REQUEST ERROR: {e}")
            return None
        
        except Exception as e:
            logger.error(f"UNEXPECTED ERROR while fetching data: {e}")
            return None


    def _parse_xml(self, xml_content: str) -> list[dict[str, str]] | None:
        """Парсинг XML ответа от API ЦБР"""
        try:
            root = ET.fromstring(xml_content)
            rates = []
            
            # Извлечение курсов валют из XML
            for valute in root.findall('Valute'):
                currency_code = valute.find('CharCode').text
                currency_name = valute.find('Name').text
                # Заменяем запятую на точку для корректного парсинга числа
                rate_value = float(valute.find('Value').text.replace(',', '.'))
                nominal = int(valute.find('Nominal').text)
                
                rates.append({
                    'code': currency_code,
                    'name': currency_name,
                    'rate': rate_value,
                    'nominal': nominal
                })
            
            return rates
        
        except ET.ParseError as e:
            logger.error(f"Parsing error: {e}")
            return None
        
        except Exception as e:
            logger.error(f"Error processing XML: {e}")
            return None


    def save_to_database(self, rates: list[dict]) -> bool:
            """Сохранение курсов в базу данных"""
            if not rates:
                logger.warning("Not saving: no currency data available")
                return False
            
            # Создание записи о запросе
            request_id = self.db.insert_request(API_URL, 'success')
            
            if not request_id:
                logger.error("Failed to create request record in database")
                return False
            
            # Сохранение курсов валют
            if self.db.insert_responses(request_id, rates):
                logger.info(f"Data successfully saved to database (request_id: {request_id})")
                return True
            else:
                logger.error("Failed to save data to database")
                return False


    def run(self) -> None:
        """Основной цикл сервиса"""
        logger.info("\n" + "=" * 50)
        logger.info("Run service")
        logger.info(f"Update interval: {FETCH_INTERVAL} minutes")
        logger.info("\n" + "=" * 50)
                
        while True:
            logger.info(f"\n--- ({datetime.now()}) ---")
            
            # Получение курсов с API
            rates = self.fetch_rates()
                        
            if rates:
                self.save_to_database(rates)  # Сохранение в БД, если данные получены
            else:
                self.db.insert_request(API_URL, 'failed')  # Сохранение информации об ошибке

            logger.info(f"Wait {FETCH_INTERVAL} minutes until the next request...")
            time.sleep(self.fetch_interval)

    
def main():
    """Точка входа приложения"""
    # Инициализация менеджера БД
    db_manager = DatabaseManager(DATABASE_URL)
    
    # Подключение к БД
    if not db_manager.connect():
        logger.critical("Connect to database failed")
        return
    
    # Создание таблиц
    if not db_manager.create_tables():
        logger.critical("Failed to create tables")
        db_manager.close()
        return
    
    # Запуск сервиса
    service = ExchangeRatesService(db_manager)
    
    try:
        service.run()
    except Exception as e:
        logger.critical(f"Critical error in application: {e}")
    finally:
        db_manager.close()


if __name__ == '__main__':
    main()