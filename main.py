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


class ExchangeRatesService:
    """Сервис для получения и сохранения курсов валют"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.fetch_interval = 5 * 60  # Конвертирование минут в секунды
    
    def fetch_rates(self) -> list[dict] | None:
        """
        Получение курсов валют с API ЦБР
        Возвращает список словарей с курсами
        """
        try:
            logger.info(f"Отправка запроса: {URL}")
            
            # Запрос к API с таймаутом
            response = requests.get(URL, timeout=TIMEOUT)
            response.raise_for_status()
            
            logger.info("Ответ получен")
            
            # Парсинг XML
            rates = self._parse_xml(response.text)
            logger.info(f"Найдено {len(rates)} валют")
            
            return rates
        
        except requests.exceptions.Timeout:
            logger.error(f"Сервер не ответил за {TIMEOUT} секунд")
            return None
        
        except requests.exceptions.ConnectionError as e:
            logger.error(f"ОШИБКА ПОДКЛЮЧЕНИЯ: {e}")
            return None
        
        except requests.exceptions.RequestException as e:
            logger.error(f"ОШИБКА ЗАПРОСА: {e}")
            return None
        
        except Exception as e:
            logger.error(f"НЕОЖИДАННАЯ ОШИБКА при получении данных: {e}")
            return None


    def _parse_xml(self, xml_content: str) -> list[dict] | None:
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
            logger.error(f"Ошибка парсинга XML: {e}")
            return None
        
        except Exception as e:
            logger.error(f"Ошибка при обработке XML: {e}")
            return None






    

if __name__ == "__main__":