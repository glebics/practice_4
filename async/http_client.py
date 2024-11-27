# http_client.py
import aiohttp
import asyncio
import logging
from typing import Optional
from functools import wraps
from aiohttp import ClientResponseError, ClientConnectorError, ClientError


def handle_aiohttp_exceptions(func):
    """
    Декоратор для перехвата исключений aiohttp и проверки статусов ответов.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            response = await func(*args, **kwargs)
            if response and response.status != 200:
                logging.error(
                    f"Неудачный статус ответа: {response.status} для URL: {response.url}")
                return None
            return response
        except ClientResponseError as e:
            url = getattr(e, 'request_info', None)
            url = url.url if url else 'Неизвестный URL'
            logging.error(f"ClientResponseError для URL {url}: {e.message}")
        except ClientConnectorError as e:
            url = getattr(e, 'request_info', None)
            url = url.url if url else 'Неизвестный URL'
            logging.error(f"ClientConnectorError для URL {url}: {e.message}")
        except ClientError as e:
            logging.error(f"ClientError: {e}")
        except Exception as e:
            logging.error(f"Неизвестная ошибка при запросе: {e}")
        return None
    return wrapper


class HttpClient:
    """
    Класс клиента для обработки HTTP-запросов.
    """

    def __init__(self, timeout: int = 60, ssl: bool = False):
        """
        Инициализация клиента.

        :param timeout: Таймаут для запросов в секундах.
        :param ssl: Флаг для отключения проверки SSL-сертификата.
        """
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.ssl = ssl
        self.session = None

    async def __aenter__(self):
        """
        Вход в контекстный менеджер: открывает сессию.
        """
        connector = aiohttp.TCPConnector(ssl=self.ssl)
        self.session = aiohttp.ClientSession(
            timeout=self.timeout, connector=connector)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """
        Выход из контекстного менеджера: закрывает сессию.
        """
        if self.session:
            await self.session.close()

    @handle_aiohttp_exceptions
    async def fetch(self, url: str) -> Optional[aiohttp.ClientResponse]:
        """
        Выполняет GET-запрос к указанному URL.

        :param url: URL для запроса.
        :return: Объект ClientResponse или None при ошибке.
        """
        return await self.session.get(url)
