# service_async.py
import os
import aiohttp
import logging
import re
import aiofiles
import pandas as pd
from datetime import datetime, date
from typing import List, Optional, Any, Dict
from bs4 import BeautifulSoup
from repository_async import AsyncRepository
import asyncio
from sqlalchemy.orm import sessionmaker
from http_client import HttpClient  # Импорт клиента
from functools import partial
import aiofiles.os  # Добавлен импорт aiofiles.os

logging.basicConfig(level=logging.INFO)


class SpimexServiceAsync:
    """
    Сервис для обработки отчетов СПбМТСБ.
    """
    REPORTS_DIR = "async/reports"  # Путь к директории для сохранения отчетов
    BASE_URL = "https://spimex.com/markets/oil_products/trades/results/"

    def __init__(self, async_sessionmaker: sessionmaker):
        """
        Инициализация сервиса.

        :param async_sessionmaker: Фабрика для создания асинхронных сессий базы данных.
        """
        self.async_sessionmaker = async_sessionmaker
        os.makedirs(self.REPORTS_DIR, exist_ok=True)

    def calculate_months_limit(self) -> int:
        """
        Вычисляет количество месяцев с начала 2023 года до текущего месяца.

        :return: Количество месяцев.
        """
        start_date = datetime(2023, 1, 1)
        current_date = datetime.now()
        return (current_date.year - start_date.year) * 12 + current_date.month - start_date.month

    async def fetch_report_links(self, months_limit: int) -> List[str]:
        """
        Асинхронно получает ссылки на отчеты.

        :param months_limit: Ограничение на количество отчетов.
        :return: Список URL-адресов отчетов.
        """
        page_number = 1
        collected_links: List[str] = []
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            while len(collected_links) < months_limit:
                url = f"{self.BASE_URL}?page={page_number}"
                try:
                    async with session.get(url) as response:
                        if response.status != 200:
                            logging.info(
                                f"Ошибка загрузки страницы {page_number}. Код ответа: {response.status}")
                            break

                        text = await response.text()
                        soup = BeautifulSoup(text, "html.parser")
                        links = soup.select(
                            "a.accordeon-inner__item-title.link.xls")

                        for link in links:
                            if "Бюллетень по итогам торгов в Секции «Нефтепродукты»" in link.text:
                                href = link.get("href")
                                full_link = f"https://spimex.com{href}"
                                collected_links.append(full_link)
                                logging.info(f"Ссылка на файл: {full_link}")

                                if len(collected_links) >= months_limit:
                                    break
                except Exception as e:
                    logging.error(
                        f"Ошибка при обработке страницы {page_number}: {e}")
                    break

                page_number += 1

        logging.info(f"Всего собрано ссылок: {len(collected_links)}")
        return collected_links[:months_limit]

    async def extract_trade_date(self, file_path: str) -> Optional[date]:
        """
        Асинхронно извлекает дату торгов из файла отчета.

        :param file_path: Путь к файлу отчета.
        :return: Дата торгов или None при ошибке.
        """
        try:
            # Чтение файла с использованием partial для передачи именованных аргументов
            loop = asyncio.get_event_loop()
            read_excel = partial(pd.read_excel, file_path, header=None)
            df = await loop.run_in_executor(None, read_excel)
            for row in df.itertuples(index=False):
                for cell in row:
                    if isinstance(cell, str) and "Дата торгов:" in cell:
                        date_match = re.search(r"\d{2}\.\d{2}\.\d{4}", cell)
                        if date_match:
                            trade_date = datetime.strptime(
                                date_match.group(), "%d.%m.%Y")
                            logging.info(
                                f"Дата торгов успешно извлечена: {trade_date.date()}")
                            return trade_date.date()
            logging.error(f"Дата не найдена в файле {file_path}")
            return None
        except Exception as e:
            logging.error(f"Ошибка извлечения даты из файла {file_path}: {e}")
            return None

    async def download_and_save_reports(self, report_links: List[str]) -> None:
        """
        Асинхронно скачивает и сохраняет отчеты по предоставленным ссылкам.

        :param report_links: Список URL-адресов отчетов.
        """
        queue: asyncio.Queue = asyncio.Queue()
        # Ограничение на количество одновременных загрузок
        semaphore = asyncio.Semaphore(5)

        # Запуск потребителя (consumer), который будет сохранять данные в БД
        consumer_task = asyncio.create_task(self.consumer(queue))

        # Создание и запуск задач скачивания отчетов
        download_tasks = [
            asyncio.create_task(self.download_report(
                url, index, semaphore, queue))
            for index, url in enumerate(report_links, start=1)
        ]

        # Ожидание завершения всех задач скачивания
        await asyncio.gather(*download_tasks)

        # Отправка сигнала завершения потребителю
        await queue.put(None)

        # Ожидание завершения потребителя
        await consumer_task

    async def download_report(self, url: str, index: int, semaphore: asyncio.Semaphore, queue: asyncio.Queue) -> Optional[str]:
        """
        Асинхронно скачивает отчет по URL и сохраняет его на диск.

        :param url: URL-адрес отчета.
        :param index: Индекс отчета для именования файла.
        :param semaphore: Семафор для ограничения количества одновременных загрузок.
        :param queue: Очередь для передачи данных потребителю.
        :return: Путь к сохраненному файлу или None при ошибке.
        """
        async with semaphore:
            async with HttpClient(ssl=False) as client:
                response = await client.fetch(url)
                if response is None:
                    logging.error(f"Не удалось скачать отчет по ссылке: {url}")
                    return None

                temp_file_path = os.path.join(
                    self.REPORTS_DIR, f"temp_report_{index}.xls")
                try:
                    content = await response.read()
                    async with aiofiles.open(temp_file_path, "wb") as file:
                        await file.write(content)
                    logging.info(f"Файл временно сохранен: {temp_file_path}")
                except Exception as e:
                    logging.error(f"Ошибка записи файла {temp_file_path}: {e}")
                    return None

                report_date = await self.extract_trade_date(temp_file_path)

                if report_date:
                    final_file_path = os.path.join(
                        self.REPORTS_DIR, f"{report_date}.xls")
                    try:
                        os.rename(temp_file_path, final_file_path)
                        logging.info(
                            f"Файл сохранен окончательно: {final_file_path}")
                    except Exception as e:
                        logging.error(
                            f"Ошибка переименования файла {temp_file_path} в {final_file_path}: {e}")
                        return None

                    # Чтение и подготовка данных для сохранения
                    try:
                        reports_data = await self.parse_report(final_file_path, report_date)
                        if reports_data:
                            await queue.put(reports_data)
                    except Exception as e:
                        logging.error(
                            f"Ошибка при парсинге отчета {final_file_path}: {e}")
                        return None

                    return final_file_path
                else:
                    try:
                        await aiofiles.os.remove(temp_file_path)
                        logging.warning(
                            f"Файл {temp_file_path} удален из-за отсутствия даты.")
                    except Exception as e:
                        logging.error(
                            f"Ошибка удаления файла {temp_file_path}: {e}")
                    return None

    async def parse_report(self, file_path: str, report_date: date) -> Optional[List[Dict[str, Any]]]:
        """
        Асинхронно парсит отчет из файла и возвращает список данных для сохранения.

        :param file_path: Путь к файлу отчета.
        :param report_date: Дата отчета.
        :return: Список словарей с данными отчетов или None при ошибке.
        """
        try:
            loop = asyncio.get_event_loop()
            read_excel = partial(pd.read_excel, file_path, skiprows=6)
            df = await loop.run_in_executor(None, read_excel)
            df.columns = df.columns.to_series().ffill()
            df = df.fillna('')

            column_mapping = {
                'Код\nИнструмента': 'exchange_product_id',
                'Наименование\nИнструмента': 'exchange_product_name',
                'Базис\nпоставки': 'delivery_basis_id',
                'Объем\nДоговоров\nв единицах\nизмерения': 'volume',
                'Обьем\nДоговоров,\nруб.': 'total',
                'Цена в Заявках (за единицу\nизмерения)': 'delivery_type_id',
                'Количество\nДоговоров,\nшт.': 'count',
            }
            df.rename(columns=column_mapping, inplace=True)
            required_columns = list(column_mapping.values())
            df = df[required_columns].replace({'-': None, '': None})
            df = df[~df['exchange_product_id'].str.contains('Итого', na=False)]
            df.dropna(subset=[
                      'exchange_product_id', 'exchange_product_name', 'delivery_basis_id'], inplace=True)

            reports_data = []
            for _, row in df.iterrows():
                report_data = {
                    "exchange_product_id": row['exchange_product_id'],
                    "exchange_product_name": row['exchange_product_name'],
                    "oil_id": row['exchange_product_id'][:4] if isinstance(row['exchange_product_id'], str) else None,
                    "delivery_basis_id": row['delivery_basis_id'],
                    "delivery_basis_name": "",
                    "delivery_type_id": str(row['delivery_type_id']) if row['delivery_type_id'] is not None else None,
                    "volume": self.try_convert_to_float(row['volume']),
                    "total": self.try_convert_to_float(row['total']),
                    "count": self.try_convert_to_int(row['count']),
                    "date": report_date
                }

                # Логирование данных перед добавлением в очередь
                logging.debug(
                    f"Подготовка данных для сохранения в базу: {report_data}")

                reports_data.append(report_data)

            return reports_data
        except Exception as e:
            logging.error(
                f"Ошибка при парсинге отчета из файла '{file_path}': {e}")
            return None

    async def consumer(self, queue: asyncio.Queue) -> None:
        """
        Потребитель, который читает данные из очереди и сохраняет их в базу данных.

        :param queue: Очередь с данными отчетов.
        """
        async with self.async_sessionmaker() as db_session:
            repository = AsyncRepository(db_session)
            try:
                while True:
                    reports_data = await queue.get()
                    if reports_data is None:
                        # Сигнал завершения
                        logging.info("Потребитель получил сигнал завершения.")
                        break

                    for report_data in reports_data:
                        try:
                            if not await repository.is_report_in_db(report_data['date']):
                                await repository.add_report_data(report_data)
                                logging.info(
                                    f"Добавлен отчет в базу данных: {report_data}")
                            else:
                                logging.info(
                                    f"Отчет за {report_data['date']} уже существует в базе данных. Пропуск записи.")
                        except Exception as e:
                            logging.error(
                                f"Ошибка при добавлении отчета в базу данных: {e}")

                # После завершения добавляем коммит
                await db_session.commit()
                logging.info("Коммит всех добавленных отчетов.")
            except Exception as e:
                await db_session.rollback()
                logging.error(
                    f"Ошибка в потребителе при сохранении отчетов: {e}")
                raise

    @staticmethod
    def try_convert_to_float(value: Any) -> Optional[float]:
        """
        Пытается преобразовать значение в float.

        :param value: Значение для преобразования.
        :return: Преобразованное значение или None.
        """
        try:
            return float(value) if value is not None else None
        except ValueError:
            return None

    @staticmethod
    def try_convert_to_int(value: Any) -> Optional[int]:
        """
        Пытается преобразовать значение в int.

        :param value: Значение для преобразования.
        :return: Преобразованное значение или None.
        """
        try:
            return int(value) if value is not None else None
        except ValueError:
            return None
