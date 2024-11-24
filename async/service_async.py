import os
import logging
import re
import aiohttp
import pandas as pd
from datetime import datetime, date
from typing import List, Optional, Any
from bs4 import BeautifulSoup
from repository_async import AsyncRepository
import asyncio
from sqlalchemy.orm import sessionmaker


class SpimexServiceAsync:
    REPORTS_DIR = "async/reports"  # Сохранение отчетов в async/reports
    BASE_URL = "https://spimex.com/markets/oil_products/trades/results/"

    def __init__(self, async_sessionmaker: sessionmaker):
        self.async_sessionmaker = async_sessionmaker
        os.makedirs(self.REPORTS_DIR, exist_ok=True)

    def calculate_months_limit(self) -> int:
        start_date = datetime(2023, 1, 1)
        current_date = datetime.now()
        return (current_date.year - start_date.year) * 12 + current_date.month - start_date.month

    async def fetch_report_links(self, months_limit: int) -> List[str]:
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

        return collected_links[:months_limit]

    async def extract_trade_date(self, file_path: str) -> Optional[date]:
        try:
            df = pd.read_excel(file_path, header=None)
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
        tasks = []
        # Ограничение на количество одновременных загрузок
        semaphore = asyncio.Semaphore(5)

        for i, url in enumerate(report_links, start=1):
            task = asyncio.create_task(self.download_report(url, i, semaphore))
            tasks.append(task)

        await asyncio.gather(*tasks)

    async def download_report(self, url: str, index: int, semaphore: asyncio.Semaphore) -> Optional[str]:
        async with semaphore:
            try:
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            temp_file_path = os.path.join(
                                self.REPORTS_DIR, f"temp_report_{index}.xls")
                            content = await response.read()
                            with open(temp_file_path, "wb") as file:
                                file.write(content)

                            report_date = await self.extract_trade_date(temp_file_path)

                            if report_date:
                                final_file_path = os.path.join(
                                    self.REPORTS_DIR, f"{report_date}.xls")
                                os.rename(temp_file_path, final_file_path)
                                logging.info(
                                    f"Файл сохранен: {final_file_path}")

                                # Создаём новую сессию для базы данных
                                async with self.async_sessionmaker() as db_session:
                                    repository = AsyncRepository(db_session)
                                    if not await repository.is_report_in_db(report_date):
                                        await self.save_report_to_db(final_file_path, report_date, repository)
                                    else:
                                        logging.info(
                                            f"Отчет за {report_date} уже существует в базе данных. Пропуск записи.")
                                return final_file_path
                            else:
                                os.remove(temp_file_path)
                                logging.warning(
                                    f"Файл {temp_file_path} удален из-за отсутствия даты.")
                                return None
                        else:
                            logging.error(
                                f"Ошибка скачивания файла по ссылке {url}. Код ответа: {response.status}")
                            return None
            except Exception as e:
                logging.error(
                    f"Ошибка при скачивании файла по ссылке {url}: {e}")
                return None

    async def save_report_to_db(self, file_path: str, report_date: date, repository: AsyncRepository) -> None:
        try:
            df = pd.read_excel(file_path, skiprows=6)
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

            for _, row in df.iterrows():
                report_data = {
                    "exchange_product_id": row['exchange_product_id'],
                    "exchange_product_name": row['exchange_product_name'],
                    "oil_id": row['exchange_product_id'][:4] if isinstance(row['exchange_product_id'], str) else None,
                    "delivery_basis_id": row['delivery_basis_id'],
                    "delivery_basis_name": "",
                    # Преобразование в строку
                    "delivery_type_id": str(row['delivery_type_id']) if row['delivery_type_id'] is not None else None,
                    "volume": self.try_convert_to_float(row['volume']),
                    "total": self.try_convert_to_float(row['total']),
                    "count": self.try_convert_to_int(row['count']),
                    "date": report_date
                }

                # Логирование данных перед вставкой
                logging.debug(f"Сохранение данных в базу: {report_data}")

                await repository.save_report_data(report_data)
        except Exception as e:
            logging.error(
                f"Ошибка при сохранении данных из файла '{file_path}' в базу данных: {e}")

    @staticmethod
    def try_convert_to_float(value: Any) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except ValueError:
            return None

    @staticmethod
    def try_convert_to_int(value: Any) -> Optional[int]:
        try:
            return int(value) if value is not None else None
        except ValueError:
            return None
