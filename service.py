import os
import logging
import re
import requests
import pandas as pd
from datetime import datetime, date
from typing import List, Optional, Any
from bs4 import BeautifulSoup
from repository import Repository

class SpimexService:
    REPORTS_DIR = "reports"
    BASE_URL = "https://spimex.com/markets/oil_products/trades/results/"

    def __init__(self, repository: Repository):
        self.repository = repository
        os.makedirs(self.REPORTS_DIR, exist_ok=True)

    def calculate_months_limit(self) -> int:
        start_date = datetime(2023, 1, 1)
        current_date = datetime.now()
        return (current_date.year - start_date.year) * 12 + current_date.month - start_date.month

    def fetch_report_links(self, months_limit: int) -> List[str]:
        session = requests.Session()
        page_number = 1
        collected_links: List[str] = []

        while len(collected_links) < months_limit:
            url = f"{self.BASE_URL}?page={page_number}"
            response = session.get(url)

            if response.status_code != 200:
                logging.info(f"Ошибка загрузки страницы {page_number}. Код ответа: {response.status_code}")
                break

            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.select("a.accordeon-inner__item-title.link.xls")

            for link in links:
                if "Бюллетень по итогам торгов в Секции «Нефтепродукты»" in link.text:
                    href = link.get("href")
                    full_link = f"https://spimex.com{href}"
                    collected_links.append(full_link)
                    logging.info(f"Ссылка на файл: {full_link}")

                    if len(collected_links) >= months_limit:
                        break

            page_number += 1

        return collected_links[:months_limit]

    def extract_trade_date(self, file_path: str) -> Optional[date]:
        try:
            df = pd.read_excel(file_path, header=None)
            for row in df.itertuples(index=False):
                for cell in row:
                    if isinstance(cell, str) and "Дата торгов:" in cell:
                        date_match = re.search(r"\d{2}\.\d{2}\.\d{4}", cell)
                        if date_match:
                            trade_date = datetime.strptime(date_match.group(), "%d.%m.%Y")
                            logging.info(f"Дата торгов успешно извлечена: {trade_date.date()}")
                            return trade_date.date()
            logging.error(f"Дата не найдена в файле {file_path}")
            return None
        except Exception as e:
            logging.error(f"Ошибка извлечения даты из файла {file_path}: {e}")
            return None

    def download_and_save_reports(self, report_links: List[str]) -> None:
        for i, url in enumerate(report_links, start=1):
            file_path = self.download_report(url, i)
            if file_path:
                report_date = self.extract_trade_date(file_path)
                if report_date and not self.repository.is_report_in_db(report_date):
                    self.save_report_to_db(file_path)
                else:
                    logging.info(f"Отчет за {report_date} уже существует в базе данных. Пропуск записи.")

    def download_report(self, url: str, index: int) -> Optional[str]:
        response = requests.get(url)
        if response.status_code == 200:
            temp_file_path = os.path.join(self.REPORTS_DIR, f"temp_report_{index}.xls")
            with open(temp_file_path, "wb") as file:
                file.write(response.content)

            report_date = self.extract_trade_date(temp_file_path)

            if report_date:
                final_file_path = os.path.join(self.REPORTS_DIR, f"{report_date}.xls")
                os.rename(temp_file_path, final_file_path)
                logging.info(f"Файл сохранен: {final_file_path}")
                return final_file_path
            else:
                os.remove(temp_file_path)
                logging.warning(f"Файл {temp_file_path} удален из-за отсутствия даты.")
                return None
        else:
            logging.error(f"Ошибка скачивания файла по ссылке {url}. Код ответа: {response.status_code}")
            return None

    def save_report_to_db(self, file_path: str) -> None:
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
            df.dropna(subset=['exchange_product_id', 'exchange_product_name', 'delivery_basis_id'], inplace=True)

            for _, row in df.iterrows():
                report_data = {
                    "exchange_product_id": row['exchange_product_id'],
                    "exchange_product_name": row['exchange_product_name'],
                    "oil_id": row['exchange_product_id'][:4] if isinstance(row['exchange_product_id'], str) else None,
                    "delivery_basis_id": row['delivery_basis_id'],
                    "delivery_basis_name": "",
                    "delivery_type_id": self.try_convert_to_float(row['delivery_type_id']),
                    "volume": self.try_convert_to_float(row['volume']),
                    "total": self.try_convert_to_float(row['total']),
                    "count": self.try_convert_to_int(row['count']),
                    "date": datetime.strptime(file_path.split("/")[-1].replace(".xls", ""), "%Y-%m-%d")
                }
                self.repository.save_report_data(report_data)
        except Exception as e:
            logging.error(f"Ошибка при сохранении данных из файла '{file_path}' в базу данных: {e}")

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
