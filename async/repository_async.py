# repository_async.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import Dict, Any
from models_async import SpimexTradingResultAsync  # Импорт асинхронной модели
from datetime import date
import logging


class AsyncRepository:
    def __init__(self, db_session: AsyncSession):
        self.db = db_session

    async def is_report_in_db(self, report_date: date) -> bool:
        """
        Асинхронно проверяет, существует ли отчет для указанной даты в базе данных.

        :param report_date: Дата отчета для проверки.
        :return: True, если отчет существует, иначе False.
        """
        stmt = select(SpimexTradingResultAsync).where(
            SpimexTradingResultAsync.date == report_date)
        try:
            result = await self.db.execute(stmt)
            exists = result.scalars().first() is not None
            logging.debug(
                f"Проверка отчета за {report_date}: {'найден' if exists else 'не найден'}")
            return exists
        except Exception as e:
            logging.error(f"Ошибка при проверке отчета за {report_date}: {e}")
            raise

    async def add_report_data(self, report_data: Dict[str, Any]) -> None:
        """
        Асинхронно добавляет данные отчета в сессию базы данных.

        :param report_data: Данные отчета для добавления.
        """
        try:
            new_report = SpimexTradingResultAsync(**report_data)
            self.db.add(new_report)
            logging.debug(f"Добавлен новый отчет: {report_data}")
        except Exception as e:
            logging.error(f"Ошибка при добавлении отчета: {e}")
            raise
