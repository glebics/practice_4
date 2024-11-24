from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import Dict, Any
from models_async import SpimexTradingResultAsync  # Импорт асинхронной модели
from datetime import date


class AsyncRepository:
    def __init__(self, db_session: AsyncSession):
        self.db = db_session

    async def is_report_in_db(self, report_date: date) -> bool:
        """
        Асинхронно проверяет, существует ли отчет для указанной даты в базе данных.
        """
        stmt = select(SpimexTradingResultAsync).where(
            SpimexTradingResultAsync.date == report_date)
        result = await self.db.execute(stmt)
        return result.scalars().first() is not None

    async def save_report_data(self, report_data: Dict[str, Any]) -> None:
        """
        Асинхронно сохраняет данные отчета в базу данных.
        """
        new_report = SpimexTradingResultAsync(**report_data)
        self.db.add(new_report)
        await self.db.commit()
