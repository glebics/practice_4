from sqlalchemy.orm import Session
from datetime import date
from typing import Dict, Any
from models import SpimexTradingResult  # Импорт модели


class Repository:
    def __init__(self, db_session: Session):
        self.db = db_session

    def is_report_in_db(self, report_date: date) -> bool:
        """
        Проверяет, существует ли отчет для указанной даты в базе данных.
        """
        return self.db.query(SpimexTradingResult).filter(SpimexTradingResult.date == report_date).first() is not None

    def save_report_data(self, report_data: Dict[str, Any]) -> None:
        """
        Сохраняет данные отчета в базе данных.
        """
        new_report = SpimexTradingResult(**report_data)
        self.db.add(new_report)
        self.db.commit()
