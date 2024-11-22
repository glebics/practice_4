from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.sql import func
from database import Base


class SpimexTradingResult(Base):
    __tablename__ = 'spimex_trading_results'
    __table_args__ = {
        "comment": "Таблица, содержащая результаты торгов СПбМТСБ (Санкт-Петербургской международной товарно-сырьевой биржи)"}

    pk_spimex_id = Column(Integer, primary_key=True, index=True,
                comment="Уникальный идентификатор записи") # название иземенено на pk_spimex_id,
                                                           # могут быть конфликты
    exchange_product_id = Column(
        String, nullable=False, comment="Идентификатор биржевого продукта")
    exchange_product_name = Column(
        String, nullable=True, comment="Название биржевого продукта")
    oil_id = Column(String, nullable=True, comment="Идентификатор типа нефти")
    delivery_basis_id = Column(
        String, nullable=True, comment="Идентификатор условия доставки")
    delivery_basis_name = Column(
        String, nullable=True, comment="Название условия доставки")
    delivery_type_id = Column(String, nullable=True,
                              comment="Идентификатор типа доставки")
    volume = Column(Float, nullable=True, comment="Объем торгового продукта")
    total = Column(Float, nullable=True, comment="Общая стоимость сделки")
    count = Column(Integer, nullable=True, comment="Количество сделок")
    date = Column(DateTime, nullable=False, default=func.now(),
                  comment="Дата проведения торгов")
    created_on = Column(DateTime, server_default=func.now(),
                        comment="Дата создания записи")
    updated_on = Column(DateTime, server_default=func.now(
    ), onupdate=func.now(), comment="Дата последнего обновления записи")
