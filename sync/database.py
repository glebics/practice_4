import logging
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import settings  # Импортируем объект settings


# Настройка логирования
logging.basicConfig(level=logging.INFO)


# Создание движка базы данных с использованием URL из settings
DATABASE_URL = settings.database_url  # Используем database_url напрямую
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
logging.info("Движок базы данных успешно создан.")


# Соглашение по именованию индексов, ограничений и других элементов БД
naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

# Метаданные с настройками именования и схемой
metadata = MetaData(naming_convention=naming_convention)
# , schema="your_schema" - для упрощения не буду использовать другую схему


# Базовый класс для всех моделей с метаданными
Base = declarative_base(metadata=metadata)


# Создание сессии
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
logging.info("SessionLocal успешно создан.")


# Функция для получения сессии
def get_db():
    logging.info("Создание сессии базы данных.")
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logging.error(f"Ошибка во время работы с сессией базы данных: {e}")
        raise
    finally:
        db.close()
        logging.info("Сессия базы данных закрыта.")
