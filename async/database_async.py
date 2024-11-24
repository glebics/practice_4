import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import MetaData
from async_config import async_settings  # Импорт настроек

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Соглашение по именованию индексов, ограничений и других элементов БД
naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

# Метаданные с настройками именования
metadata = MetaData(naming_convention=naming_convention)

# Базовый класс для всех моделей с метаданными
Base = declarative_base(metadata=metadata)

# Создание асинхронного движка базы данных
ASYNC_DATABASE_URL = async_settings.async_database_url

async_engine = create_async_engine(
    ASYNC_DATABASE_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,
)

logging.info("Асинхронный движок базы данных успешно создан.")

# Создание асинхронной сессии
AsyncSessionLocal = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)

logging.info("AsyncSessionLocal успешно создан.")


# Функция для получения асинхронной сессии
async def get_async_db():
    logging.info("Создание асинхронной сессии базы данных.")
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception as e:
            logging.error(
                f"Ошибка во время работы с асинхронной сессией базы данных: {e}")
            raise
        finally:
            await session.close()
            logging.info("Асинхронная сессия базы данных закрыта.")
