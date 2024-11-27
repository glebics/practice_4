from pydantic_settings import BaseSettings
from pydantic import PostgresDsn


class AsyncSettings(BaseSettings):
    db_name: str
    db_host: str
    db_port: str
    db_user: str
    db_pass: str

    class Config:
        env_file = ".env"
        case_sensitive = True

    @property
    def async_database_url(self) -> str:
        """
        Формирует URL для подключения к базе данных PostgreSQL.
        """
        return f"postgresql+asyncpg://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"


async_settings = AsyncSettings()
