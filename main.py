import logging
from database import SessionLocal, Base  # Импортируем Base для создания таблиц
from repository import Repository
from service import SpimexService

def main() -> None:
    logging.info("Начало работы программы.")

    # Создаем таблицы в базе данных, если они еще не существуют
    Base.metadata.create_all(bind=SessionLocal().bind)
    logging.info("Проверка таблиц и создание, если отсутствуют, завершена.")

    db = SessionLocal()
    try:
        repository = Repository(db)
        service = SpimexService(repository)

        # Получаем ссылки на отчеты, загружаем и сохраняем их
        months_limit = service.calculate_months_limit()
        report_links = service.fetch_report_links(months_limit)
        service.download_and_save_reports(report_links)

    finally:
        db.close()
        logging.info("Завершение работы программы.")

if __name__ == "__main__":
    main()
