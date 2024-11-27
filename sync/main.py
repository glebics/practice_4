import logging
from database import SessionLocal, Base  # Импортируем Base для создания таблиц
from repository import Repository
from service import SpimexService
import time


def run_sync():
    logging.basicConfig(level=logging.INFO)
    logging.info("Начало работы синхронной программы.")

    # Создаем таблицы в базе данных, если они еще не существуют
    Base.metadata.create_all(bind=SessionLocal().bind)
    logging.info(
        "Проверка таблиц и создание, если отсутствуют, завершена (sync).")

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
        logging.info("Синхронная программа завершена.")


if __name__ == "__main__":
    start_time = time.monotonic()
    run_sync()
    end_time = time.monotonic()
    duration = end_time - start_time
    # Запись времени выполнения в файл
    with open("sync/execution_time.txt", "w") as f:
        f.write(
            f"Время выполнения синхронной программы: {duration:.2f} секунд\n")
    print(f'Время выполнения синхронной программы: {duration:.2f} секунд')
