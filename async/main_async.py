# main_async.py
import logging
import asyncio
from database_async import async_engine, Base, AsyncSessionLocal
from service_async import SpimexServiceAsync
import time


async def run_async():
    """
    Основная асинхронная функция для запуска программы.
    """
    # Настройка логирования
    logging.basicConfig(level=logging.INFO)
    logging.info("Начало работы асинхронной программы.")

    # Создание таблиц в базе данных, если они еще не существуют
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logging.info(
        "Проверка таблиц и создание, если отсутствуют, завершена (async).")

    # Инициализация сервиса с передачей фабрики сессий
    service = SpimexServiceAsync(AsyncSessionLocal)

    # Получаем ссылки на отчеты, загружаем и сохраняем их
    months_limit = service.calculate_months_limit()
    logging.info(f"Количество месяцев для загрузки отчетов: {months_limit}")
    report_links = await service.fetch_report_links(months_limit)
    logging.info(f"Собрано {len(report_links)} ссылок на отчеты.")
    await service.download_and_save_reports(report_links)

    logging.info("Асинхронная программа завершена.")


if __name__ == "__main__":
    """
    Точка входа в программу.
    """
    start_time = time.monotonic()
    try:
        asyncio.run(run_async())
    except Exception as e:
        logging.error(f"Программа завершилась с ошибкой: {e}")
    finally:
        end_time = time.monotonic()
        duration = end_time - start_time
        # Запись времени выполнения в файл
        try:
            with open("async/execution_time.txt", "w") as f:
                f.write(
                    f"Время выполнения асинхронной программы: {duration:.2f} секунд\n")
            logging.info(
                f"Время выполнения асинхронной программы: {duration:.2f} секунд записано в execution_time.txt")
        except Exception as e:
            logging.error(f"Ошибка при записи времени выполнения: {e}")
        print(f'Время выполнения асинхронной программы: {duration:.2f} секунд')
