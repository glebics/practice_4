import logging
import asyncio
from database_async import async_engine, Base, AsyncSessionLocal
from repository_async import AsyncRepository
from service_async import SpimexServiceAsync
import time


async def run_async():
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
    report_links = await service.fetch_report_links(months_limit)
    await service.download_and_save_reports(report_links)

    logging.info("Асинхронная программа завершена.")

if __name__ == "__main__":
    start_time = time.monotonic()
    asyncio.run(run_async())
    end_time = time.monotonic()
    duration = end_time - start_time
    # Запись времени выполнения в файл
    with open("async/execution_time.txt", "w") as f:
        f.write(
            f"Время выполнения асинхронной программы: {duration:.2f} секунд\n")
    print(f'Время выполнения асинхронной программы: {duration:.2f} секунд')
