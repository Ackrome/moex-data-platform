# File: flows/transform_flow.py
from prefect import flow, task
from src.processing.spark_job import process_data
from typing import List

@task(name="Run PySpark Job")
def run_spark_job(tickers: List[str] = None):
    # Теперь мы передаем конкретный список тикеров
    # Это позволяет Spark обрабатывать только их, не блокируя всю базу
    process_data(tickers)

@flow(name="MOEX Transformation Bronze-Gold")
def transform_flow(tickers: List[str] = None):
    print(f"🔥 Starting Spark ETL for: {tickers or 'ALL'}")
    run_spark_job(tickers)

if __name__ == "__main__":
    transform_flow(["SBER"])