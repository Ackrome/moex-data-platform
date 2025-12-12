# File: src/worker/tasks.py
from celery import Celery
import os
import time
import requests
from flows.ingest_flow import ingest_flow
from flows.transform_flow import transform_flow
from src.storage.task_registry import task_registry

celery_app = Celery(
    "moex_worker",
    broker=os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0"),
)
# ВАЖНО: Возвращаем "fork" или "prefork", чтобы работала параллельность, 
# НО для Spark внутри контейнера безопаснее "threads" или "solo", если мало памяти.
# С новой логикой (append) можно попробовать "threads".
celery_app.conf.worker_pool = "threads" 
celery_app.conf.worker_concurrency = 4  # 4 одновременных задачи

def wait_for_prefect(api_url: str, timeout: int = 60):
    start_time = time.time()
    health_url = f"{api_url.rstrip('/api')}/health"
    while True:
        try:
            if requests.get(health_url, timeout=2).status_code == 200: return True
        except: pass
        if time.time() - start_time > timeout: return False
        time.sleep(2)

@celery_app.task(bind=True)
def run_etl_task(self, tickers: list, years_back: int):
    task_id = self.request.id
    print(f"👷 Worker picked up task {task_id} for {tickers}")
    task_registry.update_task(task_id, progress=1, status="🚀 Initializing...", state="RUNNING")
    
    prefect_url = os.getenv("PREFECT_API_URL", "http://prefect-server:4200/api")
    if not wait_for_prefect(prefect_url):
        task_registry.update_task(task_id, progress=100, status="❌ Prefect Timeout", state="FAILURE")
        return

    try:
        # 1. Ingestion
        ingest_flow(tickers, years_back, task_id=task_id)

        # 2. Processing (Только для этих тикеров!)
        task_registry.update_task(task_id, progress=75, status="🔥 Processing (Spark)...", state="RUNNING")
        transform_flow(tickers) # <-- Передаем список тикеров

        # 3. Done
        task_registry.update_task(task_id, progress=100, status="✅ Completed", state="SUCCESS")
        return "OK"
    except Exception as e:
        print(f"❌ Task failed: {e}")
        task_registry.update_task(task_id, progress=100, status=f"Error: {str(e)[:20]}", state="FAILURE")
        raise e