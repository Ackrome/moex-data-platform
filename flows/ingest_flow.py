# File: flows/ingest_flow.py
from prefect import flow, task
import dask
from dask.diagnostics import Callback
from datetime import datetime
from src.ingestion.moex import process_ticker_year
from src.storage.task_registry import task_registry

# --- Custom Dask Callback (RedisProgressBar) ---
class RedisProgressBar(Callback):
    def __init__(self, task_id: str, start_pct: int = 10, end_pct: int = 70):
        super().__init__()
        self.task_id = task_id
        self.start_pct = start_pct
        self.end_pct = end_pct
        self.range = end_pct - start_pct

    def _start_state(self, dsk, state):
        self._state = state

    def _posttask(self, key, result, dsk, state, worker_id):
        s = state
        ndone = len(s["finished"])
        ntasks = len(s["finished"]) + len(s["ready"]) + len(s["waiting"]) + len(s["running"])
        if ntasks > 0:
            relative_progress = ndone / ntasks
            total_progress = int(self.start_pct + (relative_progress * self.range))
            task_registry.update_task(
                self.task_id, 
                progress=total_progress,
                status=f"🌍 Loading: {ndone}/{ntasks} chunks..."
            )
    def _finish(self, dsk, state, errored): pass

# --- Updated Generator ---

@task(name="Generate Tasks")
def generate_download_tasks(tickers: list, years_back: int = 5):
    current_year = datetime.now().year
    start_year = current_year - years_back
    years = range(start_year, current_year + 1)
    
    tasks = []
    
    for ticker in tickers:
        t = ticker.upper().strip()
        is_index = (t == 'IMOEX')
        
        for year in years:
            # 1. ДНЕВКИ (1d): Качаем сразу год (один файл)
            # month=None
            tasks.append(process_ticker_year(t, year, 24, None, is_index))
            
            # 2. МИНУТКИ (1m): Качаем ТОЛЬКО последние 2 года, чтобы не сойти с ума
            # И разбиваем каждый год на 12 месяцев
            # if year >= current_year - 1:
            for month in range(1, 13):
                # Если месяц в будущем (например, сейчас март, а мы просим декабрь), 
                # скрипт вернет EMPTY, это нормально.
                if year == current_year and month > datetime.now().month:
                    continue
                    
                tasks.append(process_ticker_year(t, year, 1, month, is_index))
                    
    return tasks

@flow(name="MOEX Ingestion Bronze")
def ingest_flow(tickers: list, years_back: int, task_id: str = None):
    print(f"🚀 Starting Ingestion for: {tickers}")
    
    lazy_results = generate_download_tasks(tickers, years_back)
    
    if not lazy_results:
        print("⚠️ No tasks generated.")
        return

    print(f"📦 Created {len(lazy_results)} lazy tasks.")
    
    # Используем больше потоков, так как задачи стали меньше
    num_workers = 12 
    
    if task_id:
        with RedisProgressBar(task_id, start_pct=10, end_pct=70):
            results = dask.compute(*lazy_results, scheduler='threads', num_workers=num_workers)
    else:
        results = dask.compute(*lazy_results, scheduler='threads', num_workers=num_workers)
    
    success_cnt = sum(1 for r in results if "SUCCESS" in r)
    print(f"🏁 Flow finished. Processed: {len(results)}. Saved: {success_cnt}.")

if __name__ == "__main__":
    ingest_flow(['SBER'], 1)