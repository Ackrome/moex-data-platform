# File: src/ingestion/moex.py
import requests
import time
import json
import random
import calendar  # <--- НОВОЕ: Для работы с датами месяца
import dask
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.storage.minio_client import minio_client

# Константы API
BASE_URL_SHARES = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json"
BASE_URL_INDEX = "https://iss.moex.com/iss/engines/stock/markets/index/boards/SNDX/securities/{ticker}/candles.json"

def get_robust_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        read=5, 
        connect=5, 
        backoff_factor=2, 
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (DataEngineer Student Project)' 
    })
    return session

def download_chunk(ticker: str, year: int, interval: int, month: int = None, is_index: bool = False) -> str:
    """
    Скачивает данные. 
    Если передан month, качает только этот месяц и сохраняет в подпапку.
    """
    base_url = BASE_URL_INDEX if is_index else BASE_URL_SHARES
    base_url = base_url.format(ticker=ticker)
    
    interval_name = "1m" if interval == 1 else "1d"
    
    # --- ЛОГИКА ДАТ И ПУТЕЙ ---
    if month:
        # Если качаем месяц: SBER/1m/2024/01.json
        _, last_day = calendar.monthrange(year, month)
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{year}-{month:02d}-{last_day}"
        s3_path = f"{ticker}/{interval_name}/{year}/{month:02d}.json"
        log_prefix = f"{ticker} {year}-{month:02d}"
    else:
        # Если качаем год (для дневок): SBER/1d/2024.json
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        s3_path = f"{ticker}/{interval_name}/{year}.json"
        log_prefix = f"{ticker} {year}"

    # Проверка наличия (Идемпотентность)
    if minio_client.exists(s3_path):
        return f"SKIP: {log_prefix} (Exists)"

    all_data = []
    start_index = 0
    session = get_robust_session()

    # print(f"🔄 START: {log_prefix} | {start_date} -> {end_date}")

    while True:
        params = {
            "from": start_date,
            "till": end_date,
            "start": start_index,
            "interval": interval
        }
        
        try:
            resp = session.get(base_url, params=params, timeout=20)
            
            if resp.status_code != 200:
                print(f"❌ HTTP {resp.status_code} on {log_prefix}")
                break
                
            data = resp.json()
            if 'candles' not in data:
                break
                
            rows = data['candles']['data']
            columns = data['candles']['columns']
            
            if not rows:
                break
                
            for row in rows:
                record = dict(zip(columns, row))
                all_data.append(record)
            
            # Если вернулось < 500, значит конец данных
            if len(rows) < 500:
                break
                
            start_index += len(rows)
            
            # Пауза, чтобы не дудосить (Jitter)
            time.sleep(0.3 + random.uniform(0.1, 0.3))
            
        except Exception as e:
            print(f"❌ Error on {log_prefix}: {e}")
            time.sleep(5) # Длинная пауза при ошибке
            break

    if all_data:
        # Сортировка по времени
        all_data.sort(key=lambda x: x.get('begin', ''))
        minio_client.save_json(all_data, s3_path)
        return f"SUCCESS: {log_prefix} ({len(all_data)} rows)"
    
    # Если данных нет (например, будущий месяц), не создаем файл
    return f"EMPTY: {log_prefix}"

@dask.delayed
def process_ticker_year(ticker: str, year: int, interval: int, month: int, is_index: bool):
    # Dask обертка теперь принимает month
    return download_chunk(ticker, year, interval, month, is_index)