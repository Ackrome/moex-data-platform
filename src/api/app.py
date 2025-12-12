# File: src/api/app.py
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime
import asyncio
import json
import psycopg2
from psycopg2.extras import RealDictCursor

from src.config import settings
from src.storage.task_registry import task_registry
from src.worker.tasks import run_etl_task, celery_app
from src.storage.minio_client import minio_client
from src.api.auth import (
    Token, User, verify_password, create_access_token, 
    get_current_user, get_current_admin, get_password_hash # <--- Добавили импорт хеширования
)

app = FastAPI(title="MOEX Enterprise Analytics API")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, 
    allow_methods=["*"], allow_headers=["*"]
)

# --- Models ---
class StockMetric(BaseModel):
    ticker: str
    interval: str
    ts: datetime
    open: float
    close: float
    high: float
    low: float
    volume: float
    sma_20: Optional[float]
    rsi_14: Optional[float]

class IngestRequest(BaseModel):
    tickers: List[str]
    years_back: int = 3

class ChartModel(BaseModel):
    name: str
    code: str

class ChartResponse(BaseModel):
    id: int
    name: str
    code: str

# НОВАЯ МОДЕЛЬ
class UserCreate(BaseModel):
    username: str
    password: str

# --- Database ---
def get_db_connection():
    return psycopg2.connect(
        host=settings.POSTGRES_HOST, port=settings.POSTGRES_PORT,
        user=settings.POSTGRES_USER, password=settings.POSTGRES_PASSWORD,
        dbname=settings.POSTGRES_DB
    )

# --- AUTH ROUTES ---

@app.post("/register", status_code=201)
def register_user(user: UserCreate):
    """Регистрация нового пользователя (всегда role='user')"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Проверка на существование
            cur.execute("SELECT id FROM users WHERE username = %s", (user.username,))
            if cur.fetchone():
                raise HTTPException(status_code=400, detail="Username already registered")
            
            # Хеширование и вставка
            hashed_pw = get_password_hash(user.password)
            cur.execute(
                "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, 'user')",
                (user.username, hashed_pw)
            )
            conn.commit()
        return {"status": "created", "username": user.username}
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"Registration error: {e}")
        raise HTTPException(status_code=500, detail="Registration failed")
    finally:
        conn.close()

@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    conn = get_db_connection()
    user = None
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE username = %s", (form_data.username,))
            user = cur.fetchone()
    finally:
        conn.close()

    if not user or not verify_password(form_data.password, user['password_hash']):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token = create_access_token(data={"sub": user['username'], "role": user['role']})
    return {"access_token": access_token, "token_type": "bearer", "role": user['role'], "username": user['username']}

# --- USER CHART ROUTES ---
@app.post("/charts", response_model=Dict[str, str])
def save_chart(chart: ChartModel, current_user: User = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE username = %s", (current_user.username,))
            user_id = cur.fetchone()[0]
            query = """
                INSERT INTO user_charts (user_id, name, code) VALUES (%s, %s, %s)
                ON CONFLICT (user_id, name) DO UPDATE SET code = EXCLUDED.code
            """
            cur.execute(query, (user_id, chart.name, chart.code))
            conn.commit()
        return {"status": "saved", "name": chart.name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/charts", response_model=List[ChartResponse])
def get_my_charts(current_user: User = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT uc.id, uc.name, uc.code 
                FROM user_charts uc 
                JOIN users u ON uc.user_id = u.id 
                WHERE u.username = %s
                ORDER BY uc.created_at DESC
            """, (current_user.username,))
            return cur.fetchall()
    finally:
        conn.close()

# --- PUBLIC/PROTECTED DATA ENDPOINTS ---
@app.get("/tickers", response_model=List[str])
def get_tickers():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Проверяем существование таблицы
            cur.execute("SELECT to_regclass('public.stock_metrics')")
            if cur.fetchone()[0] is None:
                return []
            
            # Читаем тикеры. 
            # Внимание: если транзакция в Spark идет прямо сейчас, этот запрос подождет COMMIT.
            # Это хорошо! Мы не получим неполные данные.
            cur.execute("SELECT DISTINCT ticker FROM stock_metrics ORDER BY ticker")
            res = [r[0] for r in cur.fetchall()]
            return res
    except Exception as e:
        print(f"⚠️ API Error getting tickers: {e}")
        return []
    finally:
        conn.close()

@app.get("/availability/{ticker}")
def check_availability(ticker: str):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT interval FROM stock_metrics WHERE ticker = %s", (ticker.upper(),))
            existing = {r[0] for r in cur.fetchall()}
            return {"1d": "1d" in existing, "1m": "1m" in existing}
    except: return {"1d": False, "1m": False}
    finally: conn.close()

@app.get("/metrics/{ticker}", response_model=List[StockMetric])
def get_metrics(ticker: str, limit: int = 5000, interval: str = "1d"):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT * FROM (
                    SELECT ticker, interval, ts, open, high, low, close, volume, sma_20, rsi_14
                    FROM stock_metrics
                    WHERE ticker = %s AND interval = %s
                    ORDER BY ts DESC LIMIT %s
                ) sub ORDER BY ts ASC
            """
            cur.execute(query, (ticker.upper(), interval, limit))
            return cur.fetchall() or []
    except: return []
    finally: conn.close()

# --- ADMIN ENDPOINTS ---
@app.post("/etl/run")
def trigger_etl(request: IngestRequest, admin: User = Depends(get_current_admin)):
    task = run_etl_task.delay(request.tickers, request.years_back)
    task_registry.add_task(task.id, ", ".join(request.tickers))
    task_registry.update_task(task.id, status="⏳ Queued...", progress=0, state="PENDING")
    return {"task_id": task.id}

@app.post("/etl/cancel/{task_id}")
def cancel_etl(task_id: str, admin: User = Depends(get_current_admin)):
    celery_app.control.revoke(task_id, terminate=True)
    task_registry.update_task(task_id, status="⛔ Cancelled", state="REVOKED", progress=100)
    return {"status": "cancelled"}

@app.post("/etl/resync")
def resync_data(admin: User = Depends(get_current_admin)):
    # 1. Сначала идем в MinIO и смотрим, какие тикеры там РЕАЛЬНО есть
    tickers = minio_client.list_downloaded_tickers()
    
    if not tickers: 
        return {"status": "error", "detail": "No data in MinIO to resync"}
    
    # 2. Запускаем задачу. 
    # ВАЖНО: Мы передаем СПИСОК тикеров, а не None. 
    # Это значит, что Spark удалит только их, а не сделает DELETE ALL.
    task = run_etl_task.delay(tickers, 3)
    
    task_registry.add_task(task.id, f"RESYNC: {len(tickers)} tickers")
    task_registry.update_task(task.id, status=f"♻️ Queued {len(tickers)} tickers", progress=0, state="PENDING")
    
    return {"task_id": task.id, "tickers_count": len(tickers)}
# --- WebSocket ---
@app.websocket("/ws/tasks")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            tasks = task_registry.get_all_tasks()
            await websocket.send_json(tasks)
            await asyncio.sleep(0.5)
    except: pass

# File: src/api/app.py (Добавь этот кусок)

@app.get("/tasks")
def get_tasks_list():
    """Возвращает список всех задач из Redis"""
    return task_registry.get_all_tasks()