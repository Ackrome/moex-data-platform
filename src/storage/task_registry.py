# File: src/storage/task_registry.py
import redis
import json
import os


class TaskRegistry:
    def __init__(self):
        # Подключаемся к тому же Redis, что и Celery
        redis_url = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
        self.redis = redis.from_url(redis_url)
        self.KEY = "moex:tasks:registry"

    def add_task(self, task_id: str, ticker: str):
        """Регистрируем новую задачу"""
        data = {"id": task_id, "ticker": ticker, "progress": 0, "status": "Queued", "state": "PENDING"}
        self.save_task(task_id, data)

    def update_task(self, task_id: str, progress: int = None, status: str = None, state: str = None):
        """Обновляем статус"""
        data = self.get_task(task_id)
        if not data:
            return

        if progress is not None:
            data["progress"] = progress
        if status:
            data["status"] = status
        if state:
            data["state"] = state

        self.save_task(task_id, data)

    def save_task(self, task_id: str, data: dict):
        self.redis.hset(self.KEY, task_id, json.dumps(data))

    def get_task(self, task_id: str):
        raw = self.redis.hget(self.KEY, task_id)
        return json.loads(raw) if raw else None

    def get_all_tasks(self):
        """Возвращает список всех задач"""
        raw_map = self.redis.hgetall(self.KEY)
        tasks = [json.loads(v) for v in raw_map.values()]
        # Сортируем: сначала активные, потом завершенные
        return sorted(tasks, key=lambda x: x["state"] == "PENDING", reverse=True)

    def delete_task(self, task_id: str):
        self.redis.hdel(self.KEY, task_id)


task_registry = TaskRegistry()
