#!/bin/bash

echo "🚀 Starting System with MULTIPLE ISOLATED Workers..."

# Worker 1 (Обрабатывает задачи по одной, полностью изолирован)
# Очереди берет любые
nohup celery -A src.worker.tasks worker --loglevel=info --pool=solo -n worker1 > worker1.log 2>&1 &

# Worker 2 (Тоже изолирован, работает параллельно с первым)
nohup celery -A src.worker.tasks worker --loglevel=info --pool=solo -n worker2 > worker2.log 2>&1 &

# Хочешь еще больше мощности? Раскомментируй третьего:
# nohup celery -A src.worker.tasks worker --loglevel=info --pool=solo -n worker3 > worker3.log 2>&1 &

echo "🔌 Starting API..."
nohup uvicorn src.api.app:app --host 0.0.0.0 --port 8000 > api.log 2>&1 &

echo "⏳ Waiting for services..."
sleep 5


echo "🎨 Starting UI..."
python src/dashboard/app.py