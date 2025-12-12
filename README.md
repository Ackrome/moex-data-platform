<div align="center" style="border: none; padding: 0; margin: 0;">
  <h1>📈 MOEX Enterprise Data Platform</h1>
  <strong>Industrial-grade Data Engineering Solution: A scalable End-to-End pipeline for Russian Stock Market analytics.</strong>
  <br>
  </br>
  <p align="center">
    <img src="https://img.shields.io/badge/Python-3.11%2B-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
    <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark">
    <img src="https://img.shields.io/badge/Dask-FD9A00?style=for-the-badge&logo=dask&logoColor=white" alt="Dask">
    <img src="https://img.shields.io/badge/Prefect-070E3A?style=for-the-badge&logo=prefect&logoColor=white" alt="Prefect">
    <img src="https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white" alt="MinIO">
    <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL">
    <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker">
    <img src="https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI">
    <img src="https://img.shields.io/badge/Plotly_Dash-3F4F75?style=for-the-badge&logo=plotly&logoColor=white" alt="Dash">
    <img src="https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white" alt="Redis">
  </p>
  
  <h3>Real-time Analytics • Medallion Architecture • Atomic Updates</h3>
</div>

## 🚀 Обзор Проекта

Этот проект представляет собой **End-to-End платформу данных** уровня Enterprise для анализа торговой активности Московской Биржи (MOEX). Система реализует полный цикл обработки данных: от асинхронного сбора (Ingestion) до визуализации финансовых метрик, используя современные архитектурные паттерны Big Data.

Система построена на принципах **Clean Architecture** и **Medallion Architecture** (Bronze → Silver → Gold), обеспечивая надежность, масштабируемость и чистоту данных.

### Компоненты системы:

1.  **Ingestion Engine (Dask)**: Асинхронный параллельный сборщик данных. Использует распределенные вычисления для эффективной обработки тысяч HTTP-запросов к API биржи.
2.  **Processing Core (Apache Spark)**: Мощный движок трансформации. Отвечает за очистку данных, приведение типов и расчет сложных финансовых индикаторов (RSI, SMA) с использованием оконных функций.
3.  **Storage Layer (Data Lake & Warehouse)**: Гибридное хранилище. **MinIO (S3)** для сырых данных и **PostgreSQL** для витрин данных.
4.  **Orchestration (Prefect)**: Управление потоками данных, мониторинг задач и автоматический перезапуск при сбоях.
5.  **Analytics UI (Dash + FastAPI)**: Интерактивный веб-интерфейс для трейдеров и аналитиков с возможностью запуска пользовательского Python-кода (Sandbox).

---

## 🏗 Архитектура и Data Flow

Мы используем гибридный вычислительный подход: **Dask** для IO-bound задач (сеть) и **Spark** для CPU-bound задач (математика).

```mermaid
graph LR
    subgraph "External Source"
        MOEX[MOEX ISS API]
    end

    subgraph "Ingestion Layer (IO-Bound)"
        Dask[Dask Workers]
        Prefect[Prefect Flow]
    end

    subgraph "Storage Layer (Data Lake)"
        Bronze[(MinIO: Bronze<br>Raw JSON)]
        Silver[(MinIO: Silver<br>Parquet)]
    end

    subgraph "Processing Layer (CPU-Bound)"
        Spark[Apache Spark 4.0]
        SQL[Spark SQL / JDBC]
    end

    subgraph "Serving Layer (Data Warehouse)"
        Gold[(PostgreSQL: Gold<br>Star Schema)]
    end

    subgraph "Consumer Layer"
        API[FastAPI Gateway]
        Dash[Analytics Dashboard]
    end

    MOEX -->|Async HTTP| Dask
    Prefect -->|Orchestrates| Dask
    Dask -->|Writes| Bronze
    
    Bronze -->|Read/Clean| Spark
    Spark -->|Optimize| Silver
    Silver -->|Aggregates| Spark
    Spark -->|Atomic Swap| Gold
    
    Gold -->|SQL Query| API
    API -->|WebSocket/JSON| Dash
```

## ✨ Ключевые возможности

### 🤖 Intelligent Ingestion
*   **Smart Backfill**: Автоматическая загрузка исторических данных (глубина до 2010 года).
*   **Идемпотентность**: Система "знает", какие данные уже загружены, и пропускает их, экономя трафик и время.
*   **Parallel Fetching**: Использование `dask.delayed` позволяет утилизировать сетевой канал на 100%.

### 🧪 Advanced Analytics (Spark)
*   **Технический анализ**: Автоматический расчет RSI (Relative Strength Index) и SMA (Simple Moving Average) на кластере Spark.
*   **Schema Enforcement**: Строгая типизация данных при переходе из Bronze (JSON) в Silver (Parquet).
*   **Parquet Optimization**: Данные хранятся в колоночном формате с Snappy сжатием для ускорения чтения.

### 🛡️ Enterprise Engineering
*   **Atomic Data Swaps**: Обновление данных в PostgreSQL происходит через транзакционный механизм (Staging Table -> Delete Old -> Insert New -> Drop Staging). Это гарантирует **Zero Downtime** — пользователь никогда не увидит пустые графики во время ETL процесса.
*   **RBAC Security**: Ролевая модель доступа (Admin/User). Только администраторы могут запускать тяжелые ETL-процессы.
*   **Docker Isolation**: Каждый сервис (даже Spark Master/Worker) работает в изолированном контейнере.

### 📊 Visualization & Sandbox
*   **Интерактивные графики**: Candlestick charts, Volume bars, RSI indicators.
*   **Python Sandbox**: Встроенная "песочница", позволяющая аналитикам писать свой код на Python прямо в браузере для анализа загруженных данных (pandas/numpy/plotly).

---

## 🛠️ Технический стек

| Категория | Технологии | Назначение |
| :--- | :--- | :--- |
| **Compute** | ![Spark](https://img.shields.io/badge/-Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white) ![Dask](https://img.shields.io/badge/-Dask-FD9A00?style=flat-square&logo=dask&logoColor=white) | Распределенная обработка данных |
| **Storage** | ![MinIO](https://img.shields.io/badge/-MinIO-C72E49?style=flat-square&logo=minio&logoColor=white) ![Postgres](https://img.shields.io/badge/-PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white) | Объектное хранилище и реляционная БД |
| **Backend** | ![FastAPI](https://img.shields.io/badge/-FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white) ![Pydantic](https://img.shields.io/badge/-Pydantic-E92063?style=flat-square&logo=pydantic&logoColor=white) | REST API и валидация конфигурации |
| **Frontend** | ![Dash](https://img.shields.io/badge/-Plotly%20Dash-3F4F75?style=flat-square&logo=plotly&logoColor=white) ![Bootstrap](https://img.shields.io/badge/-Bootstrap-7952B3?style=flat-square&logo=bootstrap&logoColor=white) | UI/UX |
| **Ops** | ![Docker](https://img.shields.io/badge/-Docker-2496ED?style=flat-square&logo=docker&logoColor=white) ![Prefect](https://img.shields.io/badge/-Prefect-070E3A?style=flat-square&logo=prefect&logoColor=white) | Контейнеризация и оркестрация |

---

## ⚙️ Установка и Запуск

Проект полностью контейнеризирован. Вам потребуется только **Docker** и **Docker Compose**.

### 1. Клонирование репозитория
```bash
git clone https://github.com/your-username/moex-data-platform.git
cd moex-data-platform
```

### 2. Настройка окружения
Проект уже содержит файл `.env` с настройками по умолчанию для локальной разработки.
*(Опционально)* Отредактируйте `.env`, если конфликтуют порты.

### 3. Запуск (Build & Run)
Сборка кастомного образа Spark (включает Java 17 и все JAR-зависимости) и запуск сервисов:

```bash
docker-compose up -d --build
```
> ⏳ **Примечание:** Первый запуск может занять 5-10 минут, так как происходит скачивание дистрибутива Spark и компиляция базового образа.

### 4. Доступ к интерфейсам

| Сервис | URL | Описание | Креды (если есть) |
| :--- | :--- | :--- | :--- |
| **Dashboard** | `http://localhost:8050` | Основной UI аналитики | `admin` / `admin` |
| **API Docs** | `http://localhost:8000/docs` | Swagger UI | - |
| **Prefect UI** | `http://localhost:4200` | Оркестратор задач | - |
| **MinIO** | `http://localhost:9001` | S3 Браузер | `minioadmin` / `minioadmin` |
| **Spark Master** | `http://localhost:8080` | Состояние кластера | - |

---

## 📚 Сценарии использования (User Guide)

### 1. Инициализация системы
При первом старте база данных пуста. Скрипт миграции запустится автоматически, но вы можете сбросить состояние вручную:
```bash
docker exec -it moex_etl_runner python src/create_tables.py
```

### 2. Запуск пайплайна (ETL)
1.  Откройте **Dashboard** (`localhost:8050`) и войдите как `admin`.
2.  В панели **Data Ingestion** введите тикер (например, `SBER`, `GAZP` или `IMOEX`).
3.  Нажмите **Queue Task**.
4.  Наблюдайте за прогрессом в реальном времени через виджет задач (WebSocket).

### 3. Анализ данных
1.  После завершения задачи (Status: `SUCCESS`), выберите тикер в выпадающем списке.
2.  Переключайтесь между **Daily** (дневки) и **Minute** (минутки).
3.  Перейдите в раздел **Custom Analytics Sandbox**, выберите пресет "Advanced: Feature Engineering" и нажмите **Run Analysis**, чтобы увидеть гистограмму распределения волатильности.

---

## 📂 Структура проекта

```text
.
├── docker/                 # 🐳 Инфраструктура
│   ├── Dockerfile.etl      # Multi-stage build для Spark+Python
│   └── init.sql            # Схема БД Postgres
├── flows/                  # 🌪️ Prefect Оркестрация
│   ├── ingest_flow.py      # Dask: Загрузка данных
│   └── transform_flow.py   # Spark: Обработка данных
├── src/                    # 🧠 Исходный код
│   ├── api/                # FastAPI приложение
│   ├── dashboard/          # Dash приложение
│   ├── ingestion/          # Логика работы с MOEX API
│   ├── processing/         # Spark Jobs (PySpark)
│   ├── storage/            # Адаптеры MinIO/Redis
│   └── worker/             # Celery Tasks
├── requirements.txt        # Python зависимости
└── docker-compose.yml      # Описание сервисов
```

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.
