# File: src/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    # MinIO
    MINIO_ENDPOINT: str = Field(..., alias="MINIO_ENDPOINT")
    MINIO_ACCESS_KEY: str = Field(..., alias="MINIO_ROOT_USER")
    MINIO_SECRET_KEY: str = Field(..., alias="MINIO_ROOT_PASSWORD")
    
    MINIO_BUCKET_RAW: str = "raw-data"       # Bronze
    MINIO_BUCKET_SILVER: str = "silver-data" # New! Silver Layer (Parquet)
    
    # Postgres
    POSTGRES_USER: str = Field("admin", alias="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field("adminpass", alias="POSTGRES_PASSWORD")
    POSTGRES_DB: str = Field("moex_dw", alias="POSTGRES_DB")
    POSTGRES_HOST: str = Field("postgres", alias="POSTGRES_HOST")
    POSTGRES_PORT: int = 5432

    # Spark
    SPARK_MASTER_URL: str = Field("spark://spark-master:7077", alias="SPARK_MASTER_URL")

    # Pydantic V2 Config
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()