# File: src/storage/minio_client.py
import json
import s3fs
from src.config import settings

class MinioClient:
    def __init__(self):
        self.fs = s3fs.S3FileSystem(
            key=settings.MINIO_ACCESS_KEY,
            secret=settings.MINIO_SECRET_KEY,
            client_kwargs={'endpoint_url': settings.MINIO_ENDPOINT},
            use_listings_cache=False
        )
        # Инициализируем оба слоя
        self._ensure_bucket(settings.MINIO_BUCKET_RAW)
        self._ensure_bucket(settings.MINIO_BUCKET_SILVER)

    def _ensure_bucket(self, bucket_name: str):
        if not self.fs.exists(bucket_name):
            try:
                self.fs.mkdir(bucket_name)
            except Exception:
                pass 

    def save_json(self, data: list, path: str):
        full_path = f"{settings.MINIO_BUCKET_RAW}/{path}"
        with self.fs.open(full_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
    
    def exists(self, path: str) -> bool:
        full_path = f"{settings.MINIO_BUCKET_RAW}/{path}"
        return self.fs.exists(full_path)

    def list_downloaded_tickers(self) -> list:
        try:
            paths = self.fs.ls(settings.MINIO_BUCKET_RAW, detail=False)
            tickers = []
            for p in paths:
                name = p.split('/')[-1]
                if name.isupper() and len(name) >= 3:
                    tickers.append(name)
            return sorted(tickers)
        except Exception as e:
            print(f"Error listing MinIO: {e}")
            return []

minio_client = MinioClient()