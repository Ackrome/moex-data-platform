# File: tests/test_ingestion.py
import pytest
from unittest.mock import patch, MagicMock
from src.ingestion.moex import download_chunk
from src.config import settings

# Фиктивные данные для теста
MOCK_RESPONSE_DATA = {
    "candles": {
        "columns": ["begin", "open", "close", "high", "low", "volume", "value", "end"],
        "data": [
            ["2023-01-01 10:00:00", 100, 105, 110, 99, 5000, 525000, "2023-01-01 10:01:00"],
            ["2023-01-01 10:01:00", 105, 102, 106, 101, 3000, 310000, "2023-01-01 10:02:00"],
        ],
    }
}


class TestMoexIngestion:
    @patch("src.ingestion.moex.minio_client")
    @patch("src.ingestion.moex.requests.get")
    def test_download_chunk_success(self, mock_get, mock_minio):
        """
        Тест успешной загрузки данных:
        1. Эмулируем ответ API (200 OK + JSON).
        2. Эмулируем отсутствие файла в MinIO (чтобы загрузка началась).
        3. Проверяем, что minio_client.save_json был вызван.
        """
        # Настройка моков
        mock_minio.exists.return_value = False

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = MOCK_RESPONSE_DATA
        # requests.get будет возвращать наш фиктивный ответ
        mock_get.return_value = mock_response

        # Запуск функции
        result = download_chunk("TEST_TICKER", 2023, 24)

        # Проверки
        assert "SUCCESS" in result
        assert "2 rows" in result

        # Проверяем, что сохранение было вызвано 1 раз
        mock_minio.save_json.assert_called_once()

        # Проверяем аргументы сохранения (правильность пути)
        args, _ = mock_minio.save_json.call_args
        data_arg, path_arg = args
        assert len(data_arg) == 2
        assert path_arg == "TEST_TICKER/1d/2023.json"

    @patch("src.ingestion.moex.minio_client")
    def test_skip_existing(self, mock_minio):
        """
        Тест пропуска, если файл уже есть в MinIO.
        """
        mock_minio.exists.return_value = True

        result = download_chunk("SBER", 2023, 24)

        assert "SKIP" in result
        mock_minio.save_json.assert_not_called()

    @patch("src.ingestion.moex.minio_client")
    @patch("src.ingestion.moex.requests.get")
    def test_api_error_handling(self, mock_get, mock_minio):
        """
        Тест обработки 404 или 500 ошибки от API.
        """
        mock_minio.exists.return_value = False

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_response.url = "http://moex.com/..."
        mock_get.return_value = mock_response

        result = download_chunk("BROKEN_TICKER", 2023, 24)

        assert "EMPTY" in result
        mock_minio.save_json.assert_not_called()
