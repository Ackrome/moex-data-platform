# File: debug_moex.py
import requests
import json

# Тестовые параметры: Сбербанк, 2023 год
URL = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/SBER/candles.json"
PARAMS = {
    "from": "2023-01-01",
    "till": "2023-01-05",  # Берем всего 5 дней
    "interval": 24,  # Дневки
    "start": 0,
}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

print(f"📡 Testing connection to: {URL}")
try:
    resp = requests.get(URL, params=PARAMS, headers=HEADERS, timeout=10)
    print(f"Status Code: {resp.status_code}")

    if resp.status_code == 200:
        data = resp.json()
        print("Keys in JSON:", data.keys())

        if "candles" in data:
            rows = data["candles"]["data"]
            print(f"Rows received: {len(rows)}")
            if len(rows) > 0:
                print("First row sample:", rows[0])
            else:
                print("⚠️ Data array is empty!")
        else:
            print("❌ Key 'candles' missing in response!")
            print("Response snippet:", resp.text[:200])
    else:
        print("❌ HTTP Error.")
        print("Response:", resp.text[:500])

except Exception as e:
    print(f"❌ Exception: {e}")
