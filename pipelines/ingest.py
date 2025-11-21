import requests
import json
from datetime import datetime
import os

def ingest():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    data = requests.get(url).json()

    os.makedirs("data/bronze", exist_ok=True)

    filename = f"data/bronze/btc_{datetime.now().strftime('%Y%m%d_%H')}.json"
    with open(filename, "w") as f:
        json.dump(data, f)

    print("✔ Dados coletados:", filename)

if __name__ == "__main__":
    ingest()
