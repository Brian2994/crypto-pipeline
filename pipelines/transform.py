import duckdb
import glob
import json
from datetime import datetime
import os

def transform():
    os.makedirs("db", exist_ok=True)
    con = duckdb.connect("db/crypto.db")

    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_prices (
            ts TIMESTAMP,
            price_usd DOUBLE
        );
    """)

    for file in glob.glob("data/bronze/*.json"):
        with open(file) as f:
            data = json.load(f)

        timestamp_str = file.split("\\")[-1].replace("btc_", "").replace(".json", "")
        ts = datetime.strptime(timestamp_str, "%Y%m%d_%H")

        price = data["bitcoin"]["usd"]

        con.execute("INSERT INTO silver_prices VALUES (?, ?)", [ts, price])

    print("✔ Transformação concluída")

if __name__ == "__main__":
    transform()
