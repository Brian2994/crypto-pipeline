import duckdb
import os

def metrics():
    os.makedirs("db", exist_ok=True)
    con = duckdb.connect("db/crypto.db")

    con.execute("""
        CREATE OR REPLACE TABLE gold_btc_metrics AS
        SELECT
            date_trunc('day', ts) AS day,
            AVG(price_usd) AS avg_price,
            MIN(price_usd) AS min_price,
            MAX(price_usd) AS max_price
        FROM silver_prices
        GROUP BY 1
        ORDER BY 1;
    """)

    print("✔ Métricas geradas (gold layer)")

if __name__ == "__main__":
    metrics()
