import os
import duckdb

def metrics():
    os.makedirs("db", exist_ok=True)

    with duckdb.connect("db/crypto.db") as con:
        # Cria tabela gold_btc_metrics
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

        # Confirma quantas linhas foram geradas
        rows = con.execute("SELECT COUNT(*) FROM gold_btc_metrics").fetchone()[0]
        print(f"✔ Métricas geradas (gold layer) - {rows} linhas")

if __name__ == "__main__":
    metrics()