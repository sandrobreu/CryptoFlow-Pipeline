import os
import json
import psycopg2
from api_client import fetch_crypto_data


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "crypto_db"),
        user=os.getenv("POSTGRES_USER", "crypto_user"),
        password=os.getenv("POSTGRES_PASSWORD", "crypto_pass")
    )


def load_raw_data():
    data, extracted_at = fetch_crypto_data()

    conn = get_connection()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO raw_crypto_prices (
            source_name,
            coin_id,
            currency,
            price,
            market_cap,
            total_volume,
            api_timestamp,
            raw_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    """

    for record in data:
        cur.execute(
            insert_sql,
            (
                "coingecko",
                record.get("id"),
                "usd",
                record.get("current_price"),
                record.get("market_cap"),
                record.get("total_volume"),
                extracted_at,
                json.dumps(record)
            )
        )

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(data)} records into raw_crypto_prices")


if __name__ == "__main__":
    load_raw_data()