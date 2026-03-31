import os
import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "crypto_db"),
        user=os.getenv("POSTGRES_USER", "crypto_user"),
        password=os.getenv("POSTGRES_PASSWORD", "crypto_pass")
    )


def check_row_count():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM raw_crypto_prices")
    count = cur.fetchone()[0]

    cur.close()
    conn.close()

    print(f"Row count: {count}")

    if count == 0:
        raise ValueError("No data found in raw_crypto_prices")


def check_null_prices():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM raw_crypto_prices
        WHERE price IS NULL
    """)
    null_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    print(f"Null price count: {null_count}")

    if null_count > 0:
        raise ValueError("Null values found in price column")


def run_quality_checks():
    print("Running data quality checks...")

    check_row_count()
    check_null_prices()

    print("All quality checks passed!")