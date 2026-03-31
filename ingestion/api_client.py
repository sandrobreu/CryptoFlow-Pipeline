import requests
from datetime import datetime


def fetch_crypto_data(coin_ids=None, vs_currency="usd"):
    if coin_ids is None:
        coin_ids = ["bitcoin", "ethereum"]

    ids = ",".join(coin_ids)

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "ids": ids
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    extracted_at = datetime.utcnow()

    return data, extracted_at


if __name__ == "__main__":
    data, extracted_at = fetch_crypto_data()
    print(f"Extracted at: {extracted_at}")
    print(f"Number of records: {len(data)}")

    for record in data:
        print(
            record.get("id"),
            record.get("current_price"),
            record.get("market_cap"),
            record.get("total_volume")
        )