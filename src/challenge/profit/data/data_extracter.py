import pandas as pd


def extract_products_csv(path):
    return pd.read_csv(
        path,
        usecols=["product_id", "product_name", "production_costs"],
        dtype={"product_id": "float32", "production_costs": "float32"},
    )


def extract_eur_usd_rates_csv(path):
    df = pd.read_csv(
        path,
        index_col="date",
        header=None,
        names=["date", "rate"],
        skiprows=3,
        parse_dates=["date"],
    )
    return df.sort_values(by=["date"], ascending=True)


def extract_transactions_csv(path):
    df = pd.read_csv(
        path,
        parse_dates=["timestamp"],
        usecols=["timestamp", "product_id", "amount"],
        dtype={"product_id": "float32", "amount": "float32"},
        chunksize=1000000,
    )
    return df

