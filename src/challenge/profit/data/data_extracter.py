import pandas as pd


def extract_products_csv(path):
    return pd.read_csv(path, usecols=["product_id", "product_name", "production_costs"])


def extract_eur_usd_rates_csv(path):
    df = pd.read_csv(
        path,
        index_col="date",
        header=None,
        names=["date", "rate"],
        skiprows=3,
        parse_dates=["date"],
    )
    return  df.sort_values(by=["date"], ascending=True)


def extract_transactions_csv(path):
    df =  pd.read_csv(
        path, parse_dates=["timestamp"], usecols=["timestamp", "product_id", "amount"]
    ).sort_values(by=["timestamp"], ascending=True)
    df.dropna(inplace=True)
    df["timestamp"] = df["timestamp"].dt.normalize()
    return df


def extract_parquet(path):
    parquet_path= "./csv_files/transactions/"
    df = pd.read_parquet((parquet_path + path))
    return df

