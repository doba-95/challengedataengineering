import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


def transform_eur_to_usd(df_transaction, df_eur_usd):
    df_transaction.sort_values(by=["timestamp"], ascending=True, inplace=True)
    df_transaction.dropna(inplace=True)
    df_transaction["timestamp"] = df_transaction["timestamp"].dt.normalize()

    df_merged = pd.merge_asof(
        df_transaction,
        df_eur_usd,
        left_on="timestamp",
        right_on="date",
        allow_exact_matches=True,
        direction="backward",
    )
    df_merged["amount_usd"] = df_merged["amount"] * df_merged["rate"]
    df_merged.drop(["amount", "timestamp"], axis=1, inplace=True)
    return df_merged


def merge_transactions_with_conversion_products(df_transaction, df_products):
    return pd.merge(df_transaction, df_products, on="product_id", how="inner")


def calculate_profit(df):
    df["profit"] = df["amount_usd"] - df["production_costs"]
    return df


def print_best_worst_performing_product(parquet_files):
    datasets = ds.dataset("./csv_files/transactions/parquet_transactions/")
    columns_of_interest = [
        "product_id",
        "product_name",
        "amount_usd",
        "production_costs",
        "profit",
    ]
    for batch in datasets.to_batches(columns=columns_of_interest, batch_size=100000):
        batch_df = batch.to_pandas()
        print(batch_df.groupby(by="product_id").agg(
            {
                "amount_usd": "sum",
                "production_costs": "sum",
                "profit": "sum",
                "product_name": "first",
            }
        ).sort_values(by=["profit"], ascending=False).iloc[[0, -1]])