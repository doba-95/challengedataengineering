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


def batch_reader_parquet_files(parquet_path):
    datasets = ds.dataset(parquet_path)
    columns_of_interest = [
        "product_id",
        "product_name",
        "amount_usd",
        "production_costs",
        "profit",
    ]
    for batch in datasets.to_batches(columns=columns_of_interest, batch_size=1000000, use_threads=True):
        batch_df = batch.to_pandas()
        yield aggregation_of_columns(batch_df)


def aggregation_of_columns(df):
    return_df = df.groupby(by="product_id").agg(
        {
            "product_id": "first",
            "amount_usd": "sum",
            "production_costs": "sum",
            "profit": "sum",
            "product_name": "first",
        }
    )
    return return_df