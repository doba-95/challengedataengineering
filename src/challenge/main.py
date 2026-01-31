import logging
import multiprocessing
import os
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import uuid

from src.challenge.common.generated_file_cleaner import remove_parquet_files
from src.challenge.common.utils import get_parquet_schema
from src.challenge.profit.data.data_transformer import (
    batch_reader_parquet_files,
)
from src.challenge.profit.profit_etl import ProfitEtl

start = time.time()
BASE_DIR = Path(__file__).resolve().parent
TRANSACTIONS_DIR = BASE_DIR / "csv_files/transactions"
PRODUCTS_CSV = BASE_DIR / "csv_files/products3.csv"
EUR_USD_CSV = BASE_DIR / "csv_files/eur_usd_last10y.csv"
PARQUET_DIR = BASE_DIR / "csv_files/transactions/parquet_transactions"

transactions = [f for f in os.listdir(TRANSACTIONS_DIR) if f.endswith(".csv")]


def etl_runner(transaction: str):
    parquet_file = "transactions_parquet_" + uuid.uuid4().hex + ".parquet"
    writer = pq.ParquetWriter(
        PARQUET_DIR / parquet_file, get_parquet_schema()
    )
    profit_etl = ProfitEtl(transaction, PRODUCTS_CSV, EUR_USD_CSV, TRANSACTIONS_DIR, writer)
    processed_chunks = profit_etl.run()
    while True:
        try:
            next(processed_chunks)
        except StopIteration:
            break
    writer.close()


if __name__ == "__main__":
    logging.basicConfig(filename="challenge.log", level=logging.INFO)
    ## Run etl pipeline with multiple cores
    with multiprocessing.Pool(6) as pool:
        pool.map(etl_runner, transactions)

    print(time.time() - start)

    batch_df_generator = batch_reader_parquet_files(PARQUET_DIR)

    final_columns = ["product_id", "amount_usd", "production_costs", "profit", "product_name"]
    final_df = pd.DataFrame(columns=final_columns)


    while True:
        try:
            batch_df = next(batch_df_generator)
            final_df = pd.concat([final_df, batch_df])
            final_df = final_df.groupby(by="product_id", as_index=False).agg(
                {
                    "product_id": "first",
                    "amount_usd": "sum",
                    "production_costs": "sum",
                    "profit": "sum",
                    "product_name": "first",
                }
            )
        except StopIteration:
            break

    print(final_df.sort_values(by=["profit"], ascending=False).iloc[[0,-1]])


#remove_parquet_files(PARQUET_DIR)
print(time.time() - start)