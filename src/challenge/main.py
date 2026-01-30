import logging
import multiprocessing
import os
import time
import pandas as pd
from src.challenge.common.generated_file_cleaner import remove_parquet_files
from src.challenge.profit.data.data_transformer import (
    batch_reader_parquet_files,
)
from src.challenge.profit.profit_etl import ProfitEtl

start = time.time()

TRANSACTIONS_DIR = "./csv_files/transactions"
PARQUET_DIR = "./csv_files/transactions/parquet_transactions"

transactions = [f for f in os.listdir(TRANSACTIONS_DIR) if f.endswith(".csv")]


def etl_runner(transaction: str):
    profit_etl = ProfitEtl(transaction)
    processed_chunks = profit_etl.run()
    while True:
        try:
            next(processed_chunks)
        except StopIteration:
            break


if __name__ == "__main__":
    logging.basicConfig(filename="challenge.log", level=logging.INFO)
    ## Run etl pipeline with multiple cores
    with multiprocessing.Pool(6) as pool:
        pool.map(etl_runner, transactions)

    ## Print best/worst performing products
    transactions_parquets = [
        f for f in os.listdir(TRANSACTIONS_DIR) if f.endswith(".parquet")
    ]
    # with multiprocessing.Pool(6) as pool:

    processed_batch = batch_reader_parquet_files(PARQUET_DIR)

    final_columns = ["product_id", "amount_usd", "production_costs", "profit", "product_name"]
    final_df = pd.DataFrame(columns=final_columns)

    # for batch_df in next(processed_batch):
    #     if batch_df is StopIteration:
    #         break
    batches = 0
    while True:
        try:
            batches += 1
            batch_df = next(processed_batch)
            final_df = pd.concat([final_df, batch_df])
            final_df.groupby(by="product_id").agg(
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


remove_parquet_files(PARQUET_DIR)
print(time.time() - start)