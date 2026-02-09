import logging
import multiprocessing
import os
import time
from pathlib import Path
import tracemalloc

import pandas as pd
import pyarrow.parquet as pq
import uuid

from src.challenge.common.generated_file_cleaner import remove_parquet_files
from src.challenge.common.utils import get_parquet_schema
from src.challenge.profit.data.data_transformer import (
    batch_reader_parquet_files,
    aggregation_of_columns,
)
from src.challenge.profit.profit_etl import ProfitEtl

start = time.time()
BASE_DIR = Path(__file__).resolve().parent
TRANSACTIONS_DIR = BASE_DIR / "csv_files/transactions"
PRODUCTS_CSV = BASE_DIR / "csv_files/products3.csv"
EUR_USD_CSV = BASE_DIR / "csv_files/eur_usd_last10y.csv"
PARQUET_DIR = BASE_DIR / "csv_files/transactions/parquet_transactions"

transactions = [f for f in os.listdir(TRANSACTIONS_DIR) if f.endswith(".csv")]
memory_usage_first_part = []
def memroy_usage_printer(values: tuple[int, int]) -> float:
    a = values[0] / (1024**2)
    b = values[1] / (1024**2)
    print("Current memory usage: %s, Peak memory usage: %s" % (a, b))
    return a

def etl_runner(transaction: str):
    print("------------------------------------")
    print("Start processing transaction: %s!", transaction)
    print("Time used: %s", time.time() - start)
    memory_float = memroy_usage_printer(tracemalloc.get_traced_memory())
    memory_usage_first_part.append(memory_float)
    print("------------------------------------")
    parquet_file = "transactions_parquet_" + uuid.uuid4().hex + ".parquet"
    writer = pq.ParquetWriter(PARQUET_DIR / parquet_file, get_parquet_schema())
    profit_etl = ProfitEtl(
        transaction, PRODUCTS_CSV, EUR_USD_CSV, TRANSACTIONS_DIR, writer
    )
    processed_chunks = profit_etl.run()
    while True:
        try:
            next(processed_chunks)
        except StopIteration:
            break
    print("------------------------------------")
    print("Finished processing transaction: %s!", transaction)
    print("Average memory usage in first part: %s", sum(memory_usage_first_part) / len(memory_usage_first_part))
    print("Time used: %s", time.time() - start)
    memroy_usage_printer(tracemalloc.get_traced_memory())
    print("------------------------------------")
    writer.close()


if __name__ == "__main__":
    tracemalloc.start()
    logging.basicConfig(filename="main.log", level=logging.INFO)
    ## Run etl pipeline with multiple cores
    with multiprocessing.Pool(6) as pool:
        pool.map(etl_runner, transactions)
    print("------------------------------------")
    print("Writing of parquet files is done!")
    print("Time used: %s", time.time() - start)
    print("------------------------------------")

    logging.info("ETL Pipeline ran in: %s", time.time() - start)

    batch_df_generator = batch_reader_parquet_files(PARQUET_DIR)

    dfs = []
    memory_usage_second_part = []
    with multiprocessing.Pool(6) as pool:
        for df in pool.imap_unordered(aggregation_of_columns, batch_df_generator):
            dfs.append(df)
            #total_bytes = sum(df.memory_usage(deep=True).sum() for df in dfs)
            if tracemalloc.get_traced_memory()[0] > (50 * (1024**2)):
                print("Perform aggregation")
                print("Length of dfs: s%", len(dfs))
                agg_df = aggregation_of_columns(pd.concat(dfs, ignore_index=True))
                dfs.clear()
                dfs.append(agg_df)
                memory_float_part_two = memroy_usage_printer(tracemalloc.get_traced_memory())
                memory_usage_second_part.append(memory_float_part_two)

    print("------------------------------------")
    print("Generating of dfs is done!")
    print("Time used: %s", time.time() - start)
    print("Average memory usage in second part: %s", sum(memory_usage_second_part) / len(memory_usage_second_part))
    print("------------------------------------")

    result = aggregation_of_columns(pd.concat(dfs, ignore_index=True))
    memroy_usage_printer(tracemalloc.get_traced_memory())


    tracemalloc.stop()

    print(result.sort_values(by=["profit"], ascending=False).iloc[[0, -1]])
    print("------------------------------------")
    print("Pipeline is done!")
    print("Time used: %s", time.time() - start)
    print("------------------------------------")


remove_parquet_files(PARQUET_DIR)
logging.info("Pipeline done in: %s", time.time() - start)