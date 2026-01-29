import multiprocessing
import os
import time
from src.challenge.profit.data.data_extracter import (
    extract_parquet,
)
from src.challenge.profit.data.data_transformer import (
    print_best_worst_performing_product,
)
from src.challenge.profit.profit_etl import ProfitEtl

start = time.time()


transactions_parquet_path = "./csv_files/transactions/transactions_parquet.parquet"
# transactions = os.listdir("./csv_files/transactions")

pricesEtl = ProfitEtl()
pricesEtl.run()

#with multiprocessing.Pool() as pool:



## Print best/worst performing products

print_best_worst_performing_product(extract_parquet(transactions_parquet_path))

print(time.time() - start)