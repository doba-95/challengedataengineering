import multiprocessing
import os
import time

from src.challenge.common.generated_file_cleaner import remove_parquet_files
from src.challenge.profit.data.data_transformer import (
    print_best_worst_performing_product,
)
from src.challenge.profit.profit_etl import ProfitEtl

start = time.time()

TRANSACTIONS_DIR = "./csv_files/transactions"

transactions = [f for f in os.listdir(TRANSACTIONS_DIR) if f.endswith('.csv')]

def etl_runner(transaction: str):
    profit_etl = ProfitEtl(transaction)
    profit_etl.run()




if __name__ == "__main__":
    ## Run etl pipeline with multiple cores
    with multiprocessing.Pool(6) as pool:
        pool.map(etl_runner, transactions)

    ## Print best/worst performing products
    transactions_parquets = [f for f in os.listdir(TRANSACTIONS_DIR) if f.endswith('.parquet')]
    #with multiprocessing.Pool(6) as pool:
    print_best_worst_performing_product(transactions_parquets)


remove_parquet_files(TRANSACTIONS_DIR)
print(time.time() - start)