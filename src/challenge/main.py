import os
import time
from src.challenge.data.data_extracter import (
    extract_products_csv,
    extract_eur_usd_rates_csv,
    extract_transactions_csv,
    extract_parquet,
)
from src.challenge.data.data_loader import write_to_parquet
from src.challenge.data.data_transformer import (
    transform_eur_to_usd,
    merge_transactions_with_conversion_products,
    print_best_worst_performing_product,
    calculate_profit,
)

start = time.time()
transactions = os.listdir("./csv_files/transactions")
products_path = "./csv_files/products3.csv"
eur_usd_rates_path = "./csv_files/eur_usd_last10y.csv"
transactions_parquet_path = "./csv_files/transactions/transactions_parquet.parquet"

## Extract data

products = extract_products_csv(products_path)

eur_usd_rates = extract_eur_usd_rates_csv(eur_usd_rates_path)

transaction = extract_transactions_csv(os.path.join("./csv_files/transactions", transactions[0]))

## Transform data

transaction_with_conversion_rate = transform_eur_to_usd(transaction, eur_usd_rates)

merged_transactions_with_products = merge_transactions_with_conversion_products(
    transaction_with_conversion_rate, products
)

transactions_with_product_profit = calculate_profit(merged_transactions_with_products)


## Load data

write_to_parquet(transactions_with_product_profit, transactions_parquet_path)

## Print best/worst performing products

print_best_worst_performing_product(extract_parquet(transactions_parquet_path))

print(time.time() - start)