import os

from pandas import DataFrame

from src.challenge.common.etl_job import ETLJob
from src.challenge.profit.data.data_extracter import (
    extract_transactions_csv,
    extract_eur_usd_rates_csv,
    extract_products_csv,
)
from src.challenge.profit.data.data_loader import write_to_parquet
from src.challenge.profit.data.data_transformer import (
    transform_eur_to_usd,
    merge_transactions_with_conversion_products,
    calculate_profit,
)


class ProfitEtl(ETLJob):
    def __init__(self):
        self.transactions = os.listdir("./csv_files/transactions")
        self.products_path = "./csv_files/products3.csv"
        self.eur_usd_rates_path = "./csv_files/eur_usd_last10y.csv"
        self.transactions_parquet_path = "./csv_files/transactions/transactions_parquet.parquet"

    def extract(self) -> tuple[DataFrame, DataFrame, DataFrame]:
        products = extract_products_csv(self.products_path)

        eur_usd_rates = extract_eur_usd_rates_csv(self.eur_usd_rates_path)

        transaction = extract_transactions_csv(
            os.path.join("./csv_files/transactions", self.transactions[0])
        )
        return products, eur_usd_rates, transaction

    def transform(self, products: DataFrame, transaction: DataFrame, eur_usd_rates: DataFrame) -> DataFrame:
        transaction_with_conversion_rate = transform_eur_to_usd(
            transaction, eur_usd_rates
        )

        merged_transactions_with_products = merge_transactions_with_conversion_products(
            transaction_with_conversion_rate, products
        )

        return calculate_profit(
            merged_transactions_with_products
        )

    def load(self, transactions_with_product_profit: DataFrame):
        write_to_parquet(transactions_with_product_profit, self.transactions_parquet_path)

    def run(self):
        products, eur_usd_rates, transaction = self.extract()
        transactions_with_product_profit = self.transform(products, transaction, eur_usd_rates)
        self.load(transactions_with_product_profit)