import logging
import os

import uuid
from typing import Iterator

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
    def __init__(self, transaction):
        self.transaction = transaction
        self.products_path = "./csv_files/products3.csv"
        self.eur_usd_rates_path = "./csv_files/eur_usd_last10y.csv"
        self.transactions_parquet_path = "./csv_files/transactions/"
        self.parquet_id = uuid.uuid4().hex

    def extract(self) -> tuple[DataFrame, DataFrame]:
        products = extract_products_csv(self.products_path)

        eur_usd_rates = extract_eur_usd_rates_csv(self.eur_usd_rates_path)

        return products, eur_usd_rates

    def transform(
        self, products: DataFrame, transaction: DataFrame, eur_usd_rates: DataFrame
    ) -> DataFrame:
        transaction_with_conversion_rate = transform_eur_to_usd(
            transaction, eur_usd_rates
        )

        merged_transactions_with_products = merge_transactions_with_conversion_products(
            transaction_with_conversion_rate, products
        )

        return calculate_profit(merged_transactions_with_products)

    def load(self, transactions_with_product_profit: DataFrame):
        write_to_parquet(
            transactions_with_product_profit,
            (
                self.transactions_parquet_path
                + "transactions_parquet_"
                + self.parquet_id
                + ".parquet"
            ),
        )

    def run(self) -> Iterator:
        products, eur_usd_rates= self.extract()
        counter = 0
        with extract_transactions_csv(os.path.join("./csv_files/transactions", self.transaction)) as reader:
            for transaction_chunk in reader:
                counter += 1
                logging.info("Parquet file %s at counter %s", self.parquet_id, counter)
                transaction_chunk_df = self.transform(
                        products, transaction_chunk, eur_usd_rates
                    )
                yield self.load(transaction_chunk_df)
