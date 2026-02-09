import os

from typing import Iterator

from pandas import DataFrame
import pyarrow as pa
from src.challenge.common.etl_job import ETLJob
from src.challenge.common.utils import get_parquet_schema
from src.challenge.profit.data.data_extracter import (
    extract_transactions_csv,
    extract_eur_usd_rates_csv,
    extract_products_csv,
)
from src.challenge.profit.data.data_transformer import (
    transform_eur_to_usd,
    merge_transactions_with_conversion_products,
    calculate_profit,
)


class ProfitEtl(ETLJob):
    def __init__(
        self,
        transaction,
        products_path,
        eur_usd_rates_path,
        transaction_path,
        writer,
    ):
        self.transaction = transaction
        self.transaction_path = transaction_path
        self.products_path = products_path
        self.eur_usd_rates_path = eur_usd_rates_path
        self.writer = writer

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
        self.writer.write_batch(
            pa.record_batch(
                transactions_with_product_profit, schema=get_parquet_schema()
            )
        )

    def run(self) -> Iterator:
        products, eur_usd_rates = self.extract()

        with extract_transactions_csv(
            os.path.join(self.transaction_path, self.transaction)
        ) as reader:
            for transaction_chunk in reader:
                yield self.load(
                    self.transform(products, transaction_chunk, eur_usd_rates)
                )
