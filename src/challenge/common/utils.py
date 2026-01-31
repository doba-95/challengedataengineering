import pyarrow as pa

def get_parquet_schema():
    schema = pa.schema(
        [
            ("product_id", pa.float32()),
            ("amount_usd", pa.float32()),
            ("product_name", pa.large_string()),
            ("production_costs", pa.float32()),
            ('profit', pa.float32())
        ]
    )
    return schema