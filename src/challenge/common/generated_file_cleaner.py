import os


def remove_parquet_files(path):
    [os.remove(os.path.join(path, parquet_file)) for parquet_file in os.listdir(path) if parquet_file.endswith(".parquet")]