"""
Distributed ETL Benchmark: Pandas vs Dask
Auto-generates dataset if not present
"""

import argparse
import logging
import os
import time
import psutil
import numpy as np
import pandas as pd
import dask.dataframe as dd

# -------------------------------------------------
# LOGGING CONFIG
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# UTILITIES
# -------------------------------------------------
def memory_usage_mb():
    return psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)

def timed(func):
    start = time.time()
    result = func()
    end = time.time()
    return result, round(end - start, 2)

# -------------------------------------------------
# DATA GENERATION
# -------------------------------------------------
def generate_dataset(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    logger.info(f"Generating CSV dataset with {rows:,} rows...")
    df = pd.DataFrame({
        "user_id": np.random.randint(1, 100_000, rows),
        "category": np.random.choice(["A", "B", "C", "D"], rows),
        "value": np.random.randn(rows) * 100,
        "timestamp": pd.date_range("2023-01-01", periods=rows, freq="s")
    })
    df.to_csv(path, index=False)
    logger.info(f"Dataset generated at {path}")

# -------------------------------------------------
# PIPELINES
# -------------------------------------------------
def pandas_pipeline(path):
    df = pd.read_csv(path)
    df = df[df["value"] > 0]
    return df.groupby("category")["value"].mean()

def dask_pipeline(path, blocksize):
    df = dd.read_csv(path, blocksize=blocksize)
    df = df[df["value"] > 0]
    return df.groupby("category")["value"].mean().compute()

# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main(args):

    # Auto-generate dataset if missing
    if not os.path.exists(args.data_path):
        logger.warning("Dataset not found. Auto-generating dataset...")
        generate_dataset(args.data_path, args.rows)

    logger.info("Starting ETL benchmark")
    logger.info(f"Dataset path: {args.data_path}")

    # Pandas
    logger.info("Running Pandas pipeline...")
    pandas_result, pandas_time = timed(
        lambda: pandas_pipeline(args.data_path)
    )
    pandas_mem = round(memory_usage_mb(), 2)

    # Dask
    logger.info("Running Dask pipeline...")
    dask_result, dask_time = timed(
        lambda: dask_pipeline(args.data_path, args.blocksize)
    )
    dask_mem = round(memory_usage_mb(), 2)

    # Results
    results = pd.DataFrame({
        "Framework": ["Pandas", "Dask"],
        "Execution Time (sec)": [pandas_time, dask_time],
        "Memory Usage (MB)": [pandas_mem, dask_mem]
    })

    print("\n=== Performance Comparison ===")
    print(results)

    print("\n=== Aggregation Output (Sample) ===")
    print(dask_result)

    logger.info("Benchmark completed successfully")

# -------------------------------------------------
# ENTRY POINT
# -------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Auto-generating ETL Benchmark: Pandas vs Dask"
    )

    parser.add_argument(
        "--data-path",
        type=str,
        default="data/large_dataset.csv"
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=1_000_000,
        help="Rows to generate if dataset is missing"
    )

    parser.add_argument(
        "--blocksize",
        type=str,
        default="128MB"
    )

    args = parser.parse_args()
    main(args)
