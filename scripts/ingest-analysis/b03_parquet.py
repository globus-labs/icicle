#!/usr/bin/env python3
from __future__ import annotations

import os

import pandas as pd
import pyarrow.parquet as pq


def parquet_to_csv(parquet_dir: str, out_csv: str) -> None:
    if not os.path.exists(parquet_dir):
        raise FileNotFoundError(
            f'Parquet directory does not exist: {parquet_dir}',
        )

    print(f'Reading Parquet from: {parquet_dir}')
    table = pq.ParquetDataset(parquet_dir).read()
    df = table.to_pandas()
    print('Loaded DataFrame:', df.shape)
    print('Columns:', df.columns.tolist())
    print(df.head())

    # Auto-detect id column: gid or uid
    if 'gid' in df.columns:
        id_col = 'gid'
    elif 'uid' in df.columns:
        id_col = 'uid'
    else:
        raise KeyError("Neither 'gid' nor 'uid' column found in Parquet data")

    # Ensure correct dtypes (keep values as integers for quantiles)
    df[id_col] = df[id_col].astype('int64')
    df['metric'] = df['metric'].astype(str)
    df['value'] = pd.to_numeric(df['value'], errors='coerce').astype('int64')

    # quantile function generator
    def q(p: float):
        return lambda x: x.quantile(p)

    stats = (
        df.groupby([id_col, 'metric'])['value']
        .agg(
            count='count',
            min='min',
            max='max',
            mean='mean',
            p10=q(0.10),
            p25=q(0.25),
            p50=q(0.50),
            p75=q(0.75),
            p90=q(0.90),
            p99=q(0.99),
        )
        .reset_index()
    )

    # reorder for readability
    stats = stats[
        [
            id_col,
            'metric',
            'count',
            'min',
            'p10',
            'p25',
            'p50',
            'p75',
            'p90',
            'p99',
            'max',
            'mean',
        ]
    ]
    stats = stats.sort_values(by=[id_col, 'metric']).reset_index(drop=True)
    # quantile-based stats remain integers; mean stays float
    for col in [
        'count',
        'min',
        'p10',
        'p25',
        'p50',
        'p75',
        'p90',
        'p99',
        'max',
    ]:
        stats[col] = stats[col].astype('int64')

    print('Resulting stats:', stats.shape)
    print(stats.head(10))

    out_dir = os.path.dirname(out_csv)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    stats.to_csv(out_csv, index=False)
    print(f'Saved CSV: {out_csv}')


if __name__ == '__main__':
    pass
    # PARQUET_DIR = "pyspark_out/itap/itap-uid-sorted-parquet"
    # OUT_CSV = "itap-uid-exact.csv"
    # parquet_to_csv(PARQUET_DIR, OUT_CSV)

    # PARQUET_DIR = "pyspark_out/itap/itap-gid-sorted-parquet"
    # OUT_CSV = "itap-gid-exact.csv"
    # parquet_to_csv(PARQUET_DIR, OUT_CSV)

    # PARQUET_DIR = "pyspark_out/nersc/nersc-uid-sorted-parquet"
    # OUT_CSV = "nersc-uid-exact.csv"
    # parquet_to_csv(PARQUET_DIR, OUT_CSV)

    # PARQUET_DIR = "pyspark_out/nersc/nersc-gid-sorted-parquet"
    # OUT_CSV = "nersc-gid-exact.csv"
    # parquet_to_csv(PARQUET_DIR, OUT_CSV)
