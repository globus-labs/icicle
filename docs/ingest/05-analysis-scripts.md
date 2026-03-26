# Analysis Scripts

Scripts in [`scripts/ingest-analysis/`](../../scripts/ingest-analysis/) evaluate the accuracy of sketch-based approximate statistics produced by the aggregate (SND) pipeline.

The SND pipeline uses streaming sketch algorithms (DDSketch, KLL, REQ, t-digest) to compute quantile estimates in bounded memory. These scripts compare the approximate results against exact quantiles computed offline, measuring how much accuracy is traded for speed and scalability.

## Workflow

The scripts run in a specific order — exact ground truth first, then pipeline results, then comparison:

```
b01_pyspark.py          Compute exact sorted values (Parquet)
       │
b03_parquet.py          Convert exact Parquet → CSV with quantile stats
       │
   (run SND pipeline)   Produce sketch-based approximations
       │
b02_octopus.py          Extract sketch results from Kafka → CSV
       │
  ┌────┴────┐
b04a        b04b        Compare approximate vs exact quantiles
(DDSketch)  (all algos)
```

## Scripts

| Script | Purpose |
|--------|---------|
| [`b01_pyspark.py`](../../scripts/ingest-analysis/b01_pyspark.py) | **Ground-truth computation.** Uses PySpark to read raw metadata CSVs, compute exact sorted values per user/group, and write Parquet files for rank-error comparison. |
| [`b02_octopus.py`](../../scripts/ingest-analysis/b02_octopus.py) | **Sketch result extraction.** Consumes a Kafka topic containing pipeline sketch results (DDSketch, KLL, REQ, or t-digest), exports per-user and per-group metric CSVs. |
| [`b03_parquet.py`](../../scripts/ingest-analysis/b03_parquet.py) | **Exact quantile computation.** Converts PySpark Parquet outputs into CSV with exact quantile statistics (p10–p99, min, max, mean). |
| [`b04a_analysis.ipynb`](../../scripts/ingest-analysis/b04a_analysis.ipynb) | **DDSketch accuracy evaluation.** Compares DDSketch approximations against exact quantiles on NERSC data; computes rank and value errors across multiple runs. |
| [`b04b_analysis.ipynb`](../../scripts/ingest-analysis/b04b_analysis.ipynb) | **Multi-algorithm comparison.** Evaluates DDSketch, KLL, REQ, and t-digest against exact quantiles on HPSS data; outputs error Parquet files. |
| [`plots.ipynb`](../../scripts/ingest-analysis/plots.ipynb) | **Misc visualization.** Ad-hoc plotting notebook (not part of the main analysis pipeline). |

## Datasets

Ground-truth computation ([`b01_pyspark.py`](../../scripts/ingest-analysis/b01_pyspark.py)) runs PySpark on a single EC2 instance:

| Name | Source | Size | Instance | Cores | RAM | Used by |
|------|--------|------|----------|-------|-----|---------|
| ITAP | Purdue ITAP filesystem snapshots | fs-small | m6a.32xlarge | 128 | 493 GB | b01 |
| NERSC | NERSC center filesystem data | fs-medium | m6a.32xlarge | 128 | 493 GB | b01, b04a |
| HPSS | HPSS tape system metadata | fs-large | m6a.48xlarge | 192 | 739 GB | b01, b02, b04b |

## Metrics

Each script operates on four POSIX metadata attributes: **file_size**, **atime** (access time), **ctime** (change time), and **mtime** (modification time). Error analysis computes:

- **Normalized rank error** — how far the approximate rank is from the true rank
- **Relative value error** — how far the approximate quantile value is from the exact value
