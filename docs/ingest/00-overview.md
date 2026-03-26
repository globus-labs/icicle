# Snapshot Pipelines

Converts periodic metadata exports (GUFI SQLite databases, GPFS TSV listings) into two [Globus Search](https://docs.globus.org/api/search/) indexes using Apache Flink on Amazon Managed Flink with MSK (Kafka) for inter-stage messaging.

## Pipelines

Three pipelines run in sequence (PRI → CNT → SND):

| Pipeline | Mode | Description |
|----------|------|-------------|
| **PRI** | Streaming | Ingests CSV rows into the primary index (per-file POSIX metadata) |
| **CNT** | Batch | Counts objects per user, group, and directory prefix |
| **SND** | Streaming | Merges quantile sketch statistics into aggregate index records |

## Metadata Indexes

### Primary Index

One record per file system object:

| Field | Code | Description |
|-------|------|-------------|
| `path` | [`filename`](../../src/ingest/helpers.py#L70) | Fully resolved absolute path; primary key uniquely identifying each object. |
| `type` | [`file_type`](../../src/ingest/helpers.py#L61) | Object type: regular file (`f`) or symbolic link (`l`). |
| `mode` | [`mode`](../../src/ingest/helpers.py#L66) / [`mode_first_char`](../../src/ingest/helpers.py#L65) | POSIX permission and mode bits (e.g., `-rw-r--r--`); enables permission-based filtering. |
| `uid` | [`user_id`](../../src/ingest/helpers.py#L63) | Numeric user ID of the owner; enables per-user queries. |
| `gid` | [`group_id`](../../src/ingest/helpers.py#L64) | Numeric group ID; supports group-level access and allocation analysis. |
| `size` | [`file_size`](../../src/ingest/helpers.py#L62) | Object size in bytes; used for storage consumption analysis. |
| `atime` | [`access_date`](../../src/ingest/helpers.py#L67) | Last access time; identifies cold data and archive candidates. |
| `ctime` | [`change_date`](../../src/ingest/helpers.py#L68) | Last metadata change time; supports audit and compliance queries. |
| `mtime` | [`mod_date`](../../src/ingest/helpers.py#L69) | Last content modification time; enables age-based lifecycle analysis. |
| `fileset` | [`fileset_name`](../../src/ingest/csv_schema.py#L32) | Containing fileset (GPFS only); supports fileset-scoped queries. |

Built by [`GetGmeta`](../../src/ingest/helpers.py) from CSV columns defined in [`CsvSchema`](../../src/ingest/csv_schema.py).

### Aggregate Index

Pre-computed summaries per **principal** (user, group, or directory prefix):

| Field | Code | Description |
|-------|------|-------------|
| principal | [`user_id::*`](../../src/ingest/aggregation.py#L114) / [`group_id::*`](../../src/ingest/aggregation.py#L115) / [`dir::*`](../../src/ingest/aggregation.py#L200) | Aggregation key identifying the scope of the summary: numeric user ID, numeric group ID, or directory path prefix. |
| `file_count` | [`file_count`](../../src/ingest/sketches.py#L93) | Total number of files within the aggregation scope; enables rapid object count queries without scanning the primary index. |
| `size_{*}` | [`file_size_*_in_bytes`](../../src/ingest/aggregation.py#L158) | Distributional statistics over file sizes; supports capacity planning, anomaly detection, and usage reporting. |
| `atime_{*}` | [`access_date_*`](../../src/ingest/aggregation.py#L159) | Distributional statistics over access times, enabling identification of active versus dormant data. |
| `ctime_{*}` | [`change_date_*`](../../src/ingest/aggregation.py#L165) | Distributional statistics over metadata change times, supporting audit and compliance queries. |
| `mtime_{*}` | [`mod_date_*`](../../src/ingest/aggregation.py#L171) | Distributional statistics over modification times, enabling age-based lifecycle analysis. |

`{*}` = average, 10p, 25p, median, 75p, 90p, 99p, min, max. `size` additionally includes total.

Built by [`SecondaryReduce`](../../src/ingest/aggregation.py) with quantile statistics computed in [`sketches.py`](../../src/ingest/sketches.py).

### Quantile Sketches

Four algorithms selectable via the `cmode` parameter, all producing the same output schema.
Backed by [DataDog/sketches-py](https://github.com/DataDog/sketches-py) (DDSketch) and [Apache/datasketches-python](https://github.com/apache/datasketches-python) (KLL, REQ, t-Digest):

| Algorithm | Config | Error Type |
|-----------|--------|------------|
| **DDSketch** | `dd` | Relative value error (default) |
| **KLL** | `kll` | Normalized rank error |
| **REQ** | `req` | Relative rank error |
| **t-Digest** | `td` | Adaptive, most accurate at tails |

See [Analysis Scripts](05-analysis-scripts.md) for accuracy evaluation.

## Apache Flink Version

Pinned to `apache-flink==1.20` per the [Amazon Managed Flink reference](https://github.com/aws-samples/amazon-managed-service-for-apache-flink-examples/tree/main/python/GettingStarted). PyFlink 1.20 supports Python 3.8-3.11 only, so **Python 3.11** is recommended. The `.[dev]` extra (which includes PyFlink) is **Linux-only**:

```bash
python3 -m venv .venv
source .venv/bin/activate

# macOS (development), no PyFlink
pip install -e '.[dev-core]'

# Linux (full), includes PyFlink
pip install -e '.[dev]'
```

## Guides

- [Preprocessing](01-preprocessing.md): raw snapshots → CSV
- [Infrastructure](02-infrastructure.md): MSK, S3, IAM setup
- [Flink Setup](03-flink-setup.md): local testing, CloudWatch
- [Flink Pipelines](04-flink-pipelines.md): compile, deploy, run PRI / CNT / SND
- [Analysis Scripts](05-analysis-scripts.md): sketch accuracy evaluation
