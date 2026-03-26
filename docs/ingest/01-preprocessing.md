# Preprocessing

Scripts in [`scripts/ingest-preprocessing/`](../../scripts/ingest-preprocessing/) convert raw metadata exports into batched CSVs for the ingest pipeline. Each dataset has its own script due to differing source formats (GUFI SQLite vs GPFS TSV). After conversion, CSVs are uploaded to S3.

## 1. Collect db.db stats ([00_collect_dbdb.sh](../../scripts/ingest-preprocessing/00_collect_dbdb.sh))

Scan a directory tree for `db.db` files and write `path,size` stats to a text file.

```bash
# Usage: 00_collect_dbdb.sh <output_stats_txt> <search_root_dir>

# Interactive run
./scripts/ingest-preprocessing/00_collect_dbdb.sh stats.txt ~/globus/itap

# Long-running job
nohup ./scripts/ingest-preprocessing/00_collect_dbdb.sh stats.txt ~/globus/hpss &
```

## 2. Process fs-small db.db ([01_process_db.py](../../scripts/ingest-preprocessing/01_process_db.py))

Read SQLite databases listed in the stats file and export rows to batched CSVs.
Replace `<ROWS>` with the desired batch size (1k, 10k, 100k, or 1m).

```bash
nohup python -u scripts/ingest-preprocessing/01_process_db.py \
  --input-dbdb "./scripts/ingest-preprocessing/dbdb-fs-small.txt" \
  --rows-per-csv <ROWS> \
  --outfile-path "./csv/fs-small-<ROWS>" \
  --filename-prefix "/home/ubuntu/globus" \
  --filename-suffix "/db.db" \
  > nohup_fs-small.log 2>&1 &
```

## 3. Process fs-medium mmapplypolicy listing ([02_preprocess_mmapplypolicy.py](../../scripts/ingest-preprocessing/02_preprocess_mmapplypolicy.py))

Parse an mmapplypolicy listing, coerce columns, and export to batched CSVs.
Replace `<ROWS>` with the desired batch size.

```bash
nohup python -u scripts/ingest-preprocessing/02_preprocess_mmapplypolicy.py \
  --input-tsv "/home/ubuntu/globus/nersc/2016-04-15/mmapplypolicy.4677.1CC95AB7.list.allfiles" \
  --rows-per-csv <ROWS> \
  --outfile-path "./csv/fs-medium-<ROWS>" \
  --filename-prefix "/projectb/.snapshots/2016-04-15" \
  > nohup_fs-medium.log 2>&1 &
```

## 4. Process fs-large db.db ([03_process_db_cont.py](../../scripts/ingest-preprocessing/03_process_db_cont.py))

Continuation-aware variant of step 2 for large datasets. Automatically writes and resumes
from a progress file (`dbdb-fs-large_rows<ROWS>.progress`).
Replace `<ROWS>` with the desired batch size.

```bash
nohup python -u scripts/ingest-preprocessing/03_process_db_cont.py \
  --input-dbdb "./scripts/ingest-preprocessing/dbdb-fs-large.txt" \
  --rows-per-csv <ROWS> \
  --outfile-path "./csv/fs-large-<ROWS>" \
  --filename-prefix "/home/ubuntu/globus" \
  --filename-suffix "/db.db" \
  >> nohup_fs-large.log 2>&1 &
```

## 5. S3 Upload

After preprocessing, upload the CSV folders to S3. Create three buckets
(one per dataset) and sync each output folder.

> **Note:** S3 bucket names are globally unique. The names below
> (`icicle-fs-small`, etc.) are for reference — choose your own bucket names
> and update `s3_input` in `application_properties.json` and the dataset
> aliases in [`conf.py`](../../src/ingest/conf.py) accordingly.

```bash
# Create buckets (one-time)
aws s3 mb s3://icicle-fs-small
aws s3 mb s3://icicle-fs-medium
aws s3 mb s3://icicle-fs-large

# Upload fs-small (ITAP) CSVs
aws s3 sync ./csv/fs-small-1k   s3://icicle-fs-small/fs-small-1k/
aws s3 sync ./csv/fs-small-10k  s3://icicle-fs-small/fs-small-10k/
aws s3 sync ./csv/fs-small-100k s3://icicle-fs-small/fs-small-100k/
aws s3 sync ./csv/fs-small-1m   s3://icicle-fs-small/fs-small-1m/

# Upload fs-medium (NERSC) CSVs
aws s3 sync ./csv/fs-medium-1k   s3://icicle-fs-medium/fs-medium-1k/
aws s3 sync ./csv/fs-medium-10k  s3://icicle-fs-medium/fs-medium-10k/
aws s3 sync ./csv/fs-medium-100k s3://icicle-fs-medium/fs-medium-100k/
aws s3 sync ./csv/fs-medium-1m   s3://icicle-fs-medium/fs-medium-1m/

# Upload fs-large (HPSS) CSVs
aws s3 sync ./csv/fs-large-1k   s3://icicle-fs-large/fs-large-1k/
aws s3 sync ./csv/fs-large-10k  s3://icicle-fs-large/fs-large-10k/
aws s3 sync ./csv/fs-large-100k s3://icicle-fs-large/fs-large-100k/
aws s3 sync ./csv/fs-large-1m   s3://icicle-fs-large/fs-large-1m/
```

The resulting S3 layout is used by the ingest pipeline via `s3_input` in
`application_properties.json` (e.g., `s3a://icicle-fs-small/fs-small-10k`).

## 6. Reference outputs

Row counts and CSV file counts from prior runs (for sanity-checking new jobs).

| Dataset        | Total rows     | CSV files |
|----------------|----------------|-----------|
| fs-small-1k    | 8,456,326      | 5,976     |
| fs-small-10k   | 8,456,326      | 674       |
| fs-small-100k  | 8,456,326      | 78        |
| fs-small-1m    | 8,456,326      | 9         |
| fs-medium-1k   | 128,504,949    | 128,505   |
| fs-medium-10k  | 128,504,949    | 12,851    |
| fs-medium-100k | 128,504,949    | 1,286     |
| fs-medium-1m   | 128,504,949    | 129       |
| fs-large-1k    |                | 448,261   |
| fs-large-10k   | 1,041,930,723  | 66,270    |
| fs-large-100k  | 1,041,930,723  | 8,590     |
| fs-large-1m    | 1,041,930,723  | 996       |
