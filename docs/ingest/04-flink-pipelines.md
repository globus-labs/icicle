# Flink Pipelines

Compile, deploy to Amazon Managed Flink (KDA), and run the three ingest pipelines (PRI, CNT, SND) using scripts in [`scripts/ingest-kda/`](../../scripts/ingest-kda/).

## Quickstart Checklist

End-to-end flow for a new deployment:

1. Deploy dev machine — run `dev-machine.ipynb` ([03-flink-setup.md](03-flink-setup.md) section 1)
2. **Create `search.py`** on the remote instance (required before compile) and run `make compile`
3. Create KDA application (see Prerequisites below)
4. **PRI** — configure, deploy, start, wait for `READY`, verify
5. **CNT** — configure (batch mode), deploy, start, wait for `READY`
6. Post-process CNT — run [`a01_process_counts.py`](../../scripts/ingest-preprocessing/a01_process_counts.py), copy CSV to `resources/`, `make compile`
7. **SND** — configure, deploy, start, wait for `READY`, verify

> For automated runs of steps 4–7, see [section 4 (`bmk-run.sh`)](#4-automating-with-bmk-runsh).

## Prerequisites

- Dev machine deployed via `dev-machine.ipynb` (see [03-flink-setup.md](03-flink-setup.md) section 1)
- **`search.py` created** and `make compile` completed — compile will fail without it (see [03-flink-setup.md](03-flink-setup.md) section 1)
- Kafka topics created (see [02-infrastructure.md](02-infrastructure.md) section 2.1)
- KDA application created and configured:
  1. Go to the AWS KDA Console and create a new app with **Apache Flink 1.20**,
     **IAM Role** `Kinesis-Analytics-Admin`, **Template** Development, **System rollback** Off
  2. Disable **Snapshots**, **System rollback**, **Checkpointing**, and **Automatic scaling**

## 1. Compile & Deploy

### 1.1. Compile

Recompile whenever code or resources change:

```bash
cd ~/icicle/src/ingest
make compile
```

This builds the fat JAR, packages Python code into zips, and uploads the
application ZIP (`ingest-1.0.0.zip`) to `s3://search-alpha-bucket-1`.

### 1.2. Configure `kda-config.sh`

Edit `scripts/ingest-kda/kda-config.sh` before deploying:

| Variable | Description | Default |
|---|---|---|
| `APP_NAME` | KDA application name | `initial-ingest-post` |
| `ENV_FILE` | Path to `application_properties.json` | `../../src/ingest/application_properties.json` |
| `S3_BUCKET_ARN` | S3 bucket holding the compiled ZIP | `arn:aws:s3:::search-alpha-bucket-1` |
| `S3_FILE_KEY` | ZIP filename in the bucket | `ingest-1.0.0.zip` |
| `KPU` | Kinesis Processing Units (parallelism) | `128` |

> **KPU quota:** The default AWS account limit is **64 KPU per application**.
> To use 128 or 256, submit a quota increase request via the
> [Service Quotas console](https://console.aws.amazon.com/servicequotas/).

### 1.3. Deploy & Start

```bash
cd ~/icicle/scripts/ingest-kda
chmod +x *.sh

# Update application JAR & settings
./kda-update.sh

# Start the application
./kda-start.sh

# Check application status
./kda-describe.sh

# Open the Flink dashboard
./kda-url.sh
```

## 2. Running Pipelines (ITAP Example)

The three pipelines must run in order: **PRI → CNT → SND**.

> For automated runs, see [section 4 (`bmk-run.sh`)](#4-automating-with-bmk-runsh).

> **`mock_ingest` default:** `bmk-update-json.sh` defaults `--mock_ingest=true`,
> which skips writing to Globus Search. Add `--mock_ingest=false` to any
> pipeline command below to perform real ingestion.

### 2.1. Primary Pipeline (PRI)

Configure and deploy:

```bash
cd ~/icicle/scripts/ingest-kda
./bmk-update-json.sh \
  --s3_input=s3a://icicle-fs-small/fs-small-1m \
  --agg_file=itap.agg2.csv \
  --pipeline_name=PRI
./kda-update.sh
./kda-start.sh
```

Monitor until `READY` (the application transitions through `STARTING` → `RUNNING` → `READY`):

```bash
./kda-describe.sh   # prints one of: STARTING, RUNNING, READY
./kda-url.sh         # Flink dashboard
```

> **Typical timing (fs-small-1m, 128 KPU):** ~2 min starting + 2–3 min running.

After PRI completes, verify ingestion results:

```bash
cd ~/icicle/scripts/ingest-preprocessing
python a02_process_sizes.py --request-topic search-alpha-primary-ingest-results
```

### 2.2. Counting Pipeline (CNT)

```bash
cd ~/icicle/scripts/ingest-kda
./bmk-update-json.sh \
  --s3_input=s3a://icicle-fs-small/fs-small-1m \
  --agg_file=itap.agg2.csv \
  --pipeline_name=CNT \
  --execution_mode=batch
./kda-update.sh
./kda-start.sh
```

Wait for `READY` (`STARTING` → `RUNNING` → `READY`, ~2 min + 4–5 min for fs-small-1m),
then generate the aggregation CSV required by SND **on the dev machine**
(requires `confluent_kafka`):

```bash
cd ~/icicle/scripts/ingest-preprocessing

# ITAP
python a01_process_counts.py \
  --request-topic search-alpha-counts \
  --result-csv itap.agg2.csv \
  --verify-csv itap.veri.csv

# NERSC
# python a01_process_counts.py \
#   --request-topic search-alpha-counts \
#   --result-csv nersc.agg2.csv \
#   --verify-csv nersc.veri.csv

# HPSS
# python a01_process_counts.py \
#   --request-topic search-alpha-counts \
#   --result-csv hpss.agg2.csv \
#   --verify-csv hpss.veri.csv
```

**Before starting SND, complete all three steps:**

1. Run [`a01_process_counts.py`](../../scripts/ingest-preprocessing/a01_process_counts.py) (above) to generate the aggregation CSV
2. Copy the CSV into the ingest resources:
   ```bash
   cp itap.agg2.csv ~/icicle/src/ingest/resources/
   ```
3. Recompile to package the new CSV into the application ZIP:
   ```bash
   cd ~/icicle/src/ingest
   make compile
   ```

> **Warning:** SND will produce incorrect results if you skip any of
> these steps. The aggregation CSV from CNT is required input for SND.

### 2.3. Secondary Pipeline (SND)

```bash
cd ~/icicle/scripts/ingest-kda
./bmk-update-json.sh \
  --s3_input=s3a://icicle-fs-small/fs-small-1m \
  --agg_file=itap.agg2.csv \
  --pipeline_name=SND \
  --pipeline_args="usr|grp|dir"
./kda-update.sh
./kda-start.sh
```

After SND completes, verify ingestion results:

```bash
cd ~/icicle/scripts/ingest-preprocessing
python a02_process_sizes.py --request-topic search-alpha-secondary-ingest-results
```

## 3. Helper Scripts Reference

The `scripts/ingest-kda/` directory includes scripts for configuring and
running pipelines:

| Script | Purpose |
|---|---|
| `kda-update.sh` | Update KDA application code & settings |
| `kda-start.sh` | Start the KDA application |
| `kda-describe.sh` | Check application status |
| `kda-url.sh` | Get the Flink dashboard URL |
| `bmk-update-json.sh` | Update `application_properties.json` via CLI flags |
| `bmk-update-kda.sh` | Switch KDA environment and KPU |
| `bmk-prepare.sh` | Quick setup: uncomment entries to configure a run |
| `bmk-run.sh` | Automated full benchmark (PRI → CNT → SND) |

**`bmk-update-json.sh`** flags:

```bash
# Required: --s3_input, --agg_file, --pipeline_name
# Optional: --execution_mode, --cmode, --run, --pipeline_args,
#           --prefix_min, --prefix_max, --mock_ingest
```

**`bmk-update-kda.sh`** — switch KDA app target and parallelism:

```bash
# Usage: ./bmk-update-kda.sh <env> <kpu>
# Sets APP_NAME=initial-ingest-<env> and KPU=<kpu> in kda-config.sh
./bmk-update-kda.sh post 128
```

## 4. Automating with bmk-run.sh

Instead of running each pipeline manually, use `bmk-run.sh` to run the full
PRI → CNT → SND sequence automatically:

```bash
cd ~/icicle/scripts/ingest-kda
./bmk-run.sh
```

Each step updates the JSON config, deploys to KDA, starts the job, and waits
for completion before moving to the next pipeline. Edit the dataset args and
uncomment sections in the script to select which datasets to run.
