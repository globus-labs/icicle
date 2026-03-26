# Flink Setup

Development environment setup for the ingest pipeline. Covers module layout, dev machine provisioning, Globus Search credentials, local testing, and CloudWatch log queries.

## Module Structure

```
src/ingest/
  main.py              # entry point — dataset dispatch
  conf.py              # configuration, dataset registry, Flink env setup
  constants.py         # NUM_WORKERS, QUANTILE_BREAKPOINTS, derive_worker_id()
  csv_schema.py        # CsvSchema dataclass + ITAP/HPSS/NERSC schemas
  helpers.py           # GetGmeta, CountsPrepare, GetUsrGrpPathWorkerCols, run_pipelines()
  pipeline.py          # primary_pipeline, counting_pipeline, secondary_pipeline
  batch.py             # BatchGmeta (Globus Search batch ingestion)
  sketches.py          # sketch init/decode/encode, calc_file_stats, calc_time_stats
  aggregation.py       # SecondaryMap, SecondaryReduce, get_usr_grp_dir_dicts
  prefix.py            # get_prefix() directory prefix extraction
  calc.py              # backward-compatible re-export facade
  compile.sh           # build & upload to S3
  Makefile             # make deps / make compile / make clean
  pom.xml              # Maven config for fat JAR (Flink + Kafka + MSK IAM)
  utils/
    kafkaconnector.py  # Kafka source/sink builders with AWS MSK IAM auth
```

## Prerequisites

> **Reading order:** [01-preprocessing](01-preprocessing.md) →
> [02-infrastructure](02-infrastructure.md) → this doc → [04-flink-pipelines](04-flink-pipelines.md)

Before running locally or deploying to KDA, ensure all required infrastructure
is set up by following [02-infrastructure.md](02-infrastructure.md).

You must have:

* **MSK (Kafka) cluster** configured (see [02 section 1](02-infrastructure.md))
* **Kafbat UI** deployed to manage topics (see [02 section 2](02-infrastructure.md))
* **S3 buckets** for input data (see [01 section 5](01-preprocessing.md)) and
  compiled code (`search-alpha-bucket-1`, see [02 section 3](02-infrastructure.md))
* **IAM role** for Managed Flink to access S3 and MSK (see [02 section 4](02-infrastructure.md))
* **IAM user** for the dev machine to access S3, MSK, and start/stop Flink (see [02 section 1.4](02-infrastructure.md))

**Important project defaults:**

* Kafka broker addresses default to the values in `_DEFAULT_BROKERS` in [`utils/kafkaconnector.py`](../../src/ingest/utils/kafkaconnector.py). Override at runtime with the `KAFKA_BROKERS` environment variable.
* Kafka topic names are defined in `IngestConfig` in [`conf.py`](../../src/ingest/conf.py).
* Dataset-specific defaults (S3 paths, Globus Search index IDs) are in `_DATASET_REGISTRY` in [`conf.py`](../../src/ingest/conf.py).

## 1. Development Machine Setup

> **Note:** `apache-flink` does not build on macOS due to an `apache-beam`
> dependency on `pkg_resources`. Use the remote Ubuntu dev machine for all
> Flink/ingest development.

Use [`dev-machine.ipynb`](../../scripts/icicle-deploy/dev-machine.ipynb) to automatically
provision an Ubuntu 22.04 EC2 instance with all required dependencies:

* Python 3.11, Java JDK 11, Maven, AWS CLI
* Icicle repo cloned with venv and all Python dependencies
* Ingest binary dependencies (`make deps`), resources directory, and custom pth file

Run all cells in the notebook. When it finishes, SSH into the machine:

```bash
ssh flink-dev-<tag>   # tag is printed in the notebook summary
```

### After the notebook: Add Globus Search Credentials & Compile

The ingest pipeline writes to Globus Search indices using a **client credential**.
To generate the credentials for `search.py`:

1. **Create a Globus Group** on [globus.org](https://globus.org) and grant it
   `writer` role on both the primary and secondary Search indices.
2. **Create a Client ID & secret** on the
   [Globus Developers](https://developers.globus.org) console.
3. **Add the client to the Globus Group**
   (see the [Globus credential appendix](#appendix-globus-search-credential-setup) below).

Then create `src/ingest/search.py` with the generated values:

```python
# src/ingest/search.py
SEARCH_CLIENT_ID = "43ae1080-efca-459c-b61c-26331516e2f6"
SEARCH_CLIENT_SECRET = "..."  # from Globus developer console
```

Compile:

> **Important:** `search.py` **must** exist before running `make compile` —
> the build will fail without it.

```bash
cd ~/icicle/src/ingest
make compile
```

`make compile` runs `compile.sh`, which copies source files (including `search.py`)
into the packaging directory, builds the fat JAR via Maven, and uploads the
application ZIP (`ingest-1.0.0.zip`) to `s3://search-alpha-bucket-1`.

> **Note:** `search-alpha-bucket-1` is a separate S3 bucket that holds the
> compiled application code (not the input data). The KDA scripts in
> `scripts/ingest-kda/` reference this bucket to deploy the application.

> **Why store credentials in `search.py`?**
> AWS Managed Flink does not provide a secure native secret injection mechanism.
> In production, we recommend using **AWS Secrets Manager** instead.

## 2. Local Development Run

### Step 1. Configure `application_properties.json`

Edit `src/ingest/application_properties.json` to set the input data and pipeline.
Key fields in the `IngestConfig` property group:

| Field | Description | Values | Pipelines |
|---|---|---|---|
| `s3_input` | S3 path to the input dataset | See S3 paths below | all |
| `pipeline_name` | Pipeline(s) to run | `PRI` (primary), `CNT` (counting), `SND` (secondary). Combine, e.g. `PRI,CNT,SND` | — |
| `pipeline_args` | Aggregation axes | `usr\|grp\|dir` | CNT, SND |
| `execution_mode` | Flink execution mode | `streaming` for PRI/SND, `batch` for CNT | all |
| `mock_ingest` | Skip Globus Search ingestion | `true`, `false` | PRI, SND |
| `run` | Run identifier | `1`, `2`, ... | all |
| `cmode` | Sketch algorithm for counting | `dd` (DDSketch), `kll` (KLL), `req` (REQ), `td` (t-digest). Defined in `CMode` in [`conf.py`](../../src/ingest/conf.py) | SND |
| `prefix_min` / `prefix_max` | Directory prefix depth range (half-open) | Default `1` / `5`. Per-dataset defaults in `_DATASET_REGISTRY` in [`conf.py`](../../src/ingest/conf.py) | CNT, SND |

**S3 input paths and dataset aliases:**

| S3 path | Dataset | Notes |
|---|---|---|
| `s3a://icicle-fs-small/fs-small-*` | itap | Small test data |
| `s3a://icicle-fs-medium/fs-medium-*` | nersc | Medium test data |
| `s3a://icicle-fs-large/fs-large-*` | hpss | Large test data |
| `s3a://purdue-fsdump-bucket/itap/*` | itap | Legacy ITAP path |
| `s3a://purdue-fsdump-bucket/hpss/*` | hpss | Legacy HPSS path |
| `s3a://nersc-fsdump-bucket/*` | nersc | Legacy NERSC path |

Dataset detection is automatic based on the S3 path (see `_detect_dataset()` in [`conf.py`](../../src/ingest/conf.py)).

### Step 2. Set Environment Variables

```bash
export IS_LOCAL=true
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
```

> **Why export AWS credentials?** The Hadoop S3A connector used by Flink reads
> credentials from environment variables, not from `~/.aws/credentials`.
> The `aws configure get` commands pull the values already stored by `aws configure`.

### Step 3. Run Application

Run the application locally:

```bash
cd ~/icicle/src/ingest
python main.py
```

### Step 4. Check Logs

```bash
python -c "import pyflink, os; print(os.path.dirname(pyflink.__file__) + '/log')"
```

## 3. Running on Amazon Managed Flink (KDA)

For production-scale runs, see [04-flink-pipelines.md](04-flink-pipelines.md) for compiling,
deploying, creating the KDA application, and running pipelines.

### Logs & Troubleshooting

Use **CloudWatch Log Insights**:

```sql
fields @timestamp, message
| sort @timestamp asc
| filter logger like /PythonDriver/
| limit 1000
```

**Pipeline duration** — find state-transition timestamps to measure how long a
pipeline run takes. The gap between `CREATED → RUNNING` and
`RUNNING → FINISHED` is the execution duration:

```sql
fields @timestamp, @message
| filter @message like /switched from state CREATED to RUNNING/
    or @message like /switched from state RUNNING to FINISHED/
| sort @timestamp asc
```

## 4. Tests

```bash
python -m pytest tests/ingest/ -v
```

## 5. Cleanup

**Dev machine:** use [`cleanup.ipynb`](../../scripts/icicle-deploy/cleanup.ipynb) with:

* `FILESYSTEM_NAME = 'flink-dev'`
* `SSH_CONFIG_MARKER = 'dev-machine'`

**AWS services** — delete or stop these when not in use to avoid ongoing charges:

* **Amazon MSK** — delete the cluster via the MSK console. MSK incurs hourly
  broker charges even when idle.
* **Amazon Managed Flink (KDA)** — delete the application via the KDA console.
  No charge when stopped, but the application retains its configuration.
* **S3 buckets** — storage costs accrue for data at rest. Delete input data
  buckets (`icicle-fs-small`, etc.) and the code bucket (`search-alpha-bucket-1`)
  if no longer needed.

## 6. References

* Based on examples from the AWS Flink Examples Repository.

## Appendix: Globus Search Credential Setup

These steps generate the `SEARCH_CLIENT_ID` and `SEARCH_CLIENT_SECRET` values
used in `src/ingest/search.py`.

**1. Create a Globus Group and grant writer roles on both Search indices:**

```python
search_client.create_role(
    index_id,
    {"role_name": "writer", "principal": "urn:globus:groups:id:<GROUP_UUID>"},
)
```

**2. Register a client** on [Globus Developers](https://developers.globus.org) to
obtain a `SEARCH_CLIENT_ID` and `SEARCH_CLIENT_SECRET`.

**3. Add the client to the Globus Group** (uses `globus_sdk`):

```python
import globus_sdk
from globus_sdk.scopes import GroupsScopes

def add_client(native_client_id, service_client_id, group_id):
    client = globus_sdk.NativeAppAuthClient(native_client_id)
    client.oauth2_start_flow(requested_scopes=[GroupsScopes.all])
    print(f"Login at: {client.oauth2_get_authorize_url()}")
    tokens = client.oauth2_exchange_code_for_tokens(input("Code: ").strip())
    gc = globus_sdk.GroupsClient(authorizer=globus_sdk.AccessTokenAuthorizer(
        tokens.by_resource_server["groups.api.globus.org"]["access_token"],
    ))
    batch = globus_sdk.BatchMembershipActions()
    batch.add_members(service_client_id)
    print(gc.batch_membership_action(group_id, batch))
```
