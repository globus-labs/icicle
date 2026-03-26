# Infrastructure

Shared infrastructure for deploying Lustre and GPFS backends on AWS. The [fswatch backend](00-overview.md#local-dev-fswatch) runs locally and does not require infrastructure deployment.

## Quick Start

1. Configure AWS credentials and SSH key in [`deploy-config.yaml`](../../scripts/icicle-deploy/deploy-config.yaml)
2. Set up the Python environment:

    ```bash
    cd scripts/icicle-deploy
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3. Open the deployment notebook for your backend in Jupyter or VS Code:

    === "lustre"

        [`deploy-lustre.ipynb`](../../scripts/icicle-deploy/deploy-lustre.ipynb)

    === "gpfs"

        [`deploy-gpfs.ipynb`](../../scripts/icicle-deploy/deploy-gpfs.ipynb)

    === "ingest-dev"

        [`dev-machine.ipynb`](../../scripts/icicle-deploy/dev-machine.ipynb)

4. Run all cells — pip packages (`boto3`, `paramiko`, `requests`) are installed automatically by the first cell.

## Configuration

All deployment settings live in [`deploy-config.yaml`](../../scripts/icicle-deploy/deploy-config.yaml) under three sections: `common` (shared AWS and SSH settings), `lustre`, and `gpfs`. Backend-specific settings are documented in their respective guides ([Lustre](03-lustre-backend.md#configuration), [GPFS](04-gpfs-backend.md#configuration)).

## Deployment

Each notebook installs its own pip dependencies, verifies AWS credentials, and loads `deploy-config.yaml` automatically — just run all cells.

=== "lustre"

    [`deploy-lustre.ipynb`](../../scripts/icicle-deploy/deploy-lustre.ipynb) provisions a Lustre topology on AWS EC2:

    1. **Launch all instances** — EC2 instances for MDS/MGS, OSS, and client nodes with Elastic IPs.
    2. **Parallel node setup** — Server pipeline (RHEL install, Lustre format of MDT/OST targets) runs in parallel with client pipeline (Lustre client install, Filebench, Icicle).
    3. **Mount clients, changelog, and verification** — Mounts Lustre on clients, enables changelog tracking, and verifies the deployment.

    See [Lustre Backend](03-lustre-backend.md) for configuration and verification.

=== "gpfs"

    [`deploy-gpfs.ipynb`](../../scripts/icicle-deploy/deploy-gpfs.ipynb) provisions a multi-node GPFS (IBM Storage Scale) cluster on AWS EC2:

    1. **Launch all instances** — EC2 instances for server and client nodes with Elastic IPs.
    2. **Parallel node setup** — Three concurrent pipelines: (A) GPFS servers (install from S3, kernel module build via `mmbuildgpl`, SSH key exchange), (B) Kafka node (Docker, topic creation), (C) client nodes (Filebench, Icicle). When `DEDICATED_KAFKA` is false, pipeline B runs after A (co-located on gpfs0).
    3. **GPFS cluster formation** — Spectrum Scale toolkit on Node 0, adds server and client nodes, creates the fileset, installs Icicle and configures permissions on all nodes.
    4. **GPFS watches and Kafka integration** — Configures mmwatch with Kafka sinks for real-time event streaming.

    See [GPFS Backend](04-gpfs-backend.md) for configuration and verification.

=== "ingest-dev"

    [`dev-machine.ipynb`](../../scripts/icicle-deploy/dev-machine.ipynb) provisions a single Flink development machine on AWS EC2:

    1. **Launch instance** — Single EC2 instance with an Elastic IP.
    2. **System setup** — Install Python 3.11, Java 11, Maven, and AWS CLI.
    3. **Icicle & Ingest setup** — Clone Icicle, create a Python venv, run `make deps`, and install the Flink S3 filesystem plugin.

    After deployment, manually create `search.py` with Globus credentials and run `make compile`.

## Cleanup

Run [`cleanup.ipynb`](../../scripts/icicle-deploy/cleanup.ipynb) to tear down a deployment. Set `DEPLOYMENT` to `'lustre'`, `'gpfs'`, or `'ingest-dev'`, then run all cells. The notebook discovers instances by `Name` tag prefix, terminates them, releases Elastic IPs, and removes their entries from `~/.ssh/config`.

!!! warning
    `DRY_RUN` defaults to `True` — set to `False` only when ready to delete.
