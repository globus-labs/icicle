# Icicle

Scalable metadata indexing and monitoring for HPC file systems.

[Paper (coming soon)]() | [Documentation](https://globus-labs.github.io/icicle/) | [ISC High Performance 2026](https://isc-hpc.com/)

## Overview

HPC file systems store billions of files across thousands of storage targets, yet administrators lack unified metadata visibility for storage accounting, quota enforcement, compliance auditing, and data lifecycle management. Snapshot-only tools miss changes between scans; event-only tools lack historical baselines.

Icicle addresses these limitations with a unified framework that combines bulk snapshot ingestion and real-time event monitoring, populating a **dual-index architecture**: a **primary index** with per-file POSIX metadata and an **aggregate index** with pre-computed statistical summaries (quantile sketches for size and timestamps, grouped by user, group, or directory).

<p align="center">
  <img src="docs/icicle-architecture.png" alt="Icicle Architecture" width="500">
</p>

### Installation

Requires Python >= 3.11. PyFlink's transitive dependency on `apache-beam` breaks on macOS (`pkg_resources`), so we separate the install targets:

- **`dev-core`**: Event monitor + all test/lint deps. Works on macOS and Linux.
- **`dev`**: Everything in `dev-core` plus PyFlink (`apache-flink==1.20`). Use on the remote Ubuntu dev machine for ingest pipelines.

```bash
python3 -m venv .venv
source .venv/bin/activate

# macOS or Linux — event monitor development
pip install -e '.[dev-core]'

# Linux — includes PyFlink for ingest pipelines
pip install -e '.[dev]'
```

## Snapshot Pipelines

Periodic bulk ingestion of metadata exports (GUFI SQLite databases, GPFS TSV listings) into both indexes using Amazon Managed Flink. Three sub-pipelines (PRI, CNT, SND) run in sequence.

- [Overview](docs/ingest/00-overview.md): dual-index architecture, sub-pipelines, metadata indexes
- [Preprocessing](docs/ingest/01-preprocessing.md): prepare raw snapshots into CSV
- [Infrastructure](docs/ingest/02-infrastructure.md): MSK, S3, IAM setup
- [Flink Setup](docs/ingest/03-flink-setup.md): dev machine, local testing, CloudWatch
- [Flink Pipelines](docs/ingest/04-flink-pipelines.md): compile, deploy, and run PRI / CNT / SND
- [Analysis Scripts](docs/ingest/05-analysis-scripts.md): sketch accuracy evaluation

## Event Monitor

Real-time filesystem event monitoring with three backends: **fswatch** (macOS), **Lustre** changelogs (HPC), and **GPFS** via Kafka (HPC). Applies stateful event reduction to eliminate redundant events before emitting structured JSON updates.

- [Overview](docs/icicle/00-overview.md): pipeline architecture, backends, event reduction
- [Local Dev (fswatch)](docs/icicle/00-overview.md#local-dev-fswatch): macOS development backend
- [Infrastructure](docs/icicle/02-infrastructure.md): deploy Lustre / GPFS clusters on AWS
- [Lustre Backend](docs/icicle/03-lustre-backend.md): HPC Lustre changelog backend
- [GPFS Backend](docs/icicle/04-gpfs-backend.md): IBM Storage Scale mmwatch backend

## Citation

<!-- TODO: add bibtex entry after camera-ready -->
Coming soon.

## Acknowledgements

We thank the teams of the [Diaspora Project](https://diaspora-project.github.io/) and [Globus](https://www.globus.org/) for their valuable comments and feedback. This work was supported in part by the Diaspora Project, funded by the U.S. Department of Energy, Office of Science, Office of Advanced Scientific Computing Research, under Contract DE-AC02-06CH11357, and by the Globus Search Project, funded by the National Science Foundation under Award 2411188.

## License

Distributed under the MIT License. See [LICENSE](LICENSE).
