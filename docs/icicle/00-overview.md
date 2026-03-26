# Event Monitor

Maintains index freshness between snapshots by processing filesystem events as they occur. Monitor's reduction rules minimize downstream work:

1. **Update coalescing**: repeated events for the same file merge into one.
2. **Event cancellation**: transient sequences (e.g., CREATE then DELETE) are eliminated.
3. **Rename override**: directory renames bypass the above rules, since they affect all descendants.

Reduced events are emitted as structured JSON to stdout, a file, or Kafka.

=== "fswatch"

    ```mermaid
    graph LR
        A[FSWatchSource<br/>fswatch] --> B[BatchProcessor<br/>coalesce + reduce]
        B --> C[PathStateManager<br/>path-keyed]
        C --> D1[stdout]
        C --> D2[JSON file]
        C --> D3[Kafka/MSK topic]

        style B fill:#e8f0fe,stroke:#4285f4
        style C fill:#e8f0fe,stroke:#4285f4
    ```

=== "lustre"

    ```mermaid
    graph LR
        A[LustreChangelogSource<br/>lfs changelog] --> B[BatchProcessor<br/>coalesce + reduce]
        B --> C[PathStateManager<br/>path-keyed]
        C --> D1[stdout]
        C --> D2[JSON file]
        C --> D3[Kafka/MSK topic]

        style B fill:#e8f0fe,stroke:#4285f4
        style C fill:#e8f0fe,stroke:#4285f4
    ```

=== "gpfs"

    ```mermaid
    graph LR
        subgraph Source["GPFSKafkaSource"]
            direction LR
            K1[Consumer 1] --> Q[Queue<br/>stdlib / ring / shm]
            K2[Consumer 2] --> Q
            Kn[Consumer N] --> Q
        end
        Q --> B[BatchProcessor<br/>coalesce + reduce]
        B --> C[GPFSStateManager<br/>inode-keyed]
        C --> D1[stdout]
        C --> D2[JSON file]
        C --> D3[Kafka/MSK topic]

        style Source fill:#fff8e1,stroke:#f9a825
        style B fill:#e8f0fe,stroke:#4285f4
        style C fill:#e8f0fe,stroke:#4285f4
    ```

## EventSource

Abstracts backend-specific event formats into uniform key-value pairs.

| Backend | Platform | Source | State Manager |
|---------|----------|--------|---------------|
| fswatch | macOS, Linux | `FSWatchSource` (fswatch subprocess) | `PathStateManager` (path-keyed) |
| Lustre | Linux | `LustreChangelogSource` (lfs changelog subprocess) | `PathStateManager` (path-keyed) |
| GPFS | Linux | `GPFSKafkaSource` (Kafka consumer) | `GPFSStateManager` (inode-keyed) |

For Lustre, FID resolution (`fid2path`) is deferred until after reduction — each call costs ~10 ms, so skipping it for events that will be discarded (e.g., a CREATE cancelled by a DELETE) yields significant throughput gains. For GPFS, one or more Kafka consumers operate in parallel across topic partitions.

## BatchProcessor

Collects events into time-windowed batches and applies stateful reduction rules. Each incoming event is matched against prior events in the same **slot** (keyed by path or inode):

| Action | Effect | Example |
|--------|--------|---------|
| **ACCEPT** | Emit downstream | CREATE with no prior events |
| **IGNORE** | Drop new event | MODIFY after an earlier MODIFY |
| **CANCEL** | Drop both events | DELETE cancels a preceding CREATE (Robinhood-aligned) |

Path-keyed backends (fswatch, Lustre) bypass the slot on renames since the key changes. The inode-keyed backend (GPFS) keeps renames in the slot, enabling additional coalescing.

## StateManager

Maintains an in-memory tree of the filesystem hierarchy. Resolves paths for newly created objects via parent-child relationships, avoiding per-event `fid2path` calls. On directory renames, recursively updates all descendant paths.

Emits two lists: `to_update` (FID, path, stat) and `to_delete` (FID, path).

## OutputHandler

Converts the state manager's output into structured JSON. Output modes:

- **stdout**: line-delimited JSON (default)
- **JSON file**: `-o path`
- **Kafka topic**: `--kafka TOPIC` with `--kafka-bootstrap` (default `localhost:9092`)
- **AWS MSK**: `--kafka TOPIC --kafka-msk` (IAM OAUTHBEARER auth)
- **Quiet**: `-q` / `--quiet` suppresses all output (stats still logged via `-v`)

## Guides

- [Local Dev (fswatch)](#local-dev-fswatch): macOS/Linux backend — quickest way to start developing
- [Infrastructure](02-infrastructure.md): shared AWS setup for deploying Lustre and GPFS clusters
- [Lustre Backend](03-lustre-backend.md): HPC changelog backend, FID-based, scales with MDT count
- [GPFS Backend](04-gpfs-backend.md): inotify-style events via Kafka, inode-keyed, multi-partition
- [Workloads](06-workloads.md): preflight, evaluation, and filebench scripts
- [Benchmarking](07-benchmarking.md): Lustre pipeline benchmarking with drain mode

## Local Dev (fswatch)

Local development backend using `fswatch` on macOS or Linux. Events are path-based and processed by `PathStateManager`. Supports symlink detection (unlike Lustre) and is the recommended starting point for development.

### Prerequisites

- `fswatch`: `brew install fswatch` (macOS) or `sudo apt install fswatch` / build from source (Linux)
- Python >= 3.11, installed with `pip install -e '.[dev-core]'` (see [Installation](../index.md#installation))

### Quick Start

```bash
DIR=$(mktemp -d) && echo "$DIR"
icicle fswatch "$DIR"
# in another terminal, touch/edit/delete files in $DIR
```

All backends support: `-o FILE` (JSON output), `-v` (debug logging), `-c FILE` (YAML config).

See [Preflight Workloads](06-workloads.md#preflight-workloads) for the fswatch-icicle.sh preflight workload script.
