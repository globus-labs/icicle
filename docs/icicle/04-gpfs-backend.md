# GPFS Backend (IBM Storage Scale)

HPC filesystem backend consuming `mmwatch` inotify-style events via Kafka. Events are keyed by inode (not path), so renames stay in the same batch slot and can be coalesced. Supports multi-partition Kafka topics for parallel consumption via `--num-consumers`.

## Infrastructure Deployment

See [Infrastructure](02-infrastructure.md) for shared prerequisites (AWS, SSH, venv) and the [`deploy-gpfs.ipynb`](../../scripts/icicle-deploy/deploy-gpfs.ipynb) notebook walkthrough.

### Verify the deployment

```bash
# Cluster state of all nodes
sudo mmgetstate -aL

# Show filesystem details
sudo mmlsfs fs1

# List filesets
sudo mmlsfileset fs1

# List all watches
sudo mmwatch all status
```

See [`scripts/icicle-deploy/gpfs-commands.md`](../../scripts/icicle-deploy/gpfs-commands.md) for the full GPFS command reference.

## Quick Start

```bash
icicle gpfs --topic mmwatch-events --bootstrap localhost:9092
```

All backends support: `-o FILE` (JSON output), `-v` (debug logging), `-c FILE` (YAML config).

See [Workloads](06-workloads.md) for preflight and evaluation scripts.
