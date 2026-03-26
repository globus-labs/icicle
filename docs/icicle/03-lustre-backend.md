# Lustre Backend

HPC filesystem backend reading Lustre changelogs via `lfs changelog`. Events are FID-based and require `fid2path` resolution to map FIDs to file paths. One monitor instance runs per Metadata Target (MDT); throughput scales linearly with MDT count. FID resolution is deferred until after event reduction, so redundant events never trigger expensive `fid2path` calls.

## Infrastructure Deployment

See [Infrastructure](02-infrastructure.md) for shared prerequisites (AWS, SSH, venv) and the [`deploy-lustre.ipynb`](../../scripts/icicle-deploy/deploy-lustre.ipynb) notebook walkthrough.

### Verify the deployment

#### Show server and client IPs

```bash
# From your local machine — lists all Lustre hosts and IPs
cat ~/.ssh/config
```

#### Client commands (run on a Lustre client node)

=== "MDT0000"

    ```bash
    # Filesystem usage (all OSTs and MDTs)
    lfs df -h

    # Check if servers are reachable
    sudo lfs check servers

    # Show this node's LNet NID
    sudo lctl list_nids

    # Create a directory striped to MDT0000
    sudo lfs mkdir -i 0 /mnt/fs0/mdt0

    # Read changelog entries
    sudo lfs changelog fs0-MDT0000

    # Clear changelog up to <endrec> for user cl1
    sudo lfs changelog_clear fs0-MDT0000 cl1 <endrec>
    ```

=== "MDT0001"

    ```bash
    # Filesystem usage (all OSTs and MDTs)
    lfs df -h

    # Check if servers are reachable
    sudo lfs check servers

    # Show this node's LNet NID
    sudo lctl list_nids

    # Create a directory striped to MDT0001
    sudo lfs mkdir -i 1 /mnt/fs0/mdt1

    # Read changelog entries
    sudo lfs changelog fs0-MDT0001

    # Clear changelog up to <endrec> for user cl1
    sudo lfs changelog_clear fs0-MDT0001 cl1 <endrec>
    ```

=== "MDT0002"

    ```bash
    # Filesystem usage (all OSTs and MDTs)
    lfs df -h

    # Check if servers are reachable
    sudo lfs check servers

    # Show this node's LNet NID
    sudo lctl list_nids

    # Create a directory striped to MDT0002
    sudo lfs mkdir -i 2 /mnt/fs0/mdt2

    # Read changelog entries
    sudo lfs changelog fs0-MDT0002

    # Clear changelog up to <endrec> for user cl1
    sudo lfs changelog_clear fs0-MDT0002 cl1 <endrec>
    ```

=== "MDT0003"

    ```bash
    # Filesystem usage (all OSTs and MDTs)
    lfs df -h

    # Check if servers are reachable
    sudo lfs check servers

    # Show this node's LNet NID
    sudo lctl list_nids

    # Create a directory striped to MDT0003
    sudo lfs mkdir -i 3 /mnt/fs0/mdt3

    # Read changelog entries
    sudo lfs changelog fs0-MDT0003

    # Clear changelog up to <endrec> for user cl1
    sudo lfs changelog_clear fs0-MDT0003 cl1 <endrec>
    ```

#### Server commands (run on MGS/MDS node)

```bash
# MGS only: Cluster status — all registered targets (MDTs, OSTs) and their state
sudo lctl get_param mgs.MGS.live.fs0
```

=== "MDT0000"

    ```bash
    # List all loaded devices/services on this node
    sudo lctl dl

    # Show registered changelog users
    sudo lctl get_param mdd.fs0-MDT0000.changelog_users

    # Register a new changelog user
    sudo lctl --device fs0-MDT0000 changelog_register

    # Show changelog mask (what event types are tracked)
    sudo lctl get_param mdd.fs0-MDT0000.changelog_mask
    ```

=== "MDT0001"

    ```bash
    # List all loaded devices/services on this node
    sudo lctl dl

    # Show registered changelog users
    sudo lctl get_param mdd.fs0-MDT0001.changelog_users

    # Register a new changelog user
    sudo lctl --device fs0-MDT0001 changelog_register

    # Show changelog mask (what event types are tracked)
    sudo lctl get_param mdd.fs0-MDT0001.changelog_mask
    ```

=== "MDT0002"

    ```bash
    # List all loaded devices/services on this node
    sudo lctl dl

    # Show registered changelog users
    sudo lctl get_param mdd.fs0-MDT0002.changelog_users

    # Register a new changelog user
    sudo lctl --device fs0-MDT0002 changelog_register

    # Show changelog mask (what event types are tracked)
    sudo lctl get_param mdd.fs0-MDT0002.changelog_mask
    ```

=== "MDT0003"

    ```bash
    # List all loaded devices/services on this node
    sudo lctl dl

    # Show registered changelog users
    sudo lctl get_param mdd.fs0-MDT0003.changelog_users

    # Register a new changelog user
    sudo lctl --device fs0-MDT0003 changelog_register

    # Show changelog mask (what event types are tracked)
    sudo lctl get_param mdd.fs0-MDT0003.changelog_mask
    ```

## Quick Start

```bash
icicle lustre --mdt fs0-MDT0000 --mount /mnt/fs0 --fsname fs0

# With options
icicle lustre --mdt fs0-MDT0000 --mount /mnt/fs0 --fsname fs0 \
    --ignore-events OPEN,CLOSE --max-batch-size 5000

# With YAML config (CLI args override YAML values)
icicle -c config.yaml lustre --mdt fs0-MDT0000 --mount /mnt/fs0 --fsname fs0
```

All backends support: `-o FILE` (JSON output), `-v` (debug logging), `-c FILE` (YAML config).

See [Workloads](06-workloads.md) for preflight and evaluation scripts.
