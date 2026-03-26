# Benchmarking

Lustre pipeline benchmarking using `--drain` mode, which processes all available changelog records and exits. Each configuration isolates a different stage of the pipeline to measure its throughput independently. All runs output to Kafka via MSK (`--kafka lustre-out --kafka-msk`).

## Pipeline Configurations

| Configuration | Flags | What it measures |
|---------------|-------|------------------|
| Changelog mode | `--changelog-mode` | Raw changelog read + Kafka write (no state processing, no reduction) |
| Baseline | *(none)* | State processing with default FID resolution (`icicle` in-memory paths) |
| Reduction + ignore OPEN | `--reduction-rules --ignore-events OPEN` | State processing + batch reduction rules, filtering OPEN events |
| fsmonitor resolution | `--fid-resolution=fsmonitor` | State processing with cache-based `fid2path` resolution instead of in-memory |

## Commands

=== "MDT0000"

    ```bash
    # Changelog mode — raw events, no state processing
    python -m src.icicle lustre \
        --mdt fs0-MDT0000 --mount /mnt/fs0 --fsname fs0 \
        --startrec 10 --drain --kafka lustre-out --kafka-msk \
        --changelog-mode

    # Baseline — state processing, icicle FID resolution
    python -m src.icicle lustre \
        --mdt fs0-MDT0000 --mount /mnt/fs0 --fsname fs0 \
        --startrec 10 --drain --kafka lustre-out --kafka-msk

    # Reduction rules + ignore OPEN events
    python -m src.icicle lustre \
        --mdt fs0-MDT0000 --mount /mnt/fs0 --fsname fs0 \
        --startrec 10 --drain --kafka lustre-out --kafka-msk \
        --reduction-rules --ignore-events OPEN

    # fsmonitor FID resolution (cache + fid2path)
    python -m src.icicle lustre \
        --mdt fs0-MDT0000 --mount /mnt/fs0 --fsname fs0 \
        --startrec 10 --drain --kafka lustre-out --kafka-msk \
        --fid-resolution=fsmonitor
    ```

=== "MDT0001"

    ```bash
    # Changelog mode — raw events, no state processing
    python -m src.icicle lustre \
        --mdt fs0-MDT0001 --mount /mnt/fs0 --fsname fs0 \
        --startrec 0 --drain --kafka lustre-out --kafka-msk \
        --changelog-mode

    # Baseline — state processing, icicle FID resolution
    python -m src.icicle lustre \
        --mdt fs0-MDT0001 --mount /mnt/fs0 --fsname fs0 \
        --startrec 0 --drain --kafka lustre-out --kafka-msk

    # Reduction rules + ignore OPEN events
    python -m src.icicle lustre \
        --mdt fs0-MDT0001 --mount /mnt/fs0 --fsname fs0 \
        --startrec 0 --drain --kafka lustre-out --kafka-msk \
        --reduction-rules --ignore-events OPEN
    ```
