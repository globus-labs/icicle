# Workloads

On a client instance, navigate to the workloads directory and make scripts executable

```bash
cd icicle/scripts/icicle-workloads
chmod +x *.sh
export PATH="$PWD:$PATH"
```

## Preflight Workloads

Each preflight script starts the icicle monitor, runs a sequence of filesystem operations exercising all event types for that backend, then shuts down. They verify that event capture, state processing, and reduction rules work end-to-end.

=== "fswatch"

    ```bash
    # Icicle monitor with state processing (JSON lines)
    fswatch-icicle.sh $(mktemp -d)

    # Changelog mode — raw events, no state processing
    fswatch-icicle.sh $(mktemp -d) --changelog-mode

    # With batch reduction rules (auto-selected per backend)
    fswatch-icicle.sh $(mktemp -d) --reduction-rules
    ```

=== "lustre"

    ```bash
    # Icicle monitor with state processing
    lustre-icicle.sh $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX)

    # Changelog mode — raw events, no state processing
    lustre-icicle.sh $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) --changelog-mode

    # With batch reduction rules
    lustre-icicle.sh $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) --reduction-rules

    # JSON output
    lustre-icicle.sh $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) --json

    # Kafka MSK output
    lustre-icicle.sh $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) --kafka-msk

    # With reduction rules, ignoring OPEN events
    lustre-icicle.sh $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) --reduction-rules --ignore-events OPEN

    # Target a specific MDT
    lustre-icicle.sh $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) --mdt fs0-MDT0001
    ```

=== "gpfs"

    ```bash
    # Raw Kafka consumer
    gpfs-icicle.sh

    # Icicle monitor → stdout
    gpfs-icicle.sh --icicle

    # Custom GPFS directory
    gpfs-icicle.sh --dir /ibm/gpfs/fs1/test

    # Custom mmwatch topic
    gpfs-icicle.sh --topic my-topic

    # Parallel consumers
    gpfs-icicle.sh --num-consumers 4

    # Shared-memory queue
    gpfs-icicle.sh --queue-type shm
    ```

## Evaluation Workloads

Lightweight bash workloads for evaluating icicle's filesystem monitoring, originally described in the [FSMonitor paper](https://ieeexplore.ieee.org/document/8891045).

## [evaluate_output.sh](..scripts/icicle-workloads/evaluate_output.sh)

Tests correctness of event capture. Each iteration performs six operations — create, modify, rename, mkdir, move, delete — with configurable sleep between steps to give the monitoring pipeline time to process each event individually.

=== "/mnt/fs0/mdt0"

    ```bash
    # 1 iteration with 0.1s sleep between steps (useful for verifying each event)
    evaluate_output.sh -p $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) -c 1 -s 0.1

    # 100 iterations across 4 parallel workers, quiet mode
    evaluate_output.sh -p $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) -c 100 -j 4 -q
    ```

=== "/mnt/fs0/mdt1"

    ```bash
    # 1 iteration with 0.1s sleep between steps (useful for verifying each event)
    evaluate_output.sh -p $(mktemp -d /mnt/fs0/mdt1/tmpXXXXXX) -c 1 -s 0.1

    # 100 iterations across 4 parallel workers, quiet mode
    evaluate_output.sh -p $(mktemp -d /mnt/fs0/mdt1/tmpXXXXXX) -c 100 -j 4 -q
    ```

=== "/ibm/fs1/fset1"

    ```bash
    # 1 iteration with 0.1s sleep between steps (useful for verifying each event)
    evaluate_output.sh -p $(mktemp -d /ibm/fs1/fset1/tmpXXXXXX) -c 1 -s 0.1

    # 100 iterations across 4 parallel workers, quiet mode
    evaluate_output.sh -p $(mktemp -d /ibm/fs1/fset1/tmpXXXXXX) -c 100 -j 4 -q
    ```

=== "/ibm/fs1/fset2"

    ```bash
    # 1 iteration with 0.1s sleep between steps (useful for verifying each event)
    evaluate_output.sh -p $(mktemp -d /ibm/fs1/fset2/tmpXXXXXX) -c 1 -s 0.1

    # 100 iterations across 4 parallel workers, quiet mode
    evaluate_output.sh -p $(mktemp -d /ibm/fs1/fset2/tmpXXXXXX) -c 100 -j 4 -q
    ```

## [evaluate_performance.sh](..scripts/icicle-workloads/evaluate_performance.sh)

Tests throughput of event capture. Each iteration performs three operations — create, modify, delete — with configurable sleep between steps (default 0) to maximize the rate of filesystem events.

=== "/mnt/fs0/mdt0"

    ```bash
    # 1 iteration with 0.1s sleep between steps
    evaluate_performance.sh -p $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) -c 1 -s 0.1

    # 100 iterations across 4 parallel workers, quiet mode
    evaluate_performance.sh -p $(mktemp -d /mnt/fs0/mdt0/tmpXXXXXX) -c 100 -j 4 -q
    ```

=== "/mnt/fs0/mdt1"

    ```bash
    # 1 iteration with 0.1s sleep between steps
    evaluate_performance.sh -p $(mktemp -d /mnt/fs0/mdt1/tmpXXXXXX) -c 1 -s 0.1

    # 100 iterations across 4 parallel workers, quiet mode
    evaluate_performance.sh -p $(mktemp -d /mnt/fs0/mdt1/tmpXXXXXX) -c 100 -j 4 -q
    ```

=== "/ibm/fs1/fset1"

    ```bash
    # 1 iteration with 0.1s sleep between steps
    evaluate_performance.sh -p $(mktemp -d /ibm/fs1/fset1/tmpXXXXXX) -c 1 -s 0.1

    # 100 iterations across 4 parallel workers, quiet mode
    evaluate_performance.sh -p $(mktemp -d /ibm/fs1/fset1/tmpXXXXXX) -c 100 -j 4 -q
    ```

=== "/ibm/fs1/fset2"

    ```bash
    # 1 iteration with 0.1s sleep between steps
    evaluate_performance.sh -p $(mktemp -d /ibm/fs1/fset2/tmpXXXXXX) -c 1 -s 0.1

    # 100 iterations across 4 parallel workers, quiet mode
    evaluate_performance.sh -p $(mktemp -d /ibm/fs1/fset2/tmpXXXXXX) -c 100 -j 4 -q
    ```


## [run-filebench.sh](..scripts/icicle-workloads/run-filebench.sh)

Wrapper script for running filebench workloads against any filesystem path. Uses a template file (`filebench.f.template`) with a `__WORKLOAD_DIR__` placeholder that gets substituted at runtime, so a single template works for both Lustre and GPFS mounts. The default template creates 50k files with gamma-distributed sizes (mean ~16KB), runs 32 reader threads, and executes for 180 seconds. The `__WORKLOAD_DIR__` placeholder is the only value substituted — all other parameters are defined in the template and can be edited directly.


=== "/mnt/fs0/mdt0"

    ```bash
    run-filebench.sh -p /mnt/fs0/mdt0
    ```

=== "/mnt/fs0/mdt1"

    ```bash
    run-filebench.sh -p /mnt/fs0/mdt1
    ```

=== "/ibm/fs1/fset1"

    ```bash
    run-filebench.sh -p /ibm/fs1/fset1
    ```

=== "/ibm/fs1/fset2"

    ```bash
    run-filebench.sh -p /ibm/fs1/fset2
    ```
