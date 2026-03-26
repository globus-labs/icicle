# Workload Scripts

## creation-only.sh
Creates N files (or runs forever) in a random `/mnt/exacloud/tmp…` folder.

**Usage:** `./creation-only.sh [COUNT] [SLEEP_TIME] [OUTPUT]`
- `COUNT`: how many files (0 = infinite; default 0)
- `SLEEP_TIME`: seconds between each create (default 0.05)
- `OUTPUT`: 1 = show “Created…” messages; 0 = silent (default 1)

## update-only.sh
After creating N files, endlessly performs random trunc/chmod/read/write.

**Usage:** `./update-only.sh [N] [M] [OUTPUT]`
- `N`: initial files to create (default 10)
- `M`: seconds between each random operation (default 0.01)
- `OUTPUT`: 1 = show each action; 0 = silent (default 1)

Both scripts print a `rm -rf "<folder>"` command on exit to help you clean up.

## evaluate_output.sh
Performs a full create→modify→rename→mkdir→move→delete cycle on hello files.

**Usage:** `./evaluate_output.sh [COUNT] [SLEEP_TIME] [OUTPUT]`
- `COUNT`: how many iterations to run (0 = infinite; default 0)
- `SLEEP_TIME`: seconds between each step (default 0.05)
- `OUTPUT`: 1 = print each action; 0 = silent (default 1)

**Example:**
Run 3 iterations with 0.2s between actions and verbose output:
```
./evaluate_output.sh 3 0.2 1
```

## evaluate_performance.sh
Performs a light create→modify→delete cycle on hello files for performance testing.

**Usage:** `./evaluate_performance.sh [COUNT] [SLEEP_TIME] [OUTPUT]`
- `COUNT`: how many iterations to run (0 = infinite; default 0)
- `SLEEP_TIME`: seconds between each step (default 0.1)
- `OUTPUT`: 1 = print each action; 0 = silent (default 1)

**Example:**
Run 100 iterations with 0.05s between actions and silent mode:
```
./evaluate_performance.sh 100 0.05 0
```

## parallel_load.sh
Runs multiple workers in parallel, each executing one of the above workload scripts.

**Usage:** `./parallel_load.sh [COUNT] [SLEEP_TIME] [OUTPUT] [WORKERS] [WORKLOAD_NAME]`
- `COUNT`: how many iterations per worker (0 = infinite; default 0)
- `SLEEP_TIME`: seconds between each operation (default 0.05 for evaluate_output, 0.1 for evaluate_performance)
- `OUTPUT`: 1 = log actions; 0 = silent (default 1)
- `WORKERS`: number of parallel worker processes (default 1)
- `WORKLOAD_NAME`: either `evaluate_output` or `evaluate_performance`

This script launches the specified number of background worker processes, each running the selected workload script with the provided parameters, and waits for all workers to complete.
