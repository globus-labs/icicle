# icicle — Live Test Hardening Program

Autonomous test engineer for icicle's three backends: **fswatch**, **Lustre**, **GPFS**. Design and run live workloads — real filesystem ops through the real backend. **NEVER STOP.**

---

## Repo Structure

```
src/icicle/                        # Source
  events.py, source.py             # Shared: EventKind, EventSource ABC
  fswatch_events.py, fswatch_source.py
  lustre_events.py, lustre_source.py
  gpfs_events.py, gpfs_source.py
  batch.py                         # BatchProcessor, ReductionAction
  state.py                         # BaseStateManager, PathStateManager
  gpfs_state.py                    # GPFSStateManager
  monitor.py                       # Monitor orchestrator
  output.py                        # OutputHandler, StdoutOutputHandler, JsonFileOutputHandler, KafkaOutputHandler
  queue.py                         # BatchQueue ABC (StdlibQueue, RingBufferQueue)

tests/icicle/
  conftest.py                      # InMemoryEventSource fixture
  backend_config.py                # BackendConfig (fswatch, lustre, gpfs)
  scenarios.py                     # 76 mock pipeline scenarios × 3 backends
  workloads.py                     # 240 live workloads (FsOp → ExpectedChange)
  unit/                            # ~860 fast tests (no external services)
  integration/                     # Live tests (real backends)
    test_fswatch.py                #   240 workloads + ~137 fswatch-specific
    test_lustre.py                 #   240 workloads + ~37 Lustre-specific
    test_gpfs.py                   #   240 workloads + ~13 GPFS-specific
    test_kafka.py                  #   Kafka output handler
```

### Two levels of cross-backend tests

| Layer | File | What it tests | How to add |
|-------|------|---------------|-----------|
| **Mock scenarios** | `scenarios.py` | Pipeline logic (batch → state → emit) with synthetic events | Add `Scenario` to `SCENARIOS` list — auto-runs via `unit/test_unified_scenarios.py` |
| **Live workloads** | `workloads.py` | Full stack (real fs ops → real backend → pipeline → output) | Add `Workload` to `WORKLOADS` list — auto-runs via `test_workload` in each integration file |

---

## Setup

### 1. Branch and backend

```bash
BRANCH="autoresearch/icicle-$(date +%Y%m%d)"
git checkout -b "$BRANCH" 2>/dev/null || git checkout "$BRANCH"
```

If the user specifies a backend (`fswatch`, `lustre`, `gpfs`), use it. Otherwise **ask**.

### 2. Verify environment

| Backend | Checks | Variables |
|---------|--------|-----------|
| fswatch | `fswatch --version` | — |
| Lustre | `lfs df -h`, `sudo lfs changelog <MDT> \| tail -5` (see [Lustre backend docs](docs/icicle/03-lustre-backend.md#verify-the-deployment)) | `LUSTRE_MOUNT`, `FSNAME`, `MDT` |
| GPFS | `df -h /ibm/fs1`, `sudo mmwatch fs1 list`, `kcat -b $KAFKA_IP:9092 -L \| head` (see `scripts/icicle-deploy/gpfs-commands.md`) | `GPFS_MOUNT`, `TOPIC`, `BOOTSTRAP_SERVERS`, `GROUP_ID` |

If any check fails, stop and tell the user.

### 3. Read code

```bash
find src/icicle/ tests/icicle/ -name '*.py' | sort
```

Read every file. Key files:

| File | Purpose |
|------|---------|
| `tests/icicle/workloads.py` | **Start here.** `FsOp`, `ExpectedChange`, `Workload`, `execute_workload()`, `assert_workload_results()`, `WORKLOADS` list |
| `tests/icicle/scenarios.py` | Mock scenarios: `Scenario`, `SCENARIOS` list (76 entries) |
| `tests/icicle/backend_config.py` | `BackendConfig` for each backend |
| `tests/icicle/integration/test_fswatch.py` | `harness` fixture, `live_monitor`, `CollectingOutputHandler`, helpers |
| `tests/icicle/integration/test_lustre.py` | `harness` fixture, `_source_and_pipeline()`, MDT parametrization |
| `tests/icicle/integration/test_gpfs.py` | `harness` fixture, `_run_gpfs_pipeline()`, `_collect_events()` |

### 4. Results tracker

```bash
[ -f results.tsv ] || echo -e "iter\tbackend\tscenario\ttype\tpass_fail\tnotes\tcommit" > results.tsv
```

---

## The Loop (~5 min/iter, FOREVER)

### Step 1: Design a new workload or test

Check `results.tsv` — never repeat. Pick one of three paths:

#### Path A: Cross-backend workload (preferred)

Add a `Workload` to `workloads.py`. It runs automatically on **all backends** via the `harness` fixture.

```python
# workloads.py — add to WORKLOADS list
Workload(
    'my_new_workload',
    'Description of what this tests',
    ops=[
        FsOp('create_file', 'f.txt', {'content': b'hello\n'}),
        FsOp('chmod', 'f.txt', {'mode': 0o755}),
        ...
    ],
    expected=[
        ExpectedChange('update', '/f.txt', {'size': 6}),
        ...
    ],
)
```

Available `FsOp` actions: `create_file`, `mkdir`, `makedirs`, `rename`, `delete`, `rmtree`, `chmod`, `write`, `append`, `truncate`, `symlink`, `hardlink`, `copy`, `utime`, `replace`. Each sleeps `pause_after` (default 0.3s) after execution.

Available `ExpectedChange` stat_checks: `'size': N` (exact), `'size_gt': N` (greater than), `'has_stat': True` (stat dict present).

#### Path B: Cross-backend mock scenario

Add a `Scenario` to `scenarios.py` when testing pipeline logic (batch reduction, state management) without real filesystems. It runs on all 3 backends via `unit/test_unified_scenarios.py`.

#### Path C: Backend-specific test

Write directly into the backend's integration test file for behavior that only applies to one backend:

| Category | Backend | Where |
|----------|---------|-------|
| xattr, mmap, sparse files, FIFO, concurrent writers | fswatch | `integration/test_fswatch.py` |
| FID cache, fid2path fallback, changelog parsing, OPEN filtering, cross-MDT | Lustre | `integration/test_lustre.py` |
| Kafka partitions, shm queue, inode reuse, multi-consumer | GPFS | `integration/test_gpfs.py` |

### Step 2: Predict output

Before writing code, predict: what events does the backend emit? How does batch reduction coalesce them? What `(op, path)` pairs should appear? Write it down.

### Step 3: Implement

**For workloads (Path A):** Add to `WORKLOADS` in `workloads.py`. Done — it auto-runs.

**For scenarios (Path B):** Add to `SCENARIOS` in `scenarios.py`. Done — it auto-runs.

**For backend-specific (Path C):** Write test method in the backend's integration file. Reuse existing `harness`/`live_monitor` fixtures and helpers.

### Step 4: Commit, run, record

```bash
git add tests/icicle/<file>.py
git commit -m "test(icicle): <scenario> — iter N"

# Run the specific test
python -m pytest tests/icicle/integration/test_<backend>.py::test_workload[my_new_workload] -v > run.log 2>&1
# or for backend-specific:
python -m pytest tests/icicle/integration/test_<backend>.py::<Class>::<test> -v > run.log 2>&1
tail -20 run.log

# If FAIL: fix test or source, re-run.
# Stuck > 5 min? git revert HEAD, record fail, move on.

# Regression check
python -m pytest tests/icicle/ -v --tb=line > full_run.log 2>&1
tail -5 full_run.log

echo -e "N\t$BACKEND\t<scenario>\tworkload|scenario|specific\t<pass|fail>\t<notes>\t$(git rev-parse --short HEAD)" >> results.tsv
```

**GOTO Step 1.**

---

## Workload ideas by category

Combine 2+ axes. Prefer **Path A** (workload) when the operation is filesystem-generic.

| Category | Cross-backend (Path A) | Backend-specific (Path C) |
|----------|----------------------|--------------------------|
| **File ops** | create, write, append, truncate, chmod, overwrite | mmap write (fswatch), sparse files (fswatch) |
| **Dir ops** | mkdir, rmtree, nested makedirs, rename tree | — |
| **Rename/move** | rename chain, cross-dir move, atomic rename, swap, collision overwrite | rename pairing behavior (fswatch inotify) |
| **Links** | symlink create, hardlink + delete | symlink chain resolution (fswatch), xattr (fswatch) |
| **Burst/timing** | N simultaneous creates, rapid modify, rapid create-delete | concurrent thread writers (fswatch) |
| **Structure** | deep nesting, wide dirs, unicode/special filenames, empty/large files | FIFO/named pipe (fswatch) |
| **Metadata** | size accuracy, chmod mode, parent dir update on child add | explicit utime (fswatch), S_ISDIR mode check |
| **Lustre-specific** | — | FID cache miss, cross-MDT, OPEN/SATTR drops, fid2path fallback, changelog startrec |
| **GPFS-specific** | — | inode reuse, multi-partition, metadata from events, shm queue, multi-consumer |

---

## Cleanup (after each iteration)

```bash
pgrep -u "$(whoami)" fswatch 2>/dev/null | xargs kill 2>/dev/null
pgrep -u "$(whoami)" -f "icicle" 2>/dev/null | xargs kill 2>/dev/null
ls -d ${LUSTRE_MOUNT:-/tmp}/icicle_test_* ${LUSTRE_MOUNT:-/tmp}/icicle_wl_* \
      ${GPFS_MOUNT:-/tmp}/icicle_test_* ${GPFS_MOUNT:-/tmp}/icicle_wl_* 2>/dev/null | xargs rm -rf
rm -f /dev/shm/psm_* 2>/dev/null
```

---

## Directives

1. **NEVER STOP.** Target ~12 iterations/hour.
2. **Every workload/scenario is new.** Check `results.tsv`.
3. **Prefer workloads (Path A).** Cross-backend by default. Only use Path C for genuinely platform-specific behavior.
4. **Every live test uses the real backend.** No mocks, no `InMemoryEventSource`.
5. **Commit before running.** Can't fix in 5 min → `git revert HEAD`, move on.
6. **Fix source, not tests.** If behavior should be X but code produces Y, fix the code.
7. **Reuse helpers.** Never duplicate fixtures or infrastructure.
8. **Context management.** Redirect output to files. Only `tail` what you need.
9. **Timeout:** Single iteration > 10 min → abandon, revert, record, continue.

**GOTO The Loop. NEVER STOP.**
