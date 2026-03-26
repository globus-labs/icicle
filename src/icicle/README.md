# icicle -- Package Reference

The `src/icicle/` package implements a unified real-time filesystem metadata monitoring pipeline supporting fswatch, Lustre, and GPFS backends.

## Modules

### Core Pipeline

| Module | Description |
|--------|-------------|
| `source.py` | `EventSource` ABC -- `read()` returns event batches, `close()` releases resources |
| `events.py` | `EventKind` enum -- CREATED, REMOVED, RENAMED, MODIFIED |
| `batch.py` | `BatchProcessor` -- slot-based coalescing with configurable reduction rules |
| `state.py` | `BaseStateManager` ABC, `PathStateManager` -- path-keyed in-memory tree |
| `monitor.py` | `Monitor` -- wires Source -> Batch -> State -> Output, signal handling |

### Backends

| Module | Description |
|--------|-------------|
| `fswatch_source.py` | `FSWatchSource` -- reads from fswatch subprocess (macOS FSEvents) |
| `fswatch_events.py` | Flag parsing, `STAT_REDUCTION_RULES` |
| `lustre_source.py` | `LustreChangelogSource` -- polls `lfs changelog`, resolves FIDs |
| `lustre_events.py` | Changelog line parsing, `LUSTRE_EVENT_MAP` |
| `gpfs_source.py` | `GPFSKafkaSource` -- multi-threaded Kafka consumer for mmwatch |
| `gpfs_events.py` | Inotify event mapping, `parse_gpfs_message`, `GPFS_REDUCTION_RULES` |
| `gpfs_state.py` | `GPFSStateManager` -- inode-keyed state (metadata from events, no os.stat) |

### Output

| Module | Description |
|--------|-------------|
| `output.py` | `OutputHandler` ABC, `StdoutOutputHandler`, `JsonFileOutputHandler`, `KafkaOutputHandler` |

### Infrastructure

| Module | Description |
|--------|-------------|
| `queue.py` | `BatchQueue` ABC, `StdlibQueue`, `RingBufferQueue` |
| `mpsc_queue.py` | `SharedBatchQueue` -- shared-memory ring buffer for GPFS multi-consumer |
| `config.py` | YAML configuration loader (`load_config`) |
| `__main__.py` | CLI entry point -- `icicle {fswatch,lustre,gpfs}` subcommands |

## Architecture

```
FSWatchSource --------+
LustreChangelogSource +
GPFSKafkaSource ------+
        |
  BatchProcessor (slot-based coalescing, reduction rules)
        |
  PathStateManager   or   GPFSStateManager
  (path-keyed)            (inode-keyed)
        |
  OutputHandler (stdout | JSON file | Kafka)
```

fswatch and Lustre use `PathStateManager` (path-keyed, rename pairing, `os.stat` at emit). GPFS uses `GPFSStateManager` (inode-keyed, single-event renames, metadata from events).

## Output Format

Each emitted event is a JSON object:

```json
{"op": "update", "fid": "/tmp/mydir/file.txt", "path": "/tmp/mydir/file.txt", "stat": {"size": 1024, "uid": 501, "gid": 20, "mode": 33188, "atime": 1710423106, "mtime": 1710423106, "ctime": 1710423106}}
{"op": "delete", "fid": "/tmp/mydir/old.txt", "path": "/tmp/mydir/old.txt"}
```

- `update` -- file/dir was created or modified; `stat` is from `os.stat()` at emit time
- `delete` -- file/dir was removed (only emitted if previously emitted as `update`)

## Key Behaviors

- **Event coalescing**: 100 modifications to the same file within a batch produce 1 emitted update
- **Create + delete cancellation**: a file created then deleted in the same batch produces no output (Robinhood-aligned: REMOVED only cancels CREATED, not MODIFIED)
- **Directory rename**: renaming a top-level directory recursively updates all child paths in memory -- no filesystem rescan needed
- **Rename pairing**: fswatch/Lustre emit two events per rename (old path, new path); the state manager pairs them automatically
- **GPFS renames**: GPFS uses single `IN_MOVED_TO` events with inode-keyed state -- no pairing needed

## Python API

```python
# fswatch
from src.icicle import FSWatchSource, StdoutOutputHandler, Monitor

source = FSWatchSource('/tmp/mydir')
monitor = Monitor(source, StdoutOutputHandler())
monitor.run()  # blocks until SIGINT/SIGTERM
```

```python
# Lustre
from src.icicle import LustreChangelogSource, Monitor, StdoutOutputHandler
from src.icicle.batch import BatchProcessor
from src.icicle.fswatch_events import STAT_REDUCTION_RULES

source = LustreChangelogSource(mdt='fs0-MDT0000', mount_point='/mnt/fs0', fsname='fs0')
monitor = Monitor(source, StdoutOutputHandler(), batch=BatchProcessor(rules=STAT_REDUCTION_RULES))
monitor.run()
```

```python
# GPFS
from src.icicle import GPFSKafkaSource, GPFSStateManager, Monitor, StdoutOutputHandler
from src.icicle.batch import BatchProcessor
from src.icicle.gpfs_events import GPFS_REDUCTION_RULES

source = GPFSKafkaSource(topic='mmwatch-events')
batch = BatchProcessor(rules=GPFS_REDUCTION_RULES, slot_key='inode', bypass_rename=False)
monitor = Monitor(source, StdoutOutputHandler(), batch=batch, state=GPFSStateManager())
monitor.run()
```

## Tests

```bash
python -m pytest tests/icicle/ -v       # all (~1,750)
python -m pytest tests/icicle/unit/ -v  # unit only (fast)
```
