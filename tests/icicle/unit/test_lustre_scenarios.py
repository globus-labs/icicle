"""Pipeline integration tests: Lustre-style events through batch + state.

Uses InMemoryEventSource with pre-resolved events (kind=EventKind, path-based)
to verify the full pipeline works with Lustre event patterns.
"""

from __future__ import annotations

from src.icicle.batch import BatchProcessor
from src.icicle.events import EventKind
from src.icicle.fswatch_events import STAT_REDUCTION_RULES
from src.icicle.monitor import Monitor
from src.icicle.state import StateManager
from tests.icicle.conftest import InMemoryEventSource


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ev(
    path: str,
    kind: EventKind,
    is_dir: bool = False,
    ts: str = '00:00:00.000 2026.01.01',
) -> dict:
    return {'path': path, 'kind': kind, 'ts': ts, 'is_dir': is_dir}


class _FakeStat:
    st_size = 100
    st_uid = 1000
    st_gid = 1000
    st_mode = 0o100644
    st_atime = 1000
    st_mtime = 2000
    st_ctime = 3000


def _fake_stat_fn(p):
    return _FakeStat()


def _run_pipeline(
    batches: list[list[dict]],
    emit_after_each: bool = True,
) -> list[dict]:
    """Run events through batch + state and return emitted changes."""
    bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
    sm = StateManager(stat_fn=_fake_stat_fn)
    all_changes: list[dict] = []

    for batch in batches:
        bp.add_events(batch)
        coalesced = bp.flush()
        if coalesced:
            sm.process_events(coalesced)
        if emit_after_each:
            all_changes.extend(sm.emit())

    if not emit_after_each:
        all_changes.extend(sm.emit())
    return all_changes


# ===================================================================
# Basic operations
# ===================================================================
class TestBasicOperations:
    def test_create_file(self) -> None:
        changes = _run_pipeline(
            [
                [
                    _ev('/mnt/fs0/dir/file.txt', EventKind.CREATED),
                ],
            ],
        )
        updates = [c for c in changes if c['op'] == 'update']
        assert len(updates) == 1
        assert updates[0]['path'] == '/mnt/fs0/dir/file.txt'

    def test_create_directory(self) -> None:
        changes = _run_pipeline(
            [
                [
                    _ev('/mnt/fs0/newdir', EventKind.CREATED, is_dir=True),
                ],
            ],
        )
        updates = [c for c in changes if c['op'] == 'update']
        assert len(updates) == 1
        assert updates[0]['path'] == '/mnt/fs0/newdir'

    def test_create_dir_and_nested_file(self) -> None:
        """Lustre pattern: MKDIR then CREAT inside it."""
        changes = _run_pipeline(
            [
                [
                    _ev('/mnt/fs0/mydir', EventKind.CREATED, is_dir=True),
                    _ev('/mnt/fs0/mydir/file.txt', EventKind.CREATED),
                ],
            ],
        )
        paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/mnt/fs0/mydir' in paths
        assert '/mnt/fs0/mydir/file.txt' in paths


# ===================================================================
# Rename (paired RENAMED events, as emitted by LustreChangelogSource)
# ===================================================================
class TestRename:
    def test_file_rename(self) -> None:
        """Lustre source emits two RENAMED events for a file rename."""
        changes = _run_pipeline(
            [
                [_ev('/mnt/fs0/dir/old.txt', EventKind.CREATED)],
                [
                    _ev('/mnt/fs0/dir/old.txt', EventKind.RENAMED),
                    _ev('/mnt/fs0/dir/new.txt', EventKind.RENAMED),
                ],
            ],
        )
        ops = [(c['op'], c['path']) for c in changes]
        assert ('update', '/mnt/fs0/dir/old.txt') in ops
        assert ('delete', '/mnt/fs0/dir/old.txt') in ops
        assert ('update', '/mnt/fs0/dir/new.txt') in ops

    def test_directory_rename_recursive(self) -> None:
        """Rename a directory — children paths update recursively."""
        changes = _run_pipeline(
            [
                [
                    _ev('/mnt/fs0/parent', EventKind.CREATED, is_dir=True),
                    _ev('/mnt/fs0/parent/a.txt', EventKind.CREATED),
                    _ev('/mnt/fs0/parent/b.txt', EventKind.CREATED),
                ],
                [
                    _ev('/mnt/fs0/parent', EventKind.RENAMED, is_dir=True),
                    _ev('/mnt/fs0/renamed', EventKind.RENAMED, is_dir=True),
                ],
            ],
        )
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}

        assert '/mnt/fs0/renamed' in update_paths
        assert '/mnt/fs0/renamed/a.txt' in update_paths
        assert '/mnt/fs0/renamed/b.txt' in update_paths
        assert '/mnt/fs0/parent' in delete_paths
        assert '/mnt/fs0/parent/a.txt' in delete_paths
        assert '/mnt/fs0/parent/b.txt' in delete_paths

    def test_cross_directory_move(self) -> None:
        """Move file from one dir to another (Lustre RENME)."""
        changes = _run_pipeline(
            [
                [
                    _ev('/mnt/fs0/src', EventKind.CREATED, is_dir=True),
                    _ev('/mnt/fs0/dst', EventKind.CREATED, is_dir=True),
                    _ev('/mnt/fs0/src/file.txt', EventKind.CREATED),
                ],
                [
                    _ev('/mnt/fs0/src/file.txt', EventKind.RENAMED),
                    _ev('/mnt/fs0/dst/file.txt', EventKind.RENAMED),
                ],
            ],
        )
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/mnt/fs0/dst/file.txt' in update_paths


# ===================================================================
# Deletion
# ===================================================================
class TestDeletion:
    def test_delete_file(self) -> None:
        changes = _run_pipeline(
            [
                [_ev('/mnt/fs0/dir/file.txt', EventKind.CREATED)],
                [_ev('/mnt/fs0/dir/file.txt', EventKind.REMOVED)],
            ],
        )
        delete_paths = [c['path'] for c in changes if c['op'] == 'delete']
        assert '/mnt/fs0/dir/file.txt' in delete_paths

    def test_recursive_delete(self) -> None:
        """Delete directory removes all tracked children."""
        changes = _run_pipeline(
            [
                [
                    _ev('/mnt/fs0/dir', EventKind.CREATED, is_dir=True),
                    _ev('/mnt/fs0/dir/a.txt', EventKind.CREATED),
                    _ev('/mnt/fs0/dir/b.txt', EventKind.CREATED),
                ],
                [_ev('/mnt/fs0/dir', EventKind.REMOVED, is_dir=True)],
            ],
        )
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/mnt/fs0/dir' in delete_paths
        assert '/mnt/fs0/dir/a.txt' in delete_paths
        assert '/mnt/fs0/dir/b.txt' in delete_paths


# ===================================================================
# Batch reduction
# ===================================================================
class TestBatchReduction:
    def test_ephemeral_create_delete_cancelled(self) -> None:
        """Create + delete in same batch → both cancelled, no output."""
        changes = _run_pipeline(
            [
                [
                    _ev('/mnt/fs0/ephemeral.txt', EventKind.CREATED),
                    _ev('/mnt/fs0/ephemeral.txt', EventKind.REMOVED),
                ],
            ],
        )
        paths = [c['path'] for c in changes]
        assert '/mnt/fs0/ephemeral.txt' not in paths

    def test_multiple_modified_absorbed(self) -> None:
        """Lustre pattern: CREAT → CLOSE → MTIME → SATTR → CTIME
        All map to CREATED + MODIFIED*4.  Reduction absorbs MODIFIED.
        """
        changes = _run_pipeline(
            [
                [
                    _ev('/mnt/fs0/file.txt', EventKind.CREATED),
                    _ev('/mnt/fs0/file.txt', EventKind.MODIFIED),
                    _ev('/mnt/fs0/file.txt', EventKind.MODIFIED),
                    _ev('/mnt/fs0/file.txt', EventKind.MODIFIED),
                    _ev('/mnt/fs0/file.txt', EventKind.MODIFIED),
                ],
            ],
        )
        updates = [c for c in changes if c['op'] == 'update']
        assert len(updates) == 1

    def test_accessed_absorbed_by_created(self) -> None:
        """ACCESSED after CREATED is absorbed."""
        changes = _run_pipeline(
            [
                [
                    _ev('/mnt/fs0/file.txt', EventKind.CREATED),
                    _ev('/mnt/fs0/file.txt', EventKind.MODIFIED),
                ],
            ],
        )
        updates = [c for c in changes if c['op'] == 'update']
        assert len(updates) == 1

    def test_multi_file_independence(self) -> None:
        """Events for different paths don't interfere."""
        changes = _run_pipeline(
            [
                [
                    _ev('/mnt/fs0/a.txt', EventKind.CREATED),
                    _ev('/mnt/fs0/b.txt', EventKind.CREATED),
                    _ev('/mnt/fs0/a.txt', EventKind.MODIFIED),
                    _ev('/mnt/fs0/b.txt', EventKind.REMOVED),
                ],
            ],
        )
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/mnt/fs0/a.txt' in update_paths
        assert '/mnt/fs0/b.txt' not in update_paths


# ===================================================================
# Full lifecycle
# ===================================================================
class TestFullLifecycle:
    def test_create_modify_rename_delete(self) -> None:
        """Simulates: mkdir → creat → write → rename → delete → rmdir."""
        changes = _run_pipeline(
            [
                [
                    _ev('/mnt/fs0/work', EventKind.CREATED, is_dir=True),
                    _ev('/mnt/fs0/work/draft.txt', EventKind.CREATED),
                    _ev('/mnt/fs0/work/draft.txt', EventKind.MODIFIED),
                ],
                [
                    _ev('/mnt/fs0/work/draft.txt', EventKind.RENAMED),
                    _ev('/mnt/fs0/work/final.txt', EventKind.RENAMED),
                ],
                [
                    _ev('/mnt/fs0/work/final.txt', EventKind.REMOVED),
                    _ev('/mnt/fs0/work', EventKind.REMOVED, is_dir=True),
                ],
            ],
        )
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/mnt/fs0/work/draft.txt' in delete_paths
        assert '/mnt/fs0/work/final.txt' in delete_paths
        assert '/mnt/fs0/work' in delete_paths


# ===================================================================
# Monitor integration (with InMemoryEventSource)
# ===================================================================
class TestMonitorIntegration:
    def test_monitor_with_lustre_events(self) -> None:
        """Full Monitor pipeline with Lustre-pattern events."""
        batches = [
            [
                _ev('/mnt/fs0/test', EventKind.CREATED, is_dir=True),
                _ev('/mnt/fs0/test/file.txt', EventKind.CREATED),
            ],
            [
                _ev('/mnt/fs0/test/file.txt', EventKind.MODIFIED),
            ],
        ]
        source = InMemoryEventSource(batches)
        captured: list[dict] = []

        class CapturingOutput:
            def send(self, payloads: list[dict]) -> None:
                captured.extend(payloads)

            def flush(self) -> None:
                pass

            def close(self) -> None:
                pass

        monitor = Monitor(
            source,
            CapturingOutput(),
            state=StateManager(stat_fn=_fake_stat_fn),
        )
        monitor.run()

        update_paths = {c['path'] for c in captured if c['op'] == 'update'}
        assert '/mnt/fs0/test' in update_paths
        assert '/mnt/fs0/test/file.txt' in update_paths
