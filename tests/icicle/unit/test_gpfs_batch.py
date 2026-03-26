"""Tests for BatchProcessor with GPFS-specific configuration."""

from __future__ import annotations

from src.icicle.batch import BatchProcessor
from src.icicle.events import EventKind
from src.icicle.gpfs_events import GPFS_REDUCTION_RULES


def ev(inode: int, path: str, kind: EventKind, **kw) -> dict:
    """Helper to create GPFS-style event dicts."""
    return {
        'inode': inode,
        'path': path,
        'kind': kind,
        'is_dir': False,
        **kw,
    }


def gpfs_bp() -> BatchProcessor:
    """Create a BatchProcessor configured for GPFS."""
    return BatchProcessor(
        rules=GPFS_REDUCTION_RULES,
        slot_key='inode',
        bypass_rename=False,
    )


# ---------------------------------------------------------------------------
# Inode-keyed slotting
# ---------------------------------------------------------------------------


class TestInodeSlotting:
    def test_same_inode_same_slot(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/a.txt', EventKind.CREATED),
                ev(100, '/gpfs/a.txt', EventKind.MODIFIED),
            ],
        )
        # With GPFS rules, MODIFIED has no rule so both pass through
        batch = bp.flush()
        assert len(batch) == 2

    def test_different_inodes_different_slots(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/a.txt', EventKind.CREATED),
                ev(200, '/gpfs/b.txt', EventKind.CREATED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 2

    def test_same_path_different_inodes_not_coalesced(self):
        """Different inodes with same path go to different slots."""
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/file.txt', EventKind.CREATED),
                ev(200, '/gpfs/file.txt', EventKind.CREATED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 2


# ---------------------------------------------------------------------------
# GPFS reduction: only REMOVED cancels CREATED
# ---------------------------------------------------------------------------


class TestGPFSReduction:
    def test_removed_cancels_created(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/tmp.txt', EventKind.CREATED),
                ev(100, '/gpfs/tmp.txt', EventKind.REMOVED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 0
        assert bp.reduced == 2  # both events reduced

    def test_modified_not_ignored_after_created(self):
        """Unlike fswatch, GPFS does NOT ignore MODIFIED after CREATED."""
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/file.txt', EventKind.CREATED),
                ev(100, '/gpfs/file.txt', EventKind.MODIFIED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 2  # both pass through

    def test_modified_not_ignored_after_modified(self):
        """GPFS has no ignore rules — every MODIFIED passes through."""
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/file.txt', EventKind.MODIFIED),
                ev(100, '/gpfs/file.txt', EventKind.MODIFIED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 2

    def test_accessed_not_ignored(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/file.txt', EventKind.CREATED),
                ev(100, '/gpfs/file.txt', EventKind.MODIFIED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 2

    def test_removed_only_cancels_created_not_modified(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/file.txt', EventKind.MODIFIED),
                ev(100, '/gpfs/file.txt', EventKind.REMOVED),
            ],
        )
        batch = bp.flush()
        # REMOVED only cancels CREATED, not MODIFIED
        assert len(batch) == 2


# ---------------------------------------------------------------------------
# RENAMED not bypassed (GPFS mode)
# ---------------------------------------------------------------------------


class TestRenamedNotBypassed:
    def test_renamed_goes_through_slotting(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/new.txt', EventKind.RENAMED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['kind'] == EventKind.RENAMED

    def test_renamed_slotted_by_inode(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/a.txt', EventKind.CREATED),
                ev(100, '/gpfs/b.txt', EventKind.RENAMED),
            ],
        )
        batch = bp.flush()
        # Both in same slot (inode 100), no reduction rule applies
        assert len(batch) == 2


# ---------------------------------------------------------------------------
# Backward compatibility (default params = fswatch behavior)
# ---------------------------------------------------------------------------


class TestDefaultBehavior:
    def test_default_bp_no_rules_passes_all(self):
        bp = BatchProcessor()
        bp.add_events(
            [
                {
                    'path': '/a',
                    'kind': EventKind.CREATED,
                    'ts': 0.0,
                    'is_dir': False,
                },
                {
                    'path': '/a',
                    'kind': EventKind.MODIFIED,
                    'ts': 0.0,
                    'is_dir': False,
                },
            ],
        )
        batch = bp.flush()
        # Default: no rules, all events pass through
        assert len(batch) == 2

    def test_default_bp_bypasses_rename(self):
        bp = BatchProcessor()
        bp.add_events(
            [
                {
                    'path': '/old',
                    'kind': EventKind.RENAMED,
                    'ts': 0.0,
                    'is_dir': False,
                },
                {
                    'path': '/new',
                    'kind': EventKind.RENAMED,
                    'ts': 0.0,
                    'is_dir': False,
                },
            ],
        )
        batch = bp.flush()
        assert len(batch) == 2
        # Renames appended at end, in order
        assert batch[0]['path'] == '/old'
        assert batch[1]['path'] == '/new'


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------


class TestCounters:
    def test_counters_on_cancel(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/tmp.txt', EventKind.CREATED),
                ev(100, '/gpfs/tmp.txt', EventKind.REMOVED),
            ],
        )
        assert bp.received == 2
        assert bp.reduced == 2
        assert bp.accepted == 0

    def test_counters_on_accept(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/a.txt', EventKind.CREATED),
                ev(200, '/gpfs/b.txt', EventKind.MODIFIED),
            ],
        )
        assert bp.received == 2
        assert bp.reduced == 0
        assert bp.accepted == 2

    def test_counters_multiple_cancels(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/a', EventKind.CREATED),
                ev(100, '/a', EventKind.REMOVED),
                ev(200, '/b', EventKind.CREATED),
                ev(200, '/b', EventKind.REMOVED),
                ev(300, '/c', EventKind.CREATED),
            ],
        )
        assert bp.received == 5
        assert bp.reduced == 4
        assert bp.accepted == 1


# ---------------------------------------------------------------------------
# Reduction edge cases
# ---------------------------------------------------------------------------


class TestReductionEdgeCases:
    def test_empty_slot_returns_accept(self):
        bp = gpfs_bp()
        bp.add_events([ev(100, '/gpfs/f.txt', EventKind.REMOVED)])
        batch = bp.flush()
        assert len(batch) == 1

    def test_cancel_removes_correct_prior_event(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/f.txt', EventKind.MODIFIED),
                ev(100, '/gpfs/f.txt', EventKind.CREATED),
                ev(100, '/gpfs/f.txt', EventKind.MODIFIED),
                ev(100, '/gpfs/f.txt', EventKind.REMOVED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 2
        assert all(e['kind'] == EventKind.MODIFIED for e in batch)

    def test_two_create_delete_cycles_same_inode(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/f.txt', EventKind.CREATED),
                ev(100, '/gpfs/f.txt', EventKind.REMOVED),
                ev(100, '/gpfs/f2.txt', EventKind.CREATED),
                ev(100, '/gpfs/f2.txt', EventKind.REMOVED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 0

    def test_removed_without_prior_created(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/f.txt', EventKind.MODIFIED),
                ev(100, '/gpfs/f.txt', EventKind.REMOVED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 2

    def test_many_events_same_inode(self):
        bp = gpfs_bp()
        events = [
            ev(100, '/gpfs/f.txt', EventKind.MODIFIED) for _ in range(10)
        ]
        bp.add_events(events)
        batch = bp.flush()
        assert len(batch) == 10

    def test_commit_clears_state(self):
        bp = gpfs_bp()
        bp.add_events([ev(100, '/gpfs/f.txt', EventKind.CREATED)])
        assert len(bp.flush()) == 1
        assert len(bp.flush()) == 0

    def test_mixed_inodes_independent_reduction(self):
        bp = gpfs_bp()
        bp.add_events(
            [
                ev(100, '/gpfs/a.txt', EventKind.CREATED),
                ev(200, '/gpfs/b.txt', EventKind.CREATED),
                ev(100, '/gpfs/a.txt', EventKind.REMOVED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['inode'] == 200
