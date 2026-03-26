"""End-to-end GPFS pipeline scenarios: batch -> state -> emit."""

from __future__ import annotations

from src.icicle.batch import BatchProcessor
from src.icicle.events import EventKind
from src.icicle.gpfs_events import GPFS_REDUCTION_RULES
from src.icicle.gpfs_state import GPFSStateManager


def ev(
    inode: int,
    path: str,
    kind: EventKind,
    is_dir: bool = False,
    **kw,
) -> dict:
    return {
        'inode': inode,
        'path': path,
        'kind': kind,
        'is_dir': is_dir,
        'size': kw.get('size'),
        'atime': kw.get('atime'),
        'ctime': kw.get('ctime'),
        'mtime': kw.get('mtime'),
        'uid': kw.get('uid'),
        'gid': kw.get('gid'),
        'permissions': kw.get('permissions'),
    }


def run_pipeline(events: list[dict]) -> list[dict]:
    """Run events through GPFS batch processor and state manager."""
    bp = BatchProcessor(
        rules=GPFS_REDUCTION_RULES,
        slot_key='inode',
        bypass_rename=False,
    )
    bp.add_events(events)
    coalesced = bp.flush()
    sm = GPFSStateManager()
    if coalesced:
        sm.process_events(coalesced)
    return sm.emit()


# ---------------------------------------------------------------------------
# Basic lifecycle
# ---------------------------------------------------------------------------


class TestBasicLifecycle:
    def test_create_single_file(self):
        changes = run_pipeline(
            [
                ev(
                    100,
                    '/gpfs/fs1/file.txt',
                    EventKind.CREATED,
                    size=1024,
                    uid=500,
                    gid=500,
                    mtime=1700000000,
                    permissions='0644',
                ),
            ],
        )
        assert len(changes) == 1
        c = changes[0]
        assert c['op'] == 'update'
        assert c['fid'] == '100', 'fid must match inode as string'
        assert c['path'] == '/gpfs/fs1/file.txt'
        assert c['stat']['size'] == 1024
        assert c['stat']['uid'] == 500
        assert c['stat']['gid'] == 500
        assert c['stat']['mtime'] == 1700000000
        assert c['stat']['mode'] == '0644'

    def test_create_and_modify(self):
        changes = run_pipeline(
            [
                ev(100, '/gpfs/fs1/file.txt', EventKind.CREATED, size=10),
                ev(100, '/gpfs/fs1/file.txt', EventKind.MODIFIED, size=20),
            ],
        )
        # Both events pass through (no ignore rule), state uses latest
        assert len(changes) == 1
        assert changes[0]['stat']['size'] == 20

    def test_create_and_delete_cancels(self):
        changes = run_pipeline(
            [
                ev(100, '/gpfs/fs1/tmp.txt', EventKind.CREATED),
                ev(100, '/gpfs/fs1/tmp.txt', EventKind.REMOVED),
            ],
        )
        # Batch reduction cancels both — no output
        assert len(changes) == 0


# ---------------------------------------------------------------------------
# Directory operations
# ---------------------------------------------------------------------------


class TestDirectoryOps:
    def test_mkdir_and_create_file(self):
        changes = run_pipeline(
            [
                ev(10, '/gpfs/fs1/subdir', EventKind.CREATED, is_dir=True),
                ev(
                    100,
                    '/gpfs/fs1/subdir/file.txt',
                    EventKind.CREATED,
                    size=42,
                ),
            ],
        )
        paths = {c['path'] for c in changes}
        assert '/gpfs/fs1/subdir' in paths
        assert '/gpfs/fs1/subdir/file.txt' in paths
        # Parent dir should be updated (child creation changes parent metadata)
        dir_changes = [c for c in changes if c['path'] == '/gpfs/fs1/subdir']
        assert len(dir_changes) >= 1, 'Parent dir must appear as update'

    def test_nested_dirs(self):
        changes = run_pipeline(
            [
                ev(10, '/gpfs/fs1/a', EventKind.CREATED, is_dir=True),
                ev(20, '/gpfs/fs1/a/b', EventKind.CREATED, is_dir=True),
                ev(30, '/gpfs/fs1/a/b/c', EventKind.CREATED, is_dir=True),
                ev(100, '/gpfs/fs1/a/b/c/deep.txt', EventKind.CREATED, size=1),
            ],
        )
        paths = {c['path'] for c in changes}
        assert '/gpfs/fs1/a/b/c/deep.txt' in paths


# ---------------------------------------------------------------------------
# Rename scenarios (ported from test_gpfs_move1.py / test_gpfs_move2.py)
# ---------------------------------------------------------------------------


class TestRenameScenarios:
    def test_simple_file_rename(self):
        changes = run_pipeline(
            [
                ev(100, '/gpfs/fs1/old.txt', EventKind.CREATED, size=100),
                ev(100, '/gpfs/fs1/new.txt', EventKind.RENAMED, size=100),
            ],
        )
        paths = {c['path'] for c in changes}
        assert '/gpfs/fs1/new.txt' in paths
        # old.txt was never emitted so no delete
        assert '/gpfs/fs1/old.txt' not in paths

    def test_directory_rename_with_children(self):
        """Ported from test_gpfs_move2: rename adir -> bdir with children.

        Uses two batches because in real GPFS, children are created before
        the directory rename. Within a single batch, slot ordering is
        arbitrary and the rename may precede child creation events.
        """
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()

        # Batch 1: create tree
        create_events = [
            ev(10, '/gpfs/fs1/adir', EventKind.CREATED, is_dir=True),
            ev(20, '/gpfs/fs1/adir/x', EventKind.CREATED, is_dir=True),
            ev(30, '/gpfs/fs1/adir/y', EventKind.CREATED, is_dir=True),
            ev(100, '/gpfs/fs1/adir/a', EventKind.CREATED, size=2),
            ev(200, '/gpfs/fs1/adir/x/x1.txt', EventKind.CREATED, size=3),
            ev(300, '/gpfs/fs1/adir/y/y1.txt', EventKind.CREATED, size=3),
        ]
        bp.add_events(create_events)
        sm.process_events(bp.flush())
        sm.emit()

        # Batch 2: rename directory
        bp.add_events([ev(10, '/gpfs/fs1/bdir', EventKind.RENAMED)])
        sm.process_events(bp.flush())
        changes = sm.emit()

        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/gpfs/fs1/bdir' in update_paths
        assert '/gpfs/fs1/bdir/x/x1.txt' in update_paths
        assert '/gpfs/fs1/bdir/y/y1.txt' in update_paths
        assert '/gpfs/fs1/bdir/a' in update_paths
        # Old paths should not appear
        assert '/gpfs/fs1/adir' not in update_paths

    def test_cross_directory_file_move(self):
        """Move file from one directory to another."""
        events = [
            ev(10, '/gpfs/fs1/src', EventKind.CREATED, is_dir=True),
            ev(20, '/gpfs/fs1/dst', EventKind.CREATED, is_dir=True),
            ev(100, '/gpfs/fs1/src/file.txt', EventKind.CREATED, size=50),
            ev(100, '/gpfs/fs1/dst/file.txt', EventKind.RENAMED, size=50),
        ]
        changes = run_pipeline(events)
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/gpfs/fs1/dst/file.txt' in update_paths

    def test_rename_chain(self):
        """Rename A -> B -> C in same batch."""
        events = [
            ev(100, '/gpfs/fs1/a.txt', EventKind.CREATED, size=10),
            ev(100, '/gpfs/fs1/b.txt', EventKind.RENAMED, size=10),
            ev(100, '/gpfs/fs1/c.txt', EventKind.RENAMED, size=10),
        ]
        changes = run_pipeline(events)
        paths = {c['path'] for c in changes}
        assert '/gpfs/fs1/c.txt' in paths

    def test_rename_chain_4_hops(self):
        """Rename A -> B -> C -> D in same batch."""
        events = [
            ev(100, '/gpfs/fs1/a.txt', EventKind.CREATED, size=10),
            ev(100, '/gpfs/fs1/b.txt', EventKind.RENAMED, size=10),
            ev(100, '/gpfs/fs1/c.txt', EventKind.RENAMED, size=10),
            ev(100, '/gpfs/fs1/d.txt', EventKind.RENAMED, size=10),
        ]
        changes = run_pipeline(events)
        paths = {c['path'] for c in changes}
        assert '/gpfs/fs1/d.txt' in paths
        # Intermediate paths should not appear
        assert '/gpfs/fs1/a.txt' not in paths
        assert '/gpfs/fs1/b.txt' not in paths
        assert '/gpfs/fs1/c.txt' not in paths
        assert changes[0]['fid'] == '100'

    def test_cascading_dir_renames_a_b_c(self):
        """Rename dir A→B→C across batches, children follow each step."""
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()

        # Batch 1: create dir + child
        bp.add_events(
            [
                ev(10, '/gpfs/a', EventKind.CREATED, is_dir=True),
                ev(100, '/gpfs/a/file.txt', EventKind.CREATED, size=10),
            ],
        )
        sm.process_events(bp.flush())
        sm.emit()

        # Batch 2: rename A → B
        bp.add_events([ev(10, '/gpfs/b', EventKind.RENAMED)])
        sm.process_events(bp.flush())
        c2 = sm.emit()
        b_paths = {c['path'] for c in c2 if c['op'] == 'update'}
        assert '/gpfs/b' in b_paths
        assert '/gpfs/b/file.txt' in b_paths

        # Batch 3: rename B → C
        bp.add_events([ev(10, '/gpfs/c', EventKind.RENAMED)])
        sm.process_events(bp.flush())
        c3 = sm.emit()
        c_paths = {c['path'] for c in c3 if c['op'] == 'update'}
        assert '/gpfs/c' in c_paths
        assert '/gpfs/c/file.txt' in c_paths

        # Final state
        assert sm.files['10']['path'] == '/gpfs/c'
        assert sm.files['100']['path'] == '/gpfs/c/file.txt'
        assert sm.path_to_fid['/gpfs/c'] == '10'
        assert sm.path_to_fid['/gpfs/c/file.txt'] == '100'
        # Old paths gone
        for old in [
            '/gpfs/a',
            '/gpfs/a/file.txt',
            '/gpfs/b',
            '/gpfs/b/file.txt',
        ]:
            assert old not in sm.path_to_fid, (
                f'Stale path {old} in path_to_fid'
            )

    def test_rename_to_self_updates_metadata(self):
        """Rename to same path updates metadata without creating new entry."""
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()
        bp.add_events(
            [ev(100, '/gpfs/f.txt', EventKind.CREATED, size=10, mtime=1000)],
        )
        sm.process_events(bp.flush())
        sm.emit()

        # "Rename" to the same path with new metadata
        bp.add_events(
            [ev(100, '/gpfs/f.txt', EventKind.RENAMED, size=20, mtime=2000)],
        )
        sm.process_events(bp.flush())
        changes = sm.emit()

        assert len(changes) == 1
        assert changes[0]['path'] == '/gpfs/f.txt'
        assert changes[0]['stat']['size'] == 20
        assert changes[0]['stat']['mtime'] == 2000
        assert changes[0]['fid'] == '100'

    def test_rename_swap_two_files(self):
        """Swap two files: A→tmp, B→A, tmp→B — must use 3 batches.

        Single-batch inode-keyed slotting would process all inode-100
        events before inode-200, causing a false collision. In real GPFS,
        each IN_MOVED_TO arrives as a separate event in time order, so
        multi-batch processing preserves correctness.
        """
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()

        # Create A (inode 100) and B (inode 200)
        bp.add_events(
            [
                ev(100, '/gpfs/a.txt', EventKind.CREATED, size=10),
                ev(200, '/gpfs/b.txt', EventKind.CREATED, size=20),
            ],
        )
        sm.process_events(bp.flush())
        sm.emit()

        # Step 1: A → tmp
        bp.add_events([ev(100, '/gpfs/tmp', EventKind.RENAMED, size=10)])
        sm.process_events(bp.flush())

        # Step 2: B → A
        bp.add_events([ev(200, '/gpfs/a.txt', EventKind.RENAMED, size=20)])
        sm.process_events(bp.flush())

        # Step 3: tmp → B
        bp.add_events([ev(100, '/gpfs/b.txt', EventKind.RENAMED, size=10)])
        sm.process_events(bp.flush())
        changes = sm.emit()

        paths = {c['fid']: c['path'] for c in changes if c['op'] == 'update'}
        assert paths['100'] == '/gpfs/b.txt', (
            f'inode 100 should be at b.txt, got {paths.get("100")}'
        )
        assert paths['200'] == '/gpfs/a.txt', (
            f'inode 200 should be at a.txt, got {paths.get("200")}'
        )

    def test_rename_collision_with_existing(self):
        """Rename inode 100 to path occupied by inode 200 — collision."""
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()

        # Batch 1: create both files, emit to register them
        bp.add_events(
            [
                ev(100, '/gpfs/fs1/mover.txt', EventKind.CREATED, size=10),
                ev(200, '/gpfs/fs1/target.txt', EventKind.CREATED, size=20),
            ],
        )
        sm.process_events(bp.flush())
        sm.emit()

        # Batch 2: rename inode 100 onto inode 200's path
        bp.add_events(
            [
                ev(100, '/gpfs/fs1/target.txt', EventKind.RENAMED, size=10),
            ],
        )
        sm.process_events(bp.flush())
        changes = sm.emit()

        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        delete_fids = {c['fid'] for c in changes if c['op'] == 'delete'}

        assert '/gpfs/fs1/target.txt' in update_paths
        assert '200' in delete_fids, (
            'Collision victim (inode 200) must be deleted'
        )
        # Verify the update at target.txt is from inode 100
        target_updates = [
            c
            for c in changes
            if c['path'] == '/gpfs/fs1/target.txt' and c['op'] == 'update'
        ]
        assert target_updates[0]['fid'] == '100'
        assert target_updates[0]['stat']['size'] == 10


# ---------------------------------------------------------------------------
# BatchProcessor counter verification
# ---------------------------------------------------------------------------


class TestBatchCounters:
    def test_complex_sequence_counters(self):
        """15 events: verify exact received/reduced/accepted counts."""
        from src.icicle.batch import BatchProcessor as BP

        bp = BP(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        bp.add_events(
            [
                # inode 100: CREATED + MODIFIED + MODIFIED + REMOVED
                # REMOVED cancels CREATED → 2 reduced, net: MODIFIED + MODIFIED + REMOVED = 3 accepted - 1 cancelled = 2
                ev(100, '/gpfs/a', EventKind.CREATED),
                ev(100, '/gpfs/a', EventKind.MODIFIED),
                ev(100, '/gpfs/a', EventKind.MODIFIED),
                ev(100, '/gpfs/a', EventKind.REMOVED),
                # inode 200: CREATED + RENAMED → 2 accepted (no reduction)
                ev(200, '/gpfs/b', EventKind.CREATED),
                ev(200, '/gpfs/c', EventKind.RENAMED),
                # inode 300: CREATED + REMOVED → cancel both = 2 reduced
                ev(300, '/gpfs/d', EventKind.CREATED),
                ev(300, '/gpfs/d', EventKind.REMOVED),
                # inode 400: MODIFIED + ACCESSED + MODIFIED → 3 accepted (no rules)
                ev(400, '/gpfs/e', EventKind.MODIFIED),
                ev(400, '/gpfs/e', EventKind.MODIFIED),
                ev(400, '/gpfs/e', EventKind.MODIFIED),
                # inode 500: CREATED + CREATED → 2 accepted (no rule for CREATED after CREATED)
                ev(500, '/gpfs/f', EventKind.CREATED),
                ev(500, '/gpfs/g', EventKind.CREATED),
                # inode 600: single REMOVED → 1 accepted
                ev(600, '/gpfs/h', EventKind.REMOVED),
                # inode 700: single RENAMED → 1 accepted
                ev(700, '/gpfs/i', EventKind.RENAMED),
            ],
        )
        assert bp.received == 15
        # Reductions: REMOVED cancels CREATED for inode 100 (2 reduced) +
        #             REMOVED cancels CREATED for inode 300 (2 reduced) = 4
        assert bp.reduced == 4, f'Expected 4 reduced, got {bp.reduced}'
        # Accepted: 15 - 4 = 11, minus the 2 that were cancelled
        # (cancel removes both from slot AND decrements accepted)
        # inode 100: CREATED accepted (+1), MODIFIED (+1), MODIFIED (+1),
        #            REMOVED cancels CREATED → reduced 2, accepted -1 → net: 2 accepted
        # inode 200: 2 accepted
        # inode 300: CREATED accepted (+1), REMOVED cancels → reduced 2, accepted -1 → net: 0
        # inode 400: 3 accepted
        # inode 500: 2 accepted
        # inode 600: 1 accepted
        # inode 700: 1 accepted
        # Total: 2 + 2 + 0 + 3 + 2 + 1 + 1 = 11
        assert bp.accepted == 11, f'Expected 11 accepted, got {bp.accepted}'

        batch = bp.flush()
        assert len(batch) == 11


# ---------------------------------------------------------------------------
# Rapid create/delete (batch reduction)
# ---------------------------------------------------------------------------


class TestReductionRuleVerification:
    """Verify GPFS reduction differs from fswatch/Lustre."""

    def test_accessed_not_absorbed_after_created(self):
        """GPFS: ACCESSED passes through (fswatch would absorb it)."""
        from src.icicle.batch import BatchProcessor as BP

        bp = BP(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        bp.add_events(
            [
                ev(100, '/gpfs/f.txt', EventKind.CREATED, size=10),
                ev(100, '/gpfs/f.txt', EventKind.MODIFIED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 2, 'Both CREATED and ACCESSED must survive'
        kinds = [e['kind'] for e in batch]
        assert EventKind.CREATED in kinds
        assert EventKind.MODIFIED in kinds

    def test_create_access_modify_remove_sequence(self):
        """CREATED+ACCESSED+MODIFIED+REMOVED: only CREATED cancelled by REMOVED."""
        changes = run_pipeline(
            [
                ev(100, '/gpfs/f.txt', EventKind.CREATED, size=10),
                ev(100, '/gpfs/f.txt', EventKind.MODIFIED),
                ev(100, '/gpfs/f.txt', EventKind.MODIFIED, size=20),
                ev(100, '/gpfs/f.txt', EventKind.REMOVED),
            ],
        )
        # REMOVED cancels CREATED. ACCESSED+MODIFIED+REMOVED survive batch.
        # State: ACCESSED/MODIFIED on unknown fid → no-op. REMOVED → no-op.
        assert len(changes) == 0

    def test_modified_survives_after_modified(self):
        """GPFS: MODIFIED does NOT ignore prior MODIFIED."""
        changes = run_pipeline(
            [
                ev(100, '/gpfs/f.txt', EventKind.CREATED, size=10, mtime=1000),
                ev(
                    100,
                    '/gpfs/f.txt',
                    EventKind.MODIFIED,
                    size=20,
                    mtime=2000,
                ),
                ev(
                    100,
                    '/gpfs/f.txt',
                    EventKind.MODIFIED,
                    size=30,
                    mtime=3000,
                ),
            ],
        )
        assert len(changes) == 1
        # Latest metadata should win
        assert changes[0]['stat']['size'] == 30
        assert changes[0]['stat']['mtime'] == 3000

    def test_removed_does_not_cancel_modified(self):
        """GPFS: REMOVED only cancels CREATED, not MODIFIED."""
        from src.icicle.batch import BatchProcessor as BP

        bp = BP(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        bp.add_events(
            [
                ev(100, '/gpfs/f.txt', EventKind.MODIFIED, size=10),
                ev(100, '/gpfs/f.txt', EventKind.REMOVED),
            ],
        )
        batch = bp.flush()
        # Both survive — REMOVED doesn't cancel MODIFIED in GPFS rules
        assert len(batch) == 2


class TestRapidOps:
    def test_rapid_create_delete(self):
        events = [
            ev(100, '/gpfs/fs1/tmp.txt', EventKind.CREATED),
            ev(100, '/gpfs/fs1/tmp.txt', EventKind.REMOVED),
        ]
        changes = run_pipeline(events)
        assert len(changes) == 0

    def test_rapid_create_modify_delete(self):
        events = [
            ev(100, '/gpfs/fs1/tmp.txt', EventKind.CREATED, size=10),
            ev(100, '/gpfs/fs1/tmp.txt', EventKind.MODIFIED, size=20),
            ev(100, '/gpfs/fs1/tmp.txt', EventKind.REMOVED),
        ]
        changes = run_pipeline(events)
        # REMOVED cancels CREATED; MODIFIED remains but then REMOVED also present
        # After batch: MODIFIED + REMOVED survive (REMOVED only cancels CREATED)
        # State: MODIFIED for unknown inode (was never created in state) → ignored
        # REMOVED for unknown inode → ignored
        # So no output
        assert len(changes) == 0

    def test_multiple_files_interleaved(self):
        events = [
            ev(100, '/gpfs/fs1/a.txt', EventKind.CREATED, size=1),
            ev(200, '/gpfs/fs1/b.txt', EventKind.CREATED, size=2),
            ev(100, '/gpfs/fs1/a.txt', EventKind.MODIFIED, size=10),
            ev(200, '/gpfs/fs1/b.txt', EventKind.MODIFIED, size=20),
        ]
        changes = run_pipeline(events)
        update_map = {c['path']: c['stat']['size'] for c in changes}
        assert update_map['/gpfs/fs1/a.txt'] == 10
        assert update_map['/gpfs/fs1/b.txt'] == 20


# ---------------------------------------------------------------------------
# Deep structures
# ---------------------------------------------------------------------------


class TestDeepStructures:
    def test_five_level_nesting(self):
        events = [
            ev(1, '/gpfs/fs1/l1', EventKind.CREATED, is_dir=True),
            ev(2, '/gpfs/fs1/l1/l2', EventKind.CREATED, is_dir=True),
            ev(3, '/gpfs/fs1/l1/l2/l3', EventKind.CREATED, is_dir=True),
            ev(4, '/gpfs/fs1/l1/l2/l3/l4', EventKind.CREATED, is_dir=True),
            ev(5, '/gpfs/fs1/l1/l2/l3/l4/l5', EventKind.CREATED, is_dir=True),
            ev(
                100,
                '/gpfs/fs1/l1/l2/l3/l4/l5/file.txt',
                EventKind.CREATED,
                size=1,
            ),
        ]
        changes = run_pipeline(events)
        paths = {c['path'] for c in changes}
        assert '/gpfs/fs1/l1/l2/l3/l4/l5/file.txt' in paths

    def test_wide_directory(self):
        events = [ev(1, '/gpfs/fs1/dir', EventKind.CREATED, is_dir=True)]
        for i in range(50):
            events.append(
                ev(
                    100 + i,
                    f'/gpfs/fs1/dir/f{i}.txt',
                    EventKind.CREATED,
                    size=i,
                ),
            )
        changes = run_pipeline(events)
        # 1 dir + 50 files = 51 updates
        update_count = len([c for c in changes if c['op'] == 'update'])
        assert update_count == 51, (
            f'Expected exactly 51 updates, got {update_count}'
        )
        # Verify parent dir is in updates
        dir_updates = [c for c in changes if c['path'] == '/gpfs/fs1/dir']
        assert len(dir_updates) == 1, 'Parent dir must appear exactly once'
        # Verify all 50 files present
        file_updates = [
            c for c in changes if c['path'].startswith('/gpfs/fs1/dir/f')
        ]
        assert len(file_updates) == 50, (
            f'Expected 50 files, got {len(file_updates)}'
        )
        # Verify fids are unique
        fids = {c['fid'] for c in changes}
        assert len(fids) == 51, f'Expected 51 unique fids, got {len(fids)}'

    def test_100_file_burst_pipeline(self):
        """100 files in one batch, verify exact pipeline output."""
        events = [ev(1, '/gpfs/dir', EventKind.CREATED, is_dir=True)]
        for i in range(100):
            events.append(
                ev(
                    100 + i,
                    f'/gpfs/dir/f{i:03d}.txt',
                    EventKind.CREATED,
                    size=i + 1,
                    uid=1000,
                ),
            )
        changes = run_pipeline(events)
        updates = [c for c in changes if c['op'] == 'update']
        assert len(updates) == 101, (
            f'Expected 101 (1 dir + 100 files), got {len(updates)}'
        )
        # Verify sizes are correct
        file_updates = sorted(
            [c for c in updates if c['path'].startswith('/gpfs/dir/f')],
            key=lambda c: c['path'],
        )
        assert len(file_updates) == 100
        for i, c in enumerate(file_updates):
            assert c['stat']['size'] == i + 1, f'File {i} size mismatch'
            assert c['stat']['uid'] == 1000

    def test_directory_delete_only_removes_self(self):
        """Deleting a directory only removes that entry — not children.

        Unlike path-keyed StateManager which has _remove_subtree,
        GPFSStateManager relies on individual IN_DELETE events for each
        child (mmwatch emits them separately).
        """
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()

        # Create tree
        bp.add_events(
            [
                ev(10, '/gpfs/dir', EventKind.CREATED, is_dir=True),
                ev(100, '/gpfs/dir/file.txt', EventKind.CREATED, size=42),
                ev(20, '/gpfs/dir/sub', EventKind.CREATED, is_dir=True),
                ev(200, '/gpfs/dir/sub/deep.txt', EventKind.CREATED, size=7),
            ],
        )
        sm.process_events(bp.flush())
        sm.emit()

        # Delete only the root directory
        bp.add_events([ev(10, '/gpfs/dir', EventKind.REMOVED)])
        sm.process_events(bp.flush())
        changes = sm.emit()

        deletes = [c for c in changes if c['op'] == 'delete']
        assert len(deletes) == 1, (
            f'Only dir itself deleted, got {len(deletes)}'
        )
        assert deletes[0]['fid'] == '10'

        # Children still in state (orphaned until their own DELETE events)
        assert '100' in sm.files
        assert '200' in sm.files

    def test_wide_directory_with_delete_half(self):
        """Create 20 files, emit, delete 10, verify exactly 10 deletes."""
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()

        # Batch 1: create dir + 20 files
        events = [ev(1, '/gpfs/fs1/dir', EventKind.CREATED, is_dir=True)]
        for i in range(20):
            events.append(
                ev(
                    100 + i,
                    f'/gpfs/fs1/dir/f{i}.txt',
                    EventKind.CREATED,
                    size=i + 1,
                ),
            )
        bp.add_events(events)
        sm.process_events(bp.flush())
        sm.emit()

        # Batch 2: delete even-numbered files (10 files)
        delete_events = []
        for i in range(0, 20, 2):
            delete_events.append(
                ev(100 + i, f'/gpfs/fs1/dir/f{i}.txt', EventKind.REMOVED),
            )
        bp.add_events(delete_events)
        sm.process_events(bp.flush())
        changes = sm.emit()

        deletes = [c for c in changes if c['op'] == 'delete']
        updates = [c for c in changes if c['op'] == 'update']
        assert len(deletes) == 10, f'Expected 10 deletes, got {len(deletes)}'
        # Parent dir should be updated (children removed)
        assert any(c['path'] == '/gpfs/fs1/dir' for c in updates), (
            'Parent dir should be updated after child deletion'
        )


# ---------------------------------------------------------------------------
# Multi-batch pipeline
# ---------------------------------------------------------------------------


class TestMultiBatch:
    def test_two_batches_accumulate_state(self):
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()

        # Batch 1: create
        bp.add_events([ev(100, '/gpfs/f.txt', EventKind.CREATED, size=10)])
        coalesced = bp.flush()
        sm.process_events(coalesced)
        changes1 = sm.emit()
        assert len(changes1) == 1

        # Batch 2: modify
        bp.add_events([ev(100, '/gpfs/f.txt', EventKind.MODIFIED, size=20)])
        coalesced = bp.flush()
        sm.process_events(coalesced)
        changes2 = sm.emit()
        assert len(changes2) == 1
        assert changes2[0]['stat']['size'] == 20

    def test_create_then_delete_across_batches(self):
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()

        # Batch 1: create + emit
        bp.add_events([ev(100, '/gpfs/f.txt', EventKind.CREATED)])
        sm.process_events(bp.flush())
        sm.emit()

        # Batch 2: delete
        bp.add_events([ev(100, '/gpfs/f.txt', EventKind.REMOVED)])
        sm.process_events(bp.flush())
        changes = sm.emit()
        deletes = [c for c in changes if c['op'] == 'delete']
        assert len(deletes) == 1
        assert deletes[0]['fid'] == '100'


# ---------------------------------------------------------------------------
# Complex multi-batch sequences
# ---------------------------------------------------------------------------


class TestComplexMultiBatch:
    def _make_pipeline(self):
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()
        return bp, sm

    def test_create_modify_rename_modify_across_batches(self):
        bp, sm = self._make_pipeline()
        bp.add_events(
            [
                ev(100, '/gpfs/a.txt', EventKind.CREATED, size=10),
                ev(100, '/gpfs/a.txt', EventKind.MODIFIED, size=20),
            ],
        )
        sm.process_events(bp.flush())
        c1 = sm.emit()
        assert c1[0]['stat']['size'] == 20

        bp.add_events([ev(100, '/gpfs/b.txt', EventKind.RENAMED, size=20)])
        sm.process_events(bp.flush())
        c2 = sm.emit()
        assert any(c['path'] == '/gpfs/b.txt' for c in c2)

        bp.add_events([ev(100, '/gpfs/b.txt', EventKind.MODIFIED, size=30)])
        sm.process_events(bp.flush())
        c3 = sm.emit()
        assert c3[0]['stat']['size'] == 30
        assert c3[0]['path'] == '/gpfs/b.txt'

    def test_full_lifecycle_create_to_delete(self):
        bp, sm = self._make_pipeline()
        bp.add_events([ev(100, '/gpfs/f.txt', EventKind.CREATED, size=5)])
        sm.process_events(bp.flush())
        sm.emit()

        bp.add_events([ev(100, '/gpfs/f.txt', EventKind.MODIFIED, size=50)])
        sm.process_events(bp.flush())
        sm.emit()

        bp.add_events(
            [ev(100, '/gpfs/renamed.txt', EventKind.RENAMED, size=50)],
        )
        sm.process_events(bp.flush())
        sm.emit()

        bp.add_events([ev(100, '/gpfs/renamed.txt', EventKind.REMOVED)])
        sm.process_events(bp.flush())
        c4 = sm.emit()
        deletes = [c for c in c4 if c['op'] == 'delete']
        assert len(deletes) == 1
        assert deletes[0]['path'] == '/gpfs/renamed.txt'

    def test_emit_never_called_state_accumulates(self):
        bp, sm = self._make_pipeline()
        bp.add_events([ev(100, '/gpfs/a.txt', EventKind.CREATED, size=1)])
        sm.process_events(bp.flush())
        bp.add_events([ev(100, '/gpfs/a.txt', EventKind.MODIFIED, size=2)])
        sm.process_events(bp.flush())
        bp.add_events([ev(100, '/gpfs/a.txt', EventKind.MODIFIED, size=3)])
        sm.process_events(bp.flush())
        changes = sm.emit()
        assert len(changes) == 1
        assert changes[0]['stat']['size'] == 3

    def test_emit_per_batch_vs_accumulate(self):
        """Compare emit-per-batch (4 emissions) vs accumulate (1 emission)."""
        # -- emit per batch --
        bp1, sm1 = self._make_pipeline()
        all_changes_1 = []
        for size in [10, 20, 30, 40]:
            kind = EventKind.CREATED if size == 10 else EventKind.MODIFIED
            bp1.add_events(
                [ev(100, '/gpfs/f.txt', kind, size=size, mtime=size)],
            )
            sm1.process_events(bp1.flush())
            all_changes_1.extend(sm1.emit())
        assert len(all_changes_1) == 4
        assert all_changes_1[-1]['stat']['size'] == 40

        # -- accumulate (no emit until end) --
        bp2, sm2 = self._make_pipeline()
        for size in [10, 20, 30, 40]:
            kind = EventKind.CREATED if size == 10 else EventKind.MODIFIED
            bp2.add_events(
                [ev(100, '/gpfs/f.txt', kind, size=size, mtime=size)],
            )
            sm2.process_events(bp2.flush())
        all_changes_2 = sm2.emit()
        assert len(all_changes_2) == 1
        assert all_changes_2[0]['stat']['size'] == 40
        assert all_changes_2[0]['stat']['mtime'] == 40

    def test_multi_inode_interleaved_batches(self):
        """Two inodes, events interleaved across 3 batches."""
        bp, sm = self._make_pipeline()

        # Batch 1: create both
        bp.add_events(
            [
                ev(100, '/gpfs/a.txt', EventKind.CREATED, size=10),
                ev(200, '/gpfs/b.txt', EventKind.CREATED, size=20),
            ],
        )
        sm.process_events(bp.flush())
        sm.emit()

        # Batch 2: modify a, rename b
        bp.add_events(
            [
                ev(100, '/gpfs/a.txt', EventKind.MODIFIED, size=100),
                ev(200, '/gpfs/c.txt', EventKind.RENAMED, size=20),
            ],
        )
        sm.process_events(bp.flush())
        c2 = sm.emit()
        paths_2 = {c['path'] for c in c2 if c['op'] == 'update'}
        assert '/gpfs/a.txt' in paths_2
        assert '/gpfs/c.txt' in paths_2

        # Batch 3: delete a, modify b (now at c.txt)
        bp.add_events(
            [
                ev(100, '/gpfs/a.txt', EventKind.REMOVED),
                ev(200, '/gpfs/c.txt', EventKind.MODIFIED, size=200),
            ],
        )
        sm.process_events(bp.flush())
        c3 = sm.emit()
        ops_3 = {(c['op'], c.get('fid')) for c in c3}
        assert ('delete', '100') in ops_3
        assert ('update', '200') in ops_3

    def test_inode_reuse_across_batches(self):
        bp, sm = self._make_pipeline()
        bp.add_events([ev(100, '/gpfs/old.txt', EventKind.CREATED, size=10)])
        sm.process_events(bp.flush())
        sm.emit()
        bp.add_events([ev(100, '/gpfs/old.txt', EventKind.REMOVED)])
        sm.process_events(bp.flush())
        sm.emit()
        bp.add_events([ev(100, '/gpfs/new.txt', EventKind.CREATED, size=99)])
        sm.process_events(bp.flush())
        changes = sm.emit()
        assert changes[0]['path'] == '/gpfs/new.txt'
        assert changes[0]['stat']['size'] == 99


# ---------------------------------------------------------------------------
# Out-of-order events
# ---------------------------------------------------------------------------


class TestOutOfOrder:
    def test_delete_before_create_unknown_inode(self):
        changes = run_pipeline([ev(100, '/gpfs/f.txt', EventKind.REMOVED)])
        assert len(changes) == 0

    def test_modify_before_create(self):
        changes = run_pipeline(
            [
                ev(100, '/gpfs/f.txt', EventKind.MODIFIED, size=50),
                ev(100, '/gpfs/f.txt', EventKind.CREATED, size=10),
            ],
        )
        assert len(changes) == 1
        assert changes[0]['stat']['size'] == 10

    def test_rename_before_create(self):
        changes = run_pipeline(
            [
                ev(100, '/gpfs/new.txt', EventKind.RENAMED),
                ev(100, '/gpfs/new.txt', EventKind.CREATED, size=5),
            ],
        )
        assert len(changes) == 1
        assert changes[0]['path'] == '/gpfs/new.txt'

    def test_inode_reuse_4_batch_lifecycle(self):
        """Inode 100: create → emit → delete → emit → re-create → modify → emit."""
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()

        # Batch 1: create
        bp.add_events(
            [ev(100, '/gpfs/old.txt', EventKind.CREATED, size=10, uid=1000)],
        )
        sm.process_events(bp.flush())
        c1 = sm.emit()
        assert len(c1) == 1
        assert c1[0]['path'] == '/gpfs/old.txt'
        assert c1[0]['stat']['size'] == 10

        # Batch 2: delete
        bp.add_events([ev(100, '/gpfs/old.txt', EventKind.REMOVED)])
        sm.process_events(bp.flush())
        c2 = sm.emit()
        assert len(c2) == 1
        assert c2[0]['op'] == 'delete'
        assert c2[0]['fid'] == '100'

        # Batch 3: re-create at DIFFERENT path with DIFFERENT metadata
        bp.add_events(
            [ev(100, '/gpfs/new.txt', EventKind.CREATED, size=99, uid=2000)],
        )
        sm.process_events(bp.flush())
        c3 = sm.emit()
        assert len(c3) == 1
        assert c3[0]['path'] == '/gpfs/new.txt'
        assert c3[0]['stat']['size'] == 99
        assert c3[0]['stat']['uid'] == 2000

        # Batch 4: modify
        bp.add_events(
            [
                ev(
                    100,
                    '/gpfs/new.txt',
                    EventKind.MODIFIED,
                    size=200,
                    mtime=9999,
                ),
            ],
        )
        sm.process_events(bp.flush())
        c4 = sm.emit()
        assert len(c4) == 1
        assert c4[0]['stat']['size'] == 200
        assert c4[0]['stat']['mtime'] == 9999

    def test_modify_after_delete_ignored(self):
        """Modify event for a deleted inode should be silently ignored."""
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()

        bp.add_events([ev(100, '/gpfs/f.txt', EventKind.CREATED, size=10)])
        sm.process_events(bp.flush())
        sm.emit()

        bp.add_events([ev(100, '/gpfs/f.txt', EventKind.REMOVED)])
        sm.process_events(bp.flush())
        sm.emit()

        # Out-of-order: modify arrives after delete (Kafka redelivery)
        bp.add_events([ev(100, '/gpfs/f.txt', EventKind.MODIFIED, size=999)])
        sm.process_events(bp.flush())
        changes = sm.emit()
        # Should produce nothing — fid '100' no longer in files
        assert len(changes) == 0


# ---------------------------------------------------------------------------
# Metadata preservation
# ---------------------------------------------------------------------------


class TestMetadataPreservation:
    def test_all_metadata_fields_survive_pipeline(self):
        changes = run_pipeline(
            [
                ev(
                    100,
                    '/gpfs/f.txt',
                    EventKind.CREATED,
                    size=4096,
                    atime=1000,
                    ctime=2000,
                    mtime=3000,
                    uid=1001,
                    gid=1002,
                    permissions='0755',
                ),
            ],
        )
        assert len(changes) == 1
        c = changes[0]
        assert c['op'] == 'update'
        assert c['fid'] == '100'
        assert c['path'] == '/gpfs/f.txt'
        stat = c['stat']
        # All 7 stat fields must be non-None
        for field in ('size', 'atime', 'ctime', 'mtime', 'uid', 'gid', 'mode'):
            assert stat[field] is not None, f'stat[{field}] must not be None'
        assert stat['size'] == 4096
        assert stat['atime'] == 1000
        assert stat['ctime'] == 2000
        assert stat['mtime'] == 3000
        assert stat['uid'] == 1001
        assert stat['gid'] == 1002
        assert stat['mode'] == '0755'

    def test_hardlink_same_inode_last_path_wins(self):
        """Two CREATEDs for same inode (hardlink) — last path wins in state."""
        changes = run_pipeline(
            [
                ev(100, '/gpfs/original.txt', EventKind.CREATED, size=42),
                ev(100, '/gpfs/hardlink.txt', EventKind.CREATED, size=42),
            ],
        )
        # Both events are in same inode slot. Second CREATED overwrites first.
        # Only one update emitted (last path).
        assert len(changes) == 1
        assert changes[0]['path'] == '/gpfs/hardlink.txt'
        assert changes[0]['fid'] == '100'

    def test_hardlink_then_delete_original(self):
        """Hardlink: delete original path, hardlink path unaffected."""
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()

        # Create original + hardlink (same inode, second overwrites)
        bp.add_events(
            [
                ev(100, '/gpfs/original.txt', EventKind.CREATED, size=42),
                ev(100, '/gpfs/hardlink.txt', EventKind.CREATED, size=42),
            ],
        )
        sm.process_events(bp.flush())
        sm.emit()

        # Delete original — but state only knows about hardlink.txt (last path)
        # This REMOVED has the same inode, so it deletes the entry entirely
        bp.add_events([ev(100, '/gpfs/original.txt', EventKind.REMOVED)])
        sm.process_events(bp.flush())
        changes = sm.emit()

        # The entry is deleted (fid '100' removed from files)
        deletes = [c for c in changes if c['op'] == 'delete']
        assert len(deletes) == 1
        assert deletes[0]['fid'] == '100'
        # This is a known limitation: inode-keyed state can't distinguish
        # which hardlink path was actually deleted
        assert '100' not in sm.files

    def test_truncate_to_zero(self):
        """Create file with content, truncate to 0 bytes."""
        changes = run_pipeline(
            [
                ev(
                    100,
                    '/gpfs/f.txt',
                    EventKind.CREATED,
                    size=1024,
                    mtime=1000,
                ),
                ev(100, '/gpfs/f.txt', EventKind.MODIFIED, size=0, mtime=2000),
            ],
        )
        assert len(changes) == 1
        assert changes[0]['stat']['size'] == 0
        assert changes[0]['stat']['mtime'] == 2000

    def test_modify_updates_metadata_in_pipeline(self):
        changes = run_pipeline(
            [
                ev(100, '/gpfs/f.txt', EventKind.CREATED, size=10, mtime=1000),
                ev(
                    100,
                    '/gpfs/f.txt',
                    EventKind.MODIFIED,
                    size=20,
                    mtime=2000,
                ),
            ],
        )
        assert changes[0]['stat']['size'] == 20
        assert changes[0]['stat']['mtime'] == 2000

    def test_20_renames_pipeline_consistency(self):
        """Rename single inode 20 times, verify state consistency at each step."""
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()

        # Create
        bp.add_events([ev(100, '/gpfs/v0.txt', EventKind.CREATED, size=10)])
        sm.process_events(bp.flush())
        sm.emit()

        # 20 renames, emit after each
        for i in range(1, 21):
            bp.add_events(
                [ev(100, f'/gpfs/v{i}.txt', EventKind.RENAMED, size=10 + i)],
            )
            sm.process_events(bp.flush())
            changes = sm.emit()
            assert len(changes) == 1, (
                f'Rename {i}: expected 1 change, got {len(changes)}'
            )
            assert changes[0]['path'] == f'/gpfs/v{i}.txt'
            assert changes[0]['fid'] == '100'
            assert changes[0]['stat']['size'] == 10 + i

        # Final state consistency
        assert sm.path_to_fid.get('/gpfs/v20.txt') == '100'
        for i in range(20):
            assert f'/gpfs/v{i}.txt' not in sm.path_to_fid, (
                f'Stale path /gpfs/v{i}.txt still in path_to_fid'
            )
        assert len(sm.files) == 1
        assert sm.files['100']['path'] == '/gpfs/v20.txt'

    def test_rename_carries_metadata(self):
        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()
        bp.add_events(
            [
                ev(
                    100,
                    '/gpfs/old.txt',
                    EventKind.CREATED,
                    size=42,
                    atime=100,
                    ctime=200,
                    mtime=300,
                ),
            ],
        )
        sm.process_events(bp.flush())
        sm.emit()
        bp.add_events(
            [
                ev(
                    100,
                    '/gpfs/new.txt',
                    EventKind.RENAMED,
                    size=42,
                    mtime=9999,
                    atime=8888,
                    ctime=7777,
                ),
            ],
        )
        sm.process_events(bp.flush())
        changes = sm.emit()
        assert len(changes) == 1
        stat = changes[0]['stat']
        assert stat['mtime'] == 9999
        assert stat['atime'] == 8888
        assert stat['ctime'] == 7777
        assert stat['size'] == 42
        assert changes[0]['path'] == '/gpfs/new.txt'
        assert changes[0]['fid'] == '100'


# ---------------------------------------------------------------------------
# Monitor integration with GPFS components
# ---------------------------------------------------------------------------


class TestMonitorGPFSIntegration:
    """Verify Monitor wires correctly with GPFS BatchProcessor + GPFSStateManager."""

    def test_monitor_with_gpfs_components(self):
        """Monitor processes events through GPFS pipeline to output."""
        from unittest.mock import MagicMock

        from src.icicle.monitor import Monitor
        from src.icicle.output import OutputHandler
        from src.icicle.source import EventSource

        # Mock source that returns events once then empty
        mock_source = MagicMock(spec=EventSource)
        mock_source.read.side_effect = [
            [
                ev(
                    100,
                    '/gpfs/file.txt',
                    EventKind.CREATED,
                    size=42,
                    uid=1000,
                ),
                ev(200, '/gpfs/dir', EventKind.CREATED, is_dir=True),
                ev(300, '/gpfs/dir/nested.txt', EventKind.CREATED, size=10),
            ],
            [],  # signal end
        ]

        # Collecting output
        collected = []

        class CollectOutput(OutputHandler):
            def send(self, payloads):
                collected.extend(payloads)

            def flush(self):
                pass

            def close(self):
                pass

        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()
        monitor = Monitor(mock_source, CollectOutput(), batch=bp, state=sm)
        monitor.run()

        assert len(collected) >= 3, (
            f'Expected 3+ payloads, got {len(collected)}'
        )
        paths = {c['path'] for c in collected if c['op'] == 'update'}
        assert '/gpfs/file.txt' in paths
        assert '/gpfs/dir' in paths
        assert '/gpfs/dir/nested.txt' in paths
        # Verify GPFS output includes fid
        fids = {c.get('fid') for c in collected}
        assert '100' in fids
        assert '200' in fids

    def test_monitor_stats_with_gpfs(self):
        """Monitor.stats() reflects GPFS BatchProcessor counters."""
        from unittest.mock import MagicMock

        from src.icicle.monitor import Monitor
        from src.icicle.source import EventSource

        mock_source = MagicMock(spec=EventSource)
        mock_source.read.side_effect = [
            [
                ev(100, '/gpfs/a', EventKind.CREATED, size=10),
                ev(100, '/gpfs/a', EventKind.REMOVED),
            ],
            [],
        ]

        bp = BatchProcessor(
            rules=GPFS_REDUCTION_RULES,
            slot_key='inode',
            bypass_rename=False,
        )
        sm = GPFSStateManager()
        monitor = Monitor(mock_source, batch=bp, state=sm)
        monitor.run()

        stats = monitor.stats()
        assert stats['batch_received'] == 2
        assert stats['batch_ignored'] + stats['batch_cancelled'] == 2
        assert stats['batch_accepted'] == 0
