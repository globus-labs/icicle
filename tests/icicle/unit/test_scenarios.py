"""End-to-end scenarios reimplemented from old test suites.

Tests pipeline: events → BatchProcessor → StateManager → emit.
Adapted from tests/test_file_ops.py, test_file_ops_misc.py, test_complex_scenarios.py.
"""

from __future__ import annotations

from src.icicle.batch import BatchProcessor
from src.icicle.events import EventKind
from src.icicle.fswatch_events import STAT_REDUCTION_RULES
from src.icicle.state import StateManager


def ev(path: str, kind: EventKind, is_dir: bool = False, **kw) -> dict:
    return {'path': path, 'kind': kind, 'ts': 0.0, 'is_dir': is_dir, **kw}


def make_stat_result():
    class FakeStat:
        st_size = 100
        st_uid = 1000
        st_gid = 1000
        st_mode = 0o100644
        st_atime = 1000
        st_mtime = 2000
        st_ctime = 3000

    return FakeStat()


def _fake_stat_fn(p):
    return make_stat_result()


def run_pipeline(
    event_batches: list[list[dict]],
) -> tuple[StateManager, list[dict]]:
    """Run events through batch → state pipeline, return final state + emitted changes."""
    bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
    sm = StateManager(stat_fn=_fake_stat_fn)
    all_changes = []
    for batch in event_batches:
        bp.add_events(batch)
        coalesced = bp.flush()
        if coalesced:
            sm.process_events(coalesced)
    return sm, all_changes


def run_pipeline_with_emit(
    event_batches: list[list[dict]],
    emit_after_each: bool = False,
) -> tuple[StateManager, list[dict]]:
    """Run pipeline, optionally emitting after each batch."""
    bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
    sm = StateManager(stat_fn=_fake_stat_fn)
    all_changes: list[dict] = []
    for batch in event_batches:
        bp.add_events(batch)
        coalesced = bp.flush()
        if coalesced:
            sm.process_events(coalesced)
        if emit_after_each:
            all_changes.extend(sm.emit())
    if not emit_after_each:
        all_changes.extend(sm.emit())
    return sm, all_changes


# ---------------------------------------------------------------------------
# From test_file_ops.py: rename/move sequences
# ---------------------------------------------------------------------------


class TestRenameSequences:
    def test_create_rename_file(self):
        """Create file, rename it, verify state tracks new path."""
        sm, changes = run_pipeline_with_emit(
            [
                [ev('/tmp/a.txt', EventKind.CREATED)],
                [
                    ev('/tmp/a.txt', EventKind.RENAMED),
                    ev('/tmp/b.txt', EventKind.RENAMED),
                ],
            ],
        )
        assert '/tmp/a.txt' not in sm.files
        assert '/tmp/b.txt' in sm.files

    def test_create_dir_file_move_delete(self):
        """Create dir + file, move file, delete dir → verify clean state."""
        sm, _ = run_pipeline(
            [
                [
                    ev('/tmp/dir', EventKind.CREATED, is_dir=True),
                    ev('/tmp/dir/f.txt', EventKind.CREATED),
                    ev('/tmp/other', EventKind.CREATED, is_dir=True),
                ],
            ],
        )
        # Simulate move: file renamed from dir/ to other/
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/tmp/dir/f.txt', EventKind.RENAMED),
                ev('/tmp/other/f.txt', EventKind.RENAMED),
            ],
        )
        sm.process_events(bp.flush())
        assert '/tmp/other/f.txt' in sm.files
        assert '/tmp/dir/f.txt' not in sm.files

        # Delete the now-empty dir
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/tmp/dir', EventKind.REMOVED, is_dir=True)])
        sm.process_events(bp.flush())
        assert '/tmp/dir' not in sm.files

    def test_create_rename_delete_cancelled(self):
        """Create → rename → delete: transient file, batch sees create+remove=cancelled."""
        sm, changes = run_pipeline_with_emit(
            [
                [
                    ev('/tmp/a.txt', EventKind.CREATED),
                    ev(
                        '/tmp/a.txt',
                        EventKind.REMOVED,
                    ),  # batch cancels create+remove
                ],
            ],
        )
        assert '/tmp/a.txt' not in sm.files
        assert len(changes) == 0


# ---------------------------------------------------------------------------
# From test_file_ops_misc.py: complex operations
# ---------------------------------------------------------------------------


class TestComplexOps:
    def test_rename_populated_dir_modify_child(self):
        """Create populated dir, rename, modify child in new location."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        sm = StateManager(stat_fn=_fake_stat_fn)

        # Stage 1: create structure
        bp.add_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/f1.txt', EventKind.CREATED),
                ev('/a/f2.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(bp.flush())

        # Stage 2: rename dir
        bp.add_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.process_events(bp.flush())
        assert '/b/f1.txt' in sm.files
        assert '/b/f2.txt' in sm.files

        # Stage 3: modify child in new location
        bp.add_events([ev('/b/f1.txt', EventKind.MODIFIED)])
        sm.process_events(bp.flush())
        assert '/b/f1.txt' in sm.to_update

    def test_move_file_across_directories(self):
        """Create 2 dirs + file, move file between dirs."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        sm = StateManager(stat_fn=_fake_stat_fn)

        bp.add_events(
            [
                ev('/dir1', EventKind.CREATED, is_dir=True),
                ev('/dir2', EventKind.CREATED, is_dir=True),
                ev('/dir1/f.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(bp.flush())

        bp.add_events(
            [
                ev('/dir1/f.txt', EventKind.RENAMED),
                ev('/dir2/f.txt', EventKind.RENAMED),
            ],
        )
        sm.process_events(bp.flush())

        assert '/dir1/f.txt' not in sm.files
        assert '/dir2/f.txt' in sm.files
        assert '/dir2/f.txt' in sm.children['/dir2']
        assert '/dir1/f.txt' not in sm.children.get('/dir1', set())

    def test_rename_back_and_forth(self):
        """Create → rename A→B → rename B→A → final path correct."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        sm = StateManager(stat_fn=_fake_stat_fn)

        bp.add_events([ev('/a.txt', EventKind.CREATED)])
        sm.process_events(bp.flush())

        # A → B
        bp.add_events(
            [
                ev('/a.txt', EventKind.RENAMED),
                ev('/b.txt', EventKind.RENAMED),
            ],
        )
        sm.process_events(bp.flush())
        assert '/b.txt' in sm.files

        # B → A
        bp.add_events(
            [
                ev('/b.txt', EventKind.RENAMED),
                ev('/a.txt', EventKind.RENAMED),
            ],
        )
        sm.process_events(bp.flush())
        assert '/a.txt' in sm.files
        assert '/b.txt' not in sm.files

    def test_ephemeral_creation_cycle(self):
        """Repeated create/delete cycles → only final state persists."""
        events = []
        for _ in range(20):
            events.append(ev('/tmp/ephemeral.txt', EventKind.CREATED))
            events.append(ev('/tmp/ephemeral.txt', EventKind.REMOVED))
        # Final create
        events.append(ev('/tmp/ephemeral.txt', EventKind.CREATED))

        sm, changes = run_pipeline_with_emit([events])
        assert '/tmp/ephemeral.txt' in sm.files
        # Only 1 CREATED survives after all cancellations
        assert len(changes) == 1
        assert changes[0]['op'] == 'update'


# ---------------------------------------------------------------------------
# From test_complex_scenarios.py: stress/scale
# ---------------------------------------------------------------------------


class TestStressScenarios:
    def test_deep_structure_rapid_cycles(self):
        """Deep structure + rapid create/delete cycles → ephemeral files gone."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        sm = StateManager(stat_fn=_fake_stat_fn)

        # Create directory structure
        dirs = ['/root', '/root/a', '/root/a/b', '/root/a/b/c']
        for d in dirs:
            bp.add_events([ev(d, EventKind.CREATED, is_dir=True)])
        sm.process_events(bp.flush())

        # Create persistent files
        persistent = ['/root/keep.txt', '/root/a/keep.txt']
        bp.add_events([ev(p, EventKind.CREATED) for p in persistent])
        sm.process_events(bp.flush())

        # Rapid ephemeral cycles (create + delete 20 times)
        ephemeral_events = []
        for _ in range(20):
            ephemeral_events.append(
                ev('/root/a/b/temp.txt', EventKind.CREATED),
            )
            ephemeral_events.append(
                ev('/root/a/b/temp.txt', EventKind.REMOVED),
            )
        bp.add_events(ephemeral_events)
        sm.process_events(bp.flush())

        # Ephemeral file is gone, persistent files remain
        assert '/root/a/b/temp.txt' not in sm.files
        assert '/root/keep.txt' in sm.files
        assert '/root/a/keep.txt' in sm.files

    def test_project_lifecycle(self):
        """Create structure → modify → rename → selective delete → verify."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        sm = StateManager(stat_fn=_fake_stat_fn)

        # Stage 1: Create project structure
        structure = [
            ev('/proj', EventKind.CREATED, is_dir=True),
            ev('/proj/src', EventKind.CREATED, is_dir=True),
            ev('/proj/src/main.py', EventKind.CREATED),
            ev('/proj/src/utils.py', EventKind.CREATED),
            ev('/proj/tests', EventKind.CREATED, is_dir=True),
            ev('/proj/tests/test_main.py', EventKind.CREATED),
            ev('/proj/docs', EventKind.CREATED, is_dir=True),
            ev('/proj/docs/readme.md', EventKind.CREATED),
        ]
        bp.add_events(structure)
        sm.process_events(bp.flush())
        assert len(sm.files) == 8

        # Stage 2: Modify files
        bp.add_events(
            [
                ev('/proj/src/main.py', EventKind.MODIFIED),
                ev('/proj/src/main.py', EventKind.MODIFIED),
                ev('/proj/src/main.py', EventKind.MODIFIED),
            ],
        )
        coalesced = bp.flush()
        assert len(coalesced) == 1  # 3 MODIFIED → 1
        sm.process_events(coalesced)

        # Stage 3: Rename src → lib
        bp.add_events(
            [
                ev('/proj/src', EventKind.RENAMED, is_dir=True),
                ev('/proj/lib', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.process_events(bp.flush())
        assert '/proj/lib/main.py' in sm.files
        assert '/proj/lib/utils.py' in sm.files
        assert '/proj/src' not in sm.files

        # Stage 4: Delete docs
        bp.add_events([ev('/proj/docs', EventKind.REMOVED, is_dir=True)])
        sm.process_events(bp.flush())
        assert '/proj/docs' not in sm.files
        assert '/proj/docs/readme.md' not in sm.files

        # Final state: proj, lib, lib/main.py, lib/utils.py, tests, tests/test_main.py
        assert len(sm.files) == 6

    def test_systematic_directory_removal(self):
        """Remove directories in stages: leaves → subtrees → root."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        sm = StateManager(stat_fn=_fake_stat_fn)

        # Build tree
        bp.add_events(
            [
                ev('/root', EventKind.CREATED, is_dir=True),
                ev('/root/a', EventKind.CREATED, is_dir=True),
                ev('/root/a/a1', EventKind.CREATED, is_dir=True),
                ev('/root/a/a1/f.txt', EventKind.CREATED),
                ev('/root/a/a2', EventKind.CREATED, is_dir=True),
                ev('/root/b', EventKind.CREATED, is_dir=True),
                ev('/root/b/f.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(bp.flush())
        assert len(sm.files) == 7

        # Stage 1: remove leaf dir a2 (empty)
        bp.add_events([ev('/root/a/a2', EventKind.REMOVED, is_dir=True)])
        sm.process_events(bp.flush())
        assert '/root/a/a2' not in sm.files

        # Stage 2: remove subtree a (has a1/f.txt)
        bp.add_events([ev('/root/a', EventKind.REMOVED, is_dir=True)])
        sm.process_events(bp.flush())
        assert '/root/a' not in sm.files
        assert '/root/a/a1' not in sm.files
        assert '/root/a/a1/f.txt' not in sm.files

        # Stage 3: remove b
        bp.add_events([ev('/root/b', EventKind.REMOVED, is_dir=True)])
        sm.process_events(bp.flush())
        assert '/root/b' not in sm.files
        assert '/root/b/f.txt' not in sm.files

        # Only root remains
        assert len(sm.files) == 1
        assert '/root' in sm.files

    def test_rename_with_overwrite(self):
        """Rename a file to overwrite an existing file."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        sm = StateManager(stat_fn=_fake_stat_fn)

        bp.add_events(
            [
                ev('/dir', EventKind.CREATED, is_dir=True),
                ev('/dir/a.txt', EventKind.CREATED),
                ev('/dir/b.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(bp.flush())
        sm.emit()  # mark as emitted

        # Rename a.txt → b.txt (overwrites b.txt)
        bp.add_events(
            [
                ev('/dir/a.txt', EventKind.RENAMED),
                ev('/dir/b.txt', EventKind.RENAMED),
            ],
        )
        sm.process_events(bp.flush())

        # a.txt is gone, b.txt exists (but it's the old a.txt)
        assert '/dir/a.txt' not in sm.files
        assert '/dir/b.txt' in sm.files

        changes = sm.emit()
        ops = {(c['op'], c['path']) for c in changes}
        # Should see: delete /dir/a.txt, delete /dir/b.txt (old), update /dir/b.txt (new)
        assert ('delete', '/dir/a.txt') in ops
        assert ('update', '/dir/b.txt') in ops


# ---------------------------------------------------------------------------
# Pipeline: re-creation after deletion
# ---------------------------------------------------------------------------


class TestPipelineRecreation:
    def test_remove_recreate_emitted(self):
        """Create → emit → remove + create → emit: file exists, so update not delete."""
        sm, changes = run_pipeline_with_emit(
            [
                [ev('/tmp/f.txt', EventKind.CREATED)],
                [
                    ev('/tmp/f.txt', EventKind.REMOVED),
                    ev('/tmp/f.txt', EventKind.CREATED),
                ],
            ],
            emit_after_each=True,
        )

        updates = [
            c
            for c in changes
            if c['op'] == 'update' and c['path'] == '/tmp/f.txt'
        ]
        deletes = [
            c
            for c in changes
            if c['op'] == 'delete' and c['path'] == '/tmp/f.txt'
        ]

        assert len(updates) >= 2, (
            f'Expected 2 updates (create + re-create), got {len(updates)}: {changes}'
        )
        assert len(deletes) == 0, (
            f'Should NOT delete file that exists at end: {changes}'
        )

    def test_multiple_remove_recreate_cycles(self):
        """3 cycles of remove + re-create — only updates, no deletes."""
        batches = [[ev('/tmp/f.txt', EventKind.CREATED)]]
        for _ in range(3):
            batches.append(
                [
                    ev('/tmp/f.txt', EventKind.REMOVED),
                    ev('/tmp/f.txt', EventKind.CREATED),
                ],
            )

        sm, changes = run_pipeline_with_emit(batches, emit_after_each=True)

        updates = [
            c
            for c in changes
            if c['op'] == 'update' and c['path'] == '/tmp/f.txt'
        ]
        deletes = [
            c
            for c in changes
            if c['op'] == 'delete' and c['path'] == '/tmp/f.txt'
        ]

        assert '/tmp/f.txt' in sm.files
        assert len(updates) >= 4, (
            f'Expected 4 updates, got {len(updates)}: {changes}'
        )
        assert len(deletes) == 0, f'Should NOT delete: {changes}'

    def test_rename_split_no_emit_pairs(self):
        """Rename split across batches without emit — pairs correctly."""
        sm, changes = run_pipeline_with_emit(
            [
                [ev('/tmp/a.txt', EventKind.CREATED)],
                [ev('/tmp/a.txt', EventKind.RENAMED)],
                [ev('/tmp/b.txt', EventKind.RENAMED)],
            ],
            emit_after_each=False,
        )

        assert '/tmp/b.txt' in sm.files
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/tmp/b.txt' in update_paths

    def test_rename_split_with_emit_pairs_across_batches(self):
        """Rename split across batches with emit — grace period allows pairing."""
        sm, changes = run_pipeline_with_emit(
            [
                [ev('/tmp/a.txt', EventKind.CREATED)],
                [ev('/tmp/a.txt', EventKind.RENAMED)],
                [ev('/tmp/b.txt', EventKind.RENAMED)],
            ],
            emit_after_each=True,
        )

        assert '/tmp/b.txt' in sm.files

    def test_rename_dir_then_modify_child_pipeline(self):
        """Pipeline: create dir → rename dir → modify child at new path."""
        sm, changes = run_pipeline_with_emit(
            [
                [
                    ev('/a', EventKind.CREATED, is_dir=True),
                    ev('/a/f.txt', EventKind.CREATED),
                ],
                [
                    ev('/a', EventKind.RENAMED, is_dir=True),
                    ev('/b', EventKind.RENAMED, is_dir=True),
                ],
                [ev('/b/f.txt', EventKind.MODIFIED)],
            ],
            emit_after_each=False,
        )

        assert '/b/f.txt' in sm.files
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/b' in update_paths
        assert '/b/f.txt' in update_paths


# ---------------------------------------------------------------------------
# Editor-like atomic save patterns
# ---------------------------------------------------------------------------


class TestEditorAtomicSave:
    def test_vim_swp_save(self):
        """Vim save: create target → write .swp → rename .swp → target."""
        sm, changes = run_pipeline_with_emit(
            [
                [ev('/file.txt', EventKind.CREATED)],
                [ev('/file.txt.swp', EventKind.CREATED)],
                [
                    ev('/file.txt.swp', EventKind.RENAMED),
                    ev('/file.txt', EventKind.RENAMED),
                ],
            ],
            emit_after_each=True,
        )

        assert '/file.txt' in sm.files
        assert '/file.txt.swp' not in sm.files
        updates = [
            c
            for c in changes
            if c['op'] == 'update' and c['path'] == '/file.txt'
        ]
        assert len(updates) >= 1

    def test_atomic_save_via_temp(self):
        """Atomic save: write temp → rename temp → target (overwrites)."""
        sm, changes = run_pipeline_with_emit(
            [
                [ev('/config.yaml', EventKind.CREATED)],
                [
                    ev('/config.yaml.tmp', EventKind.CREATED),
                    ev('/config.yaml.tmp', EventKind.RENAMED),
                    ev('/config.yaml', EventKind.RENAMED),
                ],
            ],
            emit_after_each=True,
        )

        assert '/config.yaml' in sm.files
        updates = [
            c
            for c in changes
            if c['op'] == 'update' and c['path'] == '/config.yaml'
        ]
        assert len(updates) >= 1


# ---------------------------------------------------------------------------
# Pipeline: rapid mkdir + rmtree
# ---------------------------------------------------------------------------


class TestPipelineDirLifecycle:
    def test_mkdir_add_files_rmtree(self):
        """Pipeline: create dir + files → rmtree. Exact delete counts."""
        sm, changes = run_pipeline_with_emit(
            [
                [
                    ev('/proj', EventKind.CREATED, is_dir=True),
                    ev('/proj/a.py', EventKind.CREATED),
                    ev('/proj/b.py', EventKind.CREATED),
                ],
                [ev('/proj', EventKind.REMOVED, is_dir=True)],
            ],
            emit_after_each=True,
        )

        updates = [c for c in changes if c['op'] == 'update']
        deletes = [c for c in changes if c['op'] == 'delete']
        assert len(updates) == 3
        assert len(deletes) == 3


# ---------------------------------------------------------------------------
# Git-like checkout
# ---------------------------------------------------------------------------


class TestGitLikeCheckoutScenario:
    def test_branch_switch_pipeline(self):
        """Create A files → emit → delete some + create B → emit."""
        sm, changes = run_pipeline_with_emit(
            [
                [
                    ev('/main.py', EventKind.CREATED),
                    ev('/old.py', EventKind.CREATED),
                ],
                [
                    ev('/old.py', EventKind.REMOVED),
                    ev('/new.py', EventKind.CREATED),
                    ev('/main.py', EventKind.MODIFIED),
                ],
            ],
            emit_after_each=True,
        )

        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/main.py' in update_paths
        assert '/new.py' in update_paths
        assert '/old.py' in delete_paths


# ---------------------------------------------------------------------------
# Log rotation
# ---------------------------------------------------------------------------


class TestLogRotationScenario:
    def test_rotate_and_recreate(self):
        """Log rotation: log → log.1, create new log. Pipeline."""
        sm, changes = run_pipeline_with_emit(
            [
                [ev('/app.log', EventKind.CREATED)],
                [ev('/app.log', EventKind.MODIFIED)],
                [
                    ev('/app.log', EventKind.RENAMED),
                    ev('/app.log.1', EventKind.RENAMED),
                ],
                [ev('/app.log', EventKind.CREATED)],
            ],
            emit_after_each=True,
        )

        assert '/app.log' in sm.files
        assert '/app.log.1' in sm.files
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/app.log' in update_paths
        assert '/app.log.1' in update_paths


# ---------------------------------------------------------------------------
# Long rename chain pipeline
# ---------------------------------------------------------------------------


class TestLongRenameChainPipeline:
    def test_5_step_chain_separate_batches(self):
        """A→B→C→D→E in separate batches, single emit."""
        sm, changes = run_pipeline_with_emit(
            [
                [ev('/A', EventKind.CREATED)],
                [ev('/A', EventKind.RENAMED), ev('/B', EventKind.RENAMED)],
                [ev('/B', EventKind.RENAMED), ev('/C', EventKind.RENAMED)],
                [ev('/C', EventKind.RENAMED), ev('/D', EventKind.RENAMED)],
                [ev('/D', EventKind.RENAMED), ev('/E', EventKind.RENAMED)],
            ],
            emit_after_each=False,
        )

        assert '/E' in sm.files
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/E' in update_paths


# ---------------------------------------------------------------------------
# Pipeline stress tests
# ---------------------------------------------------------------------------


class TestPipelineStress:
    def test_100_batches_100_files(self):
        """100 separate batches, 1 file each — 100 files tracked and emitted."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        sm = StateManager(stat_fn=_fake_stat_fn)
        for i in range(100):
            bp.add_events([ev(f'/file_{i}.txt', EventKind.CREATED)])
            sm.process_events(bp.flush())
        assert len(sm.files) == 100
        assert len(sm.emit()) == 100

    def test_create_remove_across_batches(self):
        """Create in batch 1, remove in batch 2."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        sm = StateManager(stat_fn=_fake_stat_fn)
        bp.add_events([ev('/temp', EventKind.CREATED)])
        sm.process_events(bp.flush())
        assert '/temp' in sm.files
        bp.add_events([ev('/temp', EventKind.REMOVED)])
        sm.process_events(bp.flush())
        assert '/temp' not in sm.files


# ---------------------------------------------------------------------------
# Complete project lifecycle
# ---------------------------------------------------------------------------


class TestCompleteLifecycle:
    def test_create_modify_rename_add_delete(self):
        """Full project lifecycle with emit at each step."""
        sm, changes = run_pipeline_with_emit(
            [
                [
                    ev('/proj', EventKind.CREATED, is_dir=True),
                    ev('/proj/src', EventKind.CREATED, is_dir=True),
                    ev('/proj/src/main.py', EventKind.CREATED),
                    ev('/proj/src/utils.py', EventKind.CREATED),
                    ev('/proj/tests', EventKind.CREATED, is_dir=True),
                    ev('/proj/tests/test_main.py', EventKind.CREATED),
                ],
                [ev('/proj/src/main.py', EventKind.MODIFIED)],
                [
                    ev('/proj/src', EventKind.RENAMED, is_dir=True),
                    ev('/proj/lib', EventKind.RENAMED, is_dir=True),
                ],
                [
                    ev('/proj/lib/helpers.py', EventKind.CREATED),
                    ev('/proj/README.md', EventKind.CREATED),
                ],
                [ev('/proj/tests/test_main.py', EventKind.REMOVED)],
            ],
            emit_after_each=True,
        )

        assert '/proj/lib/main.py' in sm.files
        assert '/proj/lib/utils.py' in sm.files
        assert '/proj/lib/helpers.py' in sm.files
        assert '/proj/README.md' in sm.files
        assert '/proj/src' not in sm.files
        assert '/proj/tests/test_main.py' not in sm.files


# ---------------------------------------------------------------------------
# Emit ordering
# ---------------------------------------------------------------------------


class TestEmitOrdering:
    def test_updates_before_deletes(self):
        """emit() produces updates before deletes."""
        sm = StateManager(stat_fn=_fake_stat_fn)
        sm.process_events(
            [ev('/keep', EventKind.CREATED), ev('/rm', EventKind.CREATED)],
        )
        sm.emit()
        sm.process_events(
            [ev('/keep', EventKind.MODIFIED), ev('/rm', EventKind.REMOVED)],
        )
        changes = sm.emit()
        update_idx = [i for i, c in enumerate(changes) if c['op'] == 'update']
        delete_idx = [i for i, c in enumerate(changes) if c['op'] == 'delete']
        assert max(update_idx) < min(delete_idx)


# ---------------------------------------------------------------------------
# High volume pipeline
# ---------------------------------------------------------------------------


class TestHighVolumePipeline:
    def test_10000_creates(self):
        """10000 files through full pipeline."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        sm = StateManager(stat_fn=_fake_stat_fn)
        bp.add_events([ev(f'/f_{i}', EventKind.CREATED) for i in range(10000)])
        sm.process_events(bp.flush())
        assert len(sm.files) == 10000
        assert len(sm.emit()) == 10000

    def test_5000_create_5000_remove_cancelled(self):
        """5000 create + 5000 remove → all cancelled."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        events = [ev(f'/f_{i}', EventKind.CREATED) for i in range(5000)]
        events.extend(ev(f'/f_{i}', EventKind.REMOVED) for i in range(5000))
        bp.add_events(events)
        assert len(bp.flush()) == 0
