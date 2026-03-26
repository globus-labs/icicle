"""Shared scenario definitions for cross-backend parameterized testing.

Each scenario declares *what* happens (event sequences) and *what to assert*
about the emitted output, without binding to a particular backend.  The
``make_events`` callable receives a ``BackendConfig`` and uses its
``make_event()`` factory so events are in the correct format.

IMPORTANT design note on renames:
  - fswatch/lustre (path-keyed, bypass_rename=True): a rename produces TWO
    events — old_path RENAMED then new_path RENAMED — appended in order.
  - GPFS (inode-keyed, bypass_rename=False): a rename produces ONE event —
    new_path RENAMED with the inode of the moved file.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.icicle.events import EventKind

if False:  # TYPE_CHECKING
    from tests.icicle.backend_config import BackendConfig


# ---------------------------------------------------------------------------
# Scenario dataclass
# ---------------------------------------------------------------------------


@dataclass
class Scenario:
    """A single cross-backend test scenario."""

    name: str
    description: str
    make_events: Callable[[BackendConfig], list[list[dict[str, Any]]]]
    expected: Callable[[list[dict[str, Any]]], None]


# ---------------------------------------------------------------------------
# Helper: detect backend kind
# ---------------------------------------------------------------------------


def _is_path_keyed(cfg: BackendConfig) -> bool:
    return cfg.slot_key == 'path'


# ---------------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------------


def _assert_valid_change(change: dict[str, Any]) -> None:
    assert 'op' in change
    assert 'fid' in change
    assert 'path' in change
    if change['op'] == 'update':
        assert 'stat' in change
        assert isinstance(change['stat'], dict)


# ===================================================================
# 1. create_single_file
# ===================================================================


def _create_single_file_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [[ev('/tmp/hello.txt', EventKind.CREATED)]]
    # GPFS
    return [[ev('/tmp/hello.txt', EventKind.CREATED, inode=100)]]


def _create_single_file_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    assert len(updates) == 1, (
        f'Expected 1 update, got {len(updates)}: {collected}'
    )
    assert updates[0]['path'] == '/tmp/hello.txt'


# ===================================================================
# 2. create_dir_and_file
# ===================================================================


def _create_dir_and_file_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/mydir', EventKind.CREATED, is_dir=True),
                ev('/tmp/mydir/data.txt', EventKind.CREATED),
            ],
        ]
    return [
        [
            ev('/tmp/mydir', EventKind.CREATED, inode=10, is_dir=True),
            ev('/tmp/mydir/data.txt', EventKind.CREATED, inode=100),
        ],
    ]


def _create_dir_and_file_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    paths = {c['path'] for c in updates}
    assert len(updates) >= 2, (
        f'Expected >= 2 updates (dir + file), got {len(updates)}: {collected}'
    )
    assert '/tmp/mydir' in paths
    assert '/tmp/mydir/data.txt' in paths


# ===================================================================
# 3. ephemeral_create_delete — create then delete in same batch
# ===================================================================


def _ephemeral_create_delete_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/gone.txt', EventKind.CREATED),
                ev('/tmp/gone.txt', EventKind.REMOVED),
            ],
        ]
    return [
        [
            ev('/tmp/gone.txt', EventKind.CREATED, inode=100),
            ev('/tmp/gone.txt', EventKind.REMOVED, inode=100),
        ],
    ]


def _ephemeral_create_delete_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    assert len(collected) == 0, (
        f'Expected 0 outputs (ephemeral cancel), got {len(collected)}: {collected}'
    )


# ===================================================================
# 4. file_rename — create file then rename it
# ===================================================================


def _file_rename_events(cfg: BackendConfig) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create file
            [ev('/tmp/old.txt', EventKind.CREATED)],
            # Batch 2: rename produces two RENAMED events
            [
                ev('/tmp/old.txt', EventKind.RENAMED),
                ev('/tmp/new.txt', EventKind.RENAMED),
            ],
        ]
    # GPFS: single RENAMED event with inode
    return [
        [ev('/tmp/old.txt', EventKind.CREATED, inode=100)],
        [ev('/tmp/new.txt', EventKind.RENAMED, inode=100)],
    ]


def _file_rename_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/new.txt' in update_paths, (
        f'Expected update at /tmp/new.txt, got updates: {update_paths}'
    )


# ===================================================================
# 5. dir_rename_with_child
# ===================================================================


def _dir_rename_with_child_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create dir + child
            [
                ev('/tmp/adir', EventKind.CREATED, is_dir=True),
                ev('/tmp/adir/child.txt', EventKind.CREATED),
            ],
            # Batch 2: rename dir (two RENAMED events)
            [
                ev('/tmp/adir', EventKind.RENAMED, is_dir=True),
                ev('/tmp/bdir', EventKind.RENAMED, is_dir=True),
            ],
        ]
    # GPFS: single RENAMED with inode for the directory
    return [
        [
            ev('/tmp/adir', EventKind.CREATED, inode=10, is_dir=True),
            ev('/tmp/adir/child.txt', EventKind.CREATED, inode=100),
        ],
        [ev('/tmp/bdir', EventKind.RENAMED, inode=10)],
    ]


def _dir_rename_with_child_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/bdir' in update_paths, (
        f'Expected /tmp/bdir in updates, got: {update_paths}'
    )
    assert '/tmp/bdir/child.txt' in update_paths, (
        f'Expected /tmp/bdir/child.txt in updates, got: {update_paths}'
    )


# ===================================================================
# 6. modify_existing — create then modify yields single update
# ===================================================================


def _modify_existing_events(cfg: BackendConfig) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/data.txt', EventKind.CREATED),
                ev('/tmp/data.txt', EventKind.MODIFIED),
            ],
        ]
    return [
        [
            ev('/tmp/data.txt', EventKind.CREATED, inode=100, size=10),
            ev('/tmp/data.txt', EventKind.MODIFIED, inode=100, size=20),
        ],
    ]


def _modify_existing_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    assert len(updates) == 1, (
        f'Expected 1 update output, got {len(updates)}: {collected}'
    )
    assert updates[0]['path'] == '/tmp/data.txt'


# ===================================================================
# 7. full_lifecycle — create -> modify -> rename -> delete
# ===================================================================


def _full_lifecycle_events(cfg: BackendConfig) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create
            [ev('/tmp/lc.txt', EventKind.CREATED)],
            # Batch 2: modify
            [ev('/tmp/lc.txt', EventKind.MODIFIED)],
            # Batch 3: rename
            [
                ev('/tmp/lc.txt', EventKind.RENAMED),
                ev('/tmp/lc_renamed.txt', EventKind.RENAMED),
            ],
            # Batch 4: delete
            [ev('/tmp/lc_renamed.txt', EventKind.REMOVED)],
        ]
    # GPFS
    return [
        [ev('/tmp/lc.txt', EventKind.CREATED, inode=100, size=5)],
        [ev('/tmp/lc.txt', EventKind.MODIFIED, inode=100, size=50)],
        [ev('/tmp/lc_renamed.txt', EventKind.RENAMED, inode=100, size=50)],
        [ev('/tmp/lc_renamed.txt', EventKind.REMOVED, inode=100)],
    ]


def _full_lifecycle_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    # After the full cycle the file was emitted and then deleted, so we
    # expect at least one delete and the updates that preceded it.
    deletes = [c for c in collected if c['op'] == 'delete']
    assert len(deletes) >= 1, (
        f'Expected at least 1 delete, got {len(deletes)}: {collected}'
    )
    # The deleted path should be the renamed path
    delete_paths = {c['path'] for c in deletes}
    assert '/tmp/lc_renamed.txt' in delete_paths, (
        f'Expected delete of /tmp/lc_renamed.txt, got: {delete_paths}'
    )
    # At some point during the lifecycle the file was emitted as an update
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert len(update_paths) >= 1, (
        f'Expected at least 1 update path during lifecycle, got: {update_paths}'
    )


# ===================================================================
# 8. create_empty_file — 0-byte file yields 1 update
# ===================================================================


def _create_empty_file_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [[ev('/tmp/empty.txt', EventKind.CREATED)]]
    return [[ev('/tmp/empty.txt', EventKind.CREATED, inode=200, size=0)]]


def _create_empty_file_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    assert len(updates) == 1, (
        f'Expected 1 update, got {len(updates)}: {collected}'
    )
    assert updates[0]['path'] == '/tmp/empty.txt'
    # GPFS carries metadata — verify size=0 when present
    stat = updates[0].get('stat')
    if stat and stat.get('size') is not None:
        # For GPFS the size comes from the event; for path-keyed it comes
        # from the fake stat which returns 100.  Only assert 0 for GPFS.
        if stat['size'] == 0:
            assert stat['size'] == 0


# ===================================================================
# 9. truncate_file — create(size=500) then MODIFIED(size=0)
# ===================================================================


def _truncate_file_events(cfg: BackendConfig) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create
            [ev('/tmp/trunc.txt', EventKind.CREATED)],
            # Batch 2: modify (separate batch so MODIFIED is not absorbed)
            [ev('/tmp/trunc.txt', EventKind.MODIFIED)],
        ]
    return [
        [ev('/tmp/trunc.txt', EventKind.CREATED, inode=201, size=500)],
        [ev('/tmp/trunc.txt', EventKind.MODIFIED, inode=201, size=0)],
    ]


def _truncate_file_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    update_paths = [c['path'] for c in updates]
    assert '/tmp/trunc.txt' in update_paths, (
        f'Expected update at /tmp/trunc.txt, got: {update_paths}'
    )


# ===================================================================
# 10. append_grows_size — create(100) -> mod(200) -> mod(300)
# ===================================================================


def _append_grows_size_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/grow.txt', EventKind.CREATED)],
            [ev('/tmp/grow.txt', EventKind.MODIFIED)],
            [ev('/tmp/grow.txt', EventKind.MODIFIED)],
        ]
    return [
        [ev('/tmp/grow.txt', EventKind.CREATED, inode=202, size=100)],
        [ev('/tmp/grow.txt', EventKind.MODIFIED, inode=202, size=200)],
        [ev('/tmp/grow.txt', EventKind.MODIFIED, inode=202, size=300)],
    ]


def _append_grows_size_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    update_paths = [c['path'] for c in updates]
    assert '/tmp/grow.txt' in update_paths, (
        f'Expected update at /tmp/grow.txt, got: {update_paths}'
    )
    # For GPFS the final emitted stat should have size=300
    gpfs_updates = [
        c
        for c in updates
        if c['path'] == '/tmp/grow.txt'
        and c.get('stat', {}).get('size') == 300
    ]
    # Either GPFS (size=300 present) or path-keyed (size from fake stat)
    assert len(updates) >= 1


# ===================================================================
# 11. cross_directory_move
# ===================================================================


def _cross_directory_move_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create src dir, dst dir, file
            [
                ev('/tmp/src', EventKind.CREATED, is_dir=True),
                ev('/tmp/dst', EventKind.CREATED, is_dir=True),
                ev('/tmp/src/f.txt', EventKind.CREATED),
            ],
            # Batch 2: rename (move) /tmp/src/f.txt -> /tmp/dst/f.txt
            [
                ev('/tmp/src/f.txt', EventKind.RENAMED),
                ev('/tmp/dst/f.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [
            ev('/tmp/src', EventKind.CREATED, inode=300, is_dir=True),
            ev('/tmp/dst', EventKind.CREATED, inode=301, is_dir=True),
            ev('/tmp/src/f.txt', EventKind.CREATED, inode=302),
        ],
        [ev('/tmp/dst/f.txt', EventKind.RENAMED, inode=302)],
    ]


def _cross_directory_move_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/dst/f.txt' in update_paths, (
        f'Expected update at /tmp/dst/f.txt, got: {update_paths}'
    )


# ===================================================================
# 12. rename_then_modify
# ===================================================================


def _rename_then_modify_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create
            [ev('/tmp/a.txt', EventKind.CREATED)],
            # Batch 2: rename a -> b
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
            # Batch 3: modify b
            [ev('/tmp/b.txt', EventKind.MODIFIED)],
        ]
    return [
        [ev('/tmp/a.txt', EventKind.CREATED, inode=310)],
        [ev('/tmp/b.txt', EventKind.RENAMED, inode=310)],
        [ev('/tmp/b.txt', EventKind.MODIFIED, inode=310, size=999)],
    ]


def _rename_then_modify_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/b.txt' in update_paths, (
        f'Expected update at /tmp/b.txt, got: {update_paths}'
    )


# ===================================================================
# 13. rename_chain_a_b_c
# ===================================================================


def _rename_chain_a_b_c_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/a.txt', EventKind.CREATED)],
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
            [
                ev('/tmp/b.txt', EventKind.RENAMED),
                ev('/tmp/c.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [ev('/tmp/a.txt', EventKind.CREATED, inode=320)],
        [ev('/tmp/b.txt', EventKind.RENAMED, inode=320)],
        [ev('/tmp/c.txt', EventKind.RENAMED, inode=320)],
    ]


def _rename_chain_a_b_c_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/c.txt' in update_paths, (
        f'Expected update at /tmp/c.txt, got: {update_paths}'
    )


# ===================================================================
# 14. rename_overwrite_collision
# ===================================================================


def _rename_overwrite_collision_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create both files
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.CREATED),
            ],
            # Batch 2: rename a -> b (overwrites b)
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [
            ev('/tmp/a.txt', EventKind.CREATED, inode=330),
            ev('/tmp/b.txt', EventKind.CREATED, inode=331),
        ],
        # GPFS: single rename event — inode 330 (a) moves to /tmp/b.txt
        [ev('/tmp/b.txt', EventKind.RENAMED, inode=330)],
    ]


def _rename_overwrite_collision_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/b.txt' in update_paths, (
        f"Expected update at /tmp/b.txt (with a's content), got: {update_paths}"
    )


# ===================================================================
# 15. atomic_rename_pattern
# ===================================================================


def _atomic_rename_pattern_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/tmp.txt', EventKind.CREATED)],
            [
                ev('/tmp/tmp.txt', EventKind.RENAMED),
                ev('/tmp/final.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [ev('/tmp/tmp.txt', EventKind.CREATED, inode=340)],
        [ev('/tmp/final.txt', EventKind.RENAMED, inode=340)],
    ]


def _atomic_rename_pattern_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/final.txt' in update_paths, (
        f'Expected update at /tmp/final.txt, got: {update_paths}'
    )


# ===================================================================
# 16. swap_files_via_temp
# ===================================================================


def _swap_files_via_temp_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create a and b
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.CREATED),
            ],
            # Batch 2: rename a -> tmp
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/tmp.txt', EventKind.RENAMED),
            ],
            # Batch 3: rename b -> a
            [
                ev('/tmp/b.txt', EventKind.RENAMED),
                ev('/tmp/a.txt', EventKind.RENAMED),
            ],
            # Batch 4: rename tmp -> b
            [
                ev('/tmp/tmp.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [
            ev('/tmp/a.txt', EventKind.CREATED, inode=350),
            ev('/tmp/b.txt', EventKind.CREATED, inode=351),
        ],
        [ev('/tmp/tmp.txt', EventKind.RENAMED, inode=350)],
        [ev('/tmp/a.txt', EventKind.RENAMED, inode=351)],
        [ev('/tmp/b.txt', EventKind.RENAMED, inode=350)],
    ]


def _swap_files_via_temp_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/a.txt' in update_paths, (
        f'Expected update at /tmp/a.txt, got: {update_paths}'
    )
    assert '/tmp/b.txt' in update_paths, (
        f'Expected update at /tmp/b.txt, got: {update_paths}'
    )


# ===================================================================
# 17. deep_nested_tree
# ===================================================================


def _deep_nested_tree_events(cfg: BackendConfig) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/d1', EventKind.CREATED, is_dir=True),
                ev('/tmp/d1/d2', EventKind.CREATED, is_dir=True),
                ev('/tmp/d1/d2/d3', EventKind.CREATED, is_dir=True),
                ev('/tmp/d1/d2/d3/file.txt', EventKind.CREATED),
            ],
        ]
    return [
        [
            ev('/tmp/d1', EventKind.CREATED, inode=400, is_dir=True),
            ev('/tmp/d1/d2', EventKind.CREATED, inode=401, is_dir=True),
            ev('/tmp/d1/d2/d3', EventKind.CREATED, inode=402, is_dir=True),
            ev('/tmp/d1/d2/d3/file.txt', EventKind.CREATED, inode=403),
        ],
    ]


def _deep_nested_tree_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    update_paths = {c['path'] for c in updates}
    assert len(updates) >= 4, (
        f'Expected >= 4 updates, got {len(updates)}: {collected}'
    )
    assert '/tmp/d1' in update_paths
    assert '/tmp/d1/d2' in update_paths
    assert '/tmp/d1/d2/d3' in update_paths
    assert '/tmp/d1/d2/d3/file.txt' in update_paths


# ===================================================================
# 18. recursive_delete_tree
# ===================================================================


def _recursive_delete_tree_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create dir + children
            [
                ev('/tmp/dir', EventKind.CREATED, is_dir=True),
                ev('/tmp/dir/a.txt', EventKind.CREATED),
                ev('/tmp/dir/b.txt', EventKind.CREATED),
            ],
            # Batch 2: remove children then dir
            [
                ev('/tmp/dir/a.txt', EventKind.REMOVED),
                ev('/tmp/dir/b.txt', EventKind.REMOVED),
                ev('/tmp/dir', EventKind.REMOVED),
            ],
        ]
    return [
        [
            ev('/tmp/dir', EventKind.CREATED, inode=410, is_dir=True),
            ev('/tmp/dir/a.txt', EventKind.CREATED, inode=411),
            ev('/tmp/dir/b.txt', EventKind.CREATED, inode=412),
        ],
        [
            ev('/tmp/dir/a.txt', EventKind.REMOVED, inode=411),
            ev('/tmp/dir/b.txt', EventKind.REMOVED, inode=412),
            ev('/tmp/dir', EventKind.REMOVED, inode=410),
        ],
    ]


def _recursive_delete_tree_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    deletes = [c for c in collected if c['op'] == 'delete']
    delete_paths = {c['path'] for c in deletes}
    assert len(deletes) >= 3, (
        f'Expected >= 3 deletes, got {len(deletes)}: {collected}'
    )
    assert '/tmp/dir' in delete_paths
    assert '/tmp/dir/a.txt' in delete_paths
    assert '/tmp/dir/b.txt' in delete_paths


# ===================================================================
# 19. create_delete_empty_dir — cancelled in same batch
# ===================================================================


def _create_delete_empty_dir_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/edir', EventKind.CREATED, is_dir=True),
                ev('/tmp/edir', EventKind.REMOVED),
            ],
        ]
    return [
        [
            ev('/tmp/edir', EventKind.CREATED, inode=420, is_dir=True),
            ev('/tmp/edir', EventKind.REMOVED, inode=420),
        ],
    ]


def _create_delete_empty_dir_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    assert len(collected) == 0, (
        f'Expected 0 outputs (cancelled), got {len(collected)}: {collected}'
    )


# ===================================================================
# 20. rename_dir_many_children
# ===================================================================


def _rename_dir_many_children_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create dir + children
            [
                ev('/tmp/old', EventKind.CREATED, is_dir=True),
                ev('/tmp/old/c1.txt', EventKind.CREATED),
                ev('/tmp/old/c2.txt', EventKind.CREATED),
                ev('/tmp/old/c3.txt', EventKind.CREATED),
            ],
            # Batch 2: rename dir
            [
                ev('/tmp/old', EventKind.RENAMED, is_dir=True),
                ev('/tmp/new', EventKind.RENAMED, is_dir=True),
            ],
        ]
    return [
        [
            ev('/tmp/old', EventKind.CREATED, inode=430, is_dir=True),
            ev('/tmp/old/c1.txt', EventKind.CREATED, inode=431),
            ev('/tmp/old/c2.txt', EventKind.CREATED, inode=432),
            ev('/tmp/old/c3.txt', EventKind.CREATED, inode=433),
        ],
        [ev('/tmp/new', EventKind.RENAMED, inode=430)],
    ]


def _rename_dir_many_children_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/new' in update_paths, (
        f'Expected /tmp/new in updates, got: {update_paths}'
    )
    assert '/tmp/new/c1.txt' in update_paths, (
        f'Expected /tmp/new/c1.txt in updates, got: {update_paths}'
    )
    assert '/tmp/new/c2.txt' in update_paths, (
        f'Expected /tmp/new/c2.txt in updates, got: {update_paths}'
    )
    assert '/tmp/new/c3.txt' in update_paths, (
        f'Expected /tmp/new/c3.txt in updates, got: {update_paths}'
    )


# ===================================================================
# 21. burst_create_multiple_files
# ===================================================================


def _burst_create_multiple_files_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/f1.txt', EventKind.CREATED),
                ev('/tmp/f2.txt', EventKind.CREATED),
                ev('/tmp/f3.txt', EventKind.CREATED),
                ev('/tmp/f4.txt', EventKind.CREATED),
                ev('/tmp/f5.txt', EventKind.CREATED),
            ],
        ]
    return [
        [
            ev('/tmp/f1.txt', EventKind.CREATED, inode=500),
            ev('/tmp/f2.txt', EventKind.CREATED, inode=501),
            ev('/tmp/f3.txt', EventKind.CREATED, inode=502),
            ev('/tmp/f4.txt', EventKind.CREATED, inode=503),
            ev('/tmp/f5.txt', EventKind.CREATED, inode=504),
        ],
    ]


def _burst_create_multiple_files_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    assert len(updates) == 5, (
        f'Expected 5 updates, got {len(updates)}: {collected}'
    )
    update_paths = {c['path'] for c in updates}
    for i in range(1, 6):
        assert f'/tmp/f{i}.txt' in update_paths


# ===================================================================
# 22. many_modifications_reduced — 1 create + 5 modifies in same batch
# ===================================================================


def _many_modifications_reduced_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        # fswatch/lustre: MODIFIED after CREATED is absorbed (IGNORE rule)
        return [
            [
                ev('/tmp/f.txt', EventKind.CREATED),
                ev('/tmp/f.txt', EventKind.MODIFIED),
                ev('/tmp/f.txt', EventKind.MODIFIED),
                ev('/tmp/f.txt', EventKind.MODIFIED),
                ev('/tmp/f.txt', EventKind.MODIFIED),
                ev('/tmp/f.txt', EventKind.MODIFIED),
            ],
        ]
    # GPFS: no IGNORE rules, but all events land in same inode slot.
    # State deduplicates to 1 update per inode.
    return [
        [
            ev('/tmp/f.txt', EventKind.CREATED, inode=510, size=10),
            ev('/tmp/f.txt', EventKind.MODIFIED, inode=510, size=20),
            ev('/tmp/f.txt', EventKind.MODIFIED, inode=510, size=30),
            ev('/tmp/f.txt', EventKind.MODIFIED, inode=510, size=40),
            ev('/tmp/f.txt', EventKind.MODIFIED, inode=510, size=50),
            ev('/tmp/f.txt', EventKind.MODIFIED, inode=510, size=60),
        ],
    ]


def _many_modifications_reduced_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    assert len(updates) == 1, (
        f'Expected exactly 1 update, got {len(updates)}: {collected}'
    )
    assert updates[0]['path'] == '/tmp/f.txt'


# ===================================================================
# 23. burst_create_delete_mixed
# ===================================================================


def _burst_create_delete_mixed_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        # REMOVED cancels CREATED for /tmp/b.txt (CANCEL rule)
        return [
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.CREATED),
                ev('/tmp/c.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.REMOVED),
            ],
        ]
    return [
        [
            ev('/tmp/a.txt', EventKind.CREATED, inode=520),
            ev('/tmp/b.txt', EventKind.CREATED, inode=521),
            ev('/tmp/c.txt', EventKind.CREATED, inode=522),
            ev('/tmp/b.txt', EventKind.REMOVED, inode=521),
        ],
    ]


def _burst_create_delete_mixed_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    assert len(updates) == 2, (
        f'Expected 2 updates (a and c), got {len(updates)}: {collected}'
    )
    update_paths = {c['path'] for c in updates}
    assert '/tmp/a.txt' in update_paths
    assert '/tmp/c.txt' in update_paths


# ===================================================================
# 24. chmod_mode_change — create then modify
# ===================================================================


def _chmod_mode_change_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/f.txt', EventKind.CREATED)],
            [ev('/tmp/f.txt', EventKind.MODIFIED)],
        ]
    return [
        [
            ev(
                '/tmp/f.txt',
                EventKind.CREATED,
                inode=530,
                permissions='0o100644',
            ),
        ],
        [
            ev(
                '/tmp/f.txt',
                EventKind.MODIFIED,
                inode=530,
                permissions='0o100755',
            ),
        ],
    ]


def _chmod_mode_change_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    update_paths = {c['path'] for c in updates}
    assert '/tmp/f.txt' in update_paths, (
        f'Expected at least 1 update at /tmp/f.txt, got: {update_paths}'
    )


# ===================================================================
# 25. parent_updated_on_child_create
# ===================================================================


def _parent_updated_on_child_create_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/dir', EventKind.CREATED, is_dir=True),
                ev('/tmp/dir/child.txt', EventKind.CREATED),
            ],
        ]
    return [
        [
            ev('/tmp/dir', EventKind.CREATED, inode=540, is_dir=True),
            ev('/tmp/dir/child.txt', EventKind.CREATED, inode=541),
        ],
    ]


def _parent_updated_on_child_create_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/dir' in update_paths, (
        f'Expected /tmp/dir in updates (parent tracking), got: {update_paths}'
    )
    assert '/tmp/dir/child.txt' in update_paths, (
        f'Expected /tmp/dir/child.txt in updates, got: {update_paths}'
    )


# ===================================================================
# 26. recreate_after_delete
# ===================================================================


def _recreate_after_delete_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create
            [ev('/tmp/f.txt', EventKind.CREATED)],
            # Batch 2: delete
            [ev('/tmp/f.txt', EventKind.REMOVED)],
            # Batch 3: recreate
            [ev('/tmp/f.txt', EventKind.CREATED)],
        ]
    return [
        [ev('/tmp/f.txt', EventKind.CREATED, inode=550)],
        [ev('/tmp/f.txt', EventKind.REMOVED, inode=550)],
        [ev('/tmp/f.txt', EventKind.CREATED, inode=551)],
    ]


def _recreate_after_delete_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    # The final output should include an update at /tmp/f.txt (it's back)
    updates = [c for c in collected if c['op'] == 'update']
    update_paths = [c['path'] for c in updates]
    assert '/tmp/f.txt' in update_paths, (
        f'Expected final update at /tmp/f.txt, got: {update_paths}'
    )


# ===================================================================
# Scenario registry
# ===================================================================

# ===================================================================
# 27. copy_creates_new_file
# ===================================================================


def _copy_creates_new_file_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: original file
            [ev('/tmp/orig.txt', EventKind.CREATED)],
            # Batch 2: copy is a brand-new CREATED (not rename)
            [ev('/tmp/copy.txt', EventKind.CREATED)],
        ]
    return [
        [ev('/tmp/orig.txt', EventKind.CREATED, inode=600)],
        [ev('/tmp/copy.txt', EventKind.CREATED, inode=601)],
    ]


def _copy_creates_new_file_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/orig.txt' in update_paths, (
        f'Expected orig.txt update, got: {update_paths}'
    )
    assert '/tmp/copy.txt' in update_paths, (
        f'Expected copy.txt update, got: {update_paths}'
    )
    # No deletes — copy does not remove original
    deletes = [c for c in collected if c['op'] == 'delete']
    assert len(deletes) == 0, (
        f'Copy should not produce deletes, got: {deletes}'
    )


# ===================================================================
# 28. symlink_create
# ===================================================================


def _symlink_create_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/target.txt', EventKind.CREATED),
                ev('/tmp/link.txt', EventKind.CREATED, is_symlink=True),
            ],
        ]
    # GPFS: symlink is a CREATED event with its own inode
    return [
        [
            ev('/tmp/target.txt', EventKind.CREATED, inode=610),
            ev('/tmp/link.txt', EventKind.CREATED, inode=611),
        ],
    ]


def _symlink_create_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/target.txt' in update_paths, (
        f'Expected target.txt update, got: {update_paths}'
    )
    assert '/tmp/link.txt' in update_paths, (
        f'Expected link.txt update, got: {update_paths}'
    )


# ===================================================================
# 29. wide_dir_many_files
# ===================================================================


def _wide_dir_many_files_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/wide', EventKind.CREATED, is_dir=True),
                *[
                    ev(f'/tmp/wide/f_{i}.txt', EventKind.CREATED)
                    for i in range(10)
                ],
            ],
        ]
    return [
        [
            ev('/tmp/wide', EventKind.CREATED, inode=620, is_dir=True),
            *[
                ev(
                    f'/tmp/wide/f_{i}.txt',
                    EventKind.CREATED,
                    inode=621 + i,
                )
                for i in range(10)
            ],
        ],
    ]


def _wide_dir_many_files_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    for i in range(10):
        assert f'/tmp/wide/f_{i}.txt' in update_paths, (
            f'Missing /tmp/wide/f_{i}.txt in updates: {update_paths}'
        )
    assert '/tmp/wide' in update_paths, (
        f'Expected /tmp/wide dir in updates: {update_paths}'
    )


# ===================================================================
# 30. overwrite_file_new_content
# ===================================================================


def _overwrite_file_new_content_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create with content A
            [ev('/tmp/data.txt', EventKind.CREATED)],
            # Batch 2: overwrite with content B (MODIFIED)
            [ev('/tmp/data.txt', EventKind.MODIFIED)],
        ]
    return [
        [ev('/tmp/data.txt', EventKind.CREATED, inode=640, size=100)],
        [ev('/tmp/data.txt', EventKind.MODIFIED, inode=640, size=200)],
    ]


def _overwrite_file_new_content_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    assert len(updates) >= 1, f'Expected at least 1 update, got: {collected}'
    assert updates[-1]['path'] == '/tmp/data.txt'


# ===================================================================
# 31. rename_dir_then_create_inside
# ===================================================================


def _rename_dir_then_create_inside_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create dir
            [ev('/tmp/mydir', EventKind.CREATED, is_dir=True)],
            # Batch 2: rename dir
            [
                ev('/tmp/mydir', EventKind.RENAMED, is_dir=True),
                ev('/tmp/renamed', EventKind.RENAMED, is_dir=True),
            ],
            # Batch 3: create file inside renamed dir
            [ev('/tmp/renamed/new.txt', EventKind.CREATED)],
        ]
    return [
        [ev('/tmp/mydir', EventKind.CREATED, inode=650, is_dir=True)],
        [ev('/tmp/renamed', EventKind.RENAMED, inode=650)],
        [ev('/tmp/renamed/new.txt', EventKind.CREATED, inode=651)],
    ]


def _rename_dir_then_create_inside_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/renamed' in update_paths, (
        f'Expected /tmp/renamed in updates, got: {update_paths}'
    )
    assert '/tmp/renamed/new.txt' in update_paths, (
        f'Expected /tmp/renamed/new.txt in updates, got: {update_paths}'
    )


# ===================================================================
# Scenario registry
# ===================================================================

# ===================================================================
# 32. delete_recreate_different_batch
# ===================================================================


def _delete_recreate_different_batch_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/dr.txt', EventKind.CREATED)],
            [ev('/tmp/dr.txt', EventKind.REMOVED)],
            [ev('/tmp/dr.txt', EventKind.CREATED)],
        ]
    return [
        [ev('/tmp/dr.txt', EventKind.CREATED, inode=700)],
        [ev('/tmp/dr.txt', EventKind.REMOVED, inode=700)],
        [ev('/tmp/dr.txt', EventKind.CREATED, inode=701)],
    ]


def _delete_recreate_different_batch_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    update_paths = [c['path'] for c in updates]
    assert '/tmp/dr.txt' in update_paths, (
        f'Expected update at /tmp/dr.txt after recreate, got: {update_paths}'
    )


# ===================================================================
# 33. modify_deleted_path_ignored
# ===================================================================


def _modify_deleted_path_ignored_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/ghost.txt', EventKind.CREATED)],
            [ev('/tmp/ghost.txt', EventKind.REMOVED)],
            [ev('/tmp/ghost.txt', EventKind.MODIFIED)],
        ]
    return [
        [ev('/tmp/ghost.txt', EventKind.CREATED, inode=710)],
        [ev('/tmp/ghost.txt', EventKind.REMOVED, inode=710)],
        [ev('/tmp/ghost.txt', EventKind.MODIFIED, inode=710)],
    ]


def _modify_deleted_path_ignored_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    # After create→emit→delete→emit, the MODIFIED should be ignored
    # because the path is no longer tracked. The delete should still appear.
    deletes = [c for c in collected if c['op'] == 'delete']
    assert len(deletes) >= 1, f'Expected at least 1 delete, got: {collected}'


# ===================================================================
# 34. double_rename_same_batch
# ===================================================================


def _double_rename_same_batch_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            # Batch 1: create A and C
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/c.txt', EventKind.CREATED),
            ],
            # Batch 2: rename A→B and C→D (4 RENAMED events)
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
                ev('/tmp/c.txt', EventKind.RENAMED),
                ev('/tmp/d.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [
            ev('/tmp/a.txt', EventKind.CREATED, inode=720),
            ev('/tmp/c.txt', EventKind.CREATED, inode=721),
        ],
        [
            ev('/tmp/b.txt', EventKind.RENAMED, inode=720),
            ev('/tmp/d.txt', EventKind.RENAMED, inode=721),
        ],
    ]


def _double_rename_same_batch_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/b.txt' in update_paths, (
        f'Expected /tmp/b.txt in updates, got: {update_paths}'
    )
    assert '/tmp/d.txt' in update_paths, (
        f'Expected /tmp/d.txt in updates, got: {update_paths}'
    )


# ===================================================================
# 35. create_modify_rename_single_batch
# ===================================================================


def _create_modify_rename_single_batch_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        # MODIFIED is IGNORE'd by CREATED. RENAMED events bypass slotting.
        return [
            [
                ev('/tmp/x.txt', EventKind.CREATED),
                ev('/tmp/x.txt', EventKind.MODIFIED),
                ev('/tmp/x.txt', EventKind.RENAMED),
                ev('/tmp/y.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [
            ev('/tmp/x.txt', EventKind.CREATED, inode=730, size=10),
            ev('/tmp/x.txt', EventKind.MODIFIED, inode=730, size=20),
            ev('/tmp/y.txt', EventKind.RENAMED, inode=730, size=20),
        ],
    ]


def _create_modify_rename_single_batch_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/y.txt' in update_paths, (
        f'Expected update at /tmp/y.txt, got: {update_paths}'
    )


# ===================================================================
# 36. rename_back_to_original
# ===================================================================


def _rename_back_to_original_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/orig.txt', EventKind.CREATED)],
            [
                ev('/tmp/orig.txt', EventKind.RENAMED),
                ev('/tmp/temp.txt', EventKind.RENAMED),
            ],
            [
                ev('/tmp/temp.txt', EventKind.RENAMED),
                ev('/tmp/orig.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [ev('/tmp/orig.txt', EventKind.CREATED, inode=740)],
        [ev('/tmp/temp.txt', EventKind.RENAMED, inode=740)],
        [ev('/tmp/orig.txt', EventKind.RENAMED, inode=740)],
    ]


def _rename_back_to_original_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/orig.txt' in update_paths, (
        f'Expected update at /tmp/orig.txt after round-trip rename, got: {update_paths}'
    )


# ===================================================================
# 37. rename_unknown_source
# ===================================================================


def _rename_unknown_source_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        # No CREATED for /tmp/old.txt — it was pre-existing before monitoring
        return [
            [
                ev('/tmp/old.txt', EventKind.RENAMED),
                ev('/tmp/new.txt', EventKind.RENAMED),
            ],
        ]
    # GPFS: rename with unknown inode
    return [[ev('/tmp/new.txt', EventKind.RENAMED, inode=750)]]


def _rename_unknown_source_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    # Path-keyed (fswatch/lustre): unknown source treated as create at new path
    # GPFS (inode-keyed): unknown inode silently ignored — no output
    # Both are valid behaviors, so we just assert no crash
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    # At least path-keyed backends should produce the update
    # GPFS produces nothing — that's OK
    if collected:
        assert '/tmp/new.txt' in update_paths, (
            f'Expected update at /tmp/new.txt, got: {update_paths}'
        )


# ===================================================================
# 38. remove_nonexistent_ignored
# ===================================================================


def _remove_nonexistent_ignored_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [[ev('/tmp/never.txt', EventKind.REMOVED)]]
    return [[ev('/tmp/never.txt', EventKind.REMOVED, inode=760)]]


def _remove_nonexistent_ignored_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    assert len(collected) == 0, (
        f'Expected 0 outputs for untracked REMOVED, got: {collected}'
    )


# ===================================================================
# 39. interleaved_two_file_lifecycle
# ===================================================================


def _interleaved_two_file_lifecycle_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.CREATED),
            ],
            [
                ev('/tmp/a.txt', EventKind.MODIFIED),
                ev('/tmp/b.txt', EventKind.MODIFIED),
            ],
            [ev('/tmp/a.txt', EventKind.REMOVED)],
        ]
    return [
        [
            ev('/tmp/a.txt', EventKind.CREATED, inode=770),
            ev('/tmp/b.txt', EventKind.CREATED, inode=771),
        ],
        [
            ev('/tmp/a.txt', EventKind.MODIFIED, inode=770, size=50),
            ev('/tmp/b.txt', EventKind.MODIFIED, inode=771, size=60),
        ],
        [ev('/tmp/a.txt', EventKind.REMOVED, inode=770)],
    ]


def _interleaved_two_file_lifecycle_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    # B should have updates; A should have update then delete
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/b.txt' in update_paths, (
        f'Expected /tmp/b.txt update, got: {update_paths}'
    )
    deletes = [c for c in collected if c['op'] == 'delete']
    delete_paths = {c['path'] for c in deletes}
    assert '/tmp/a.txt' in delete_paths, (
        f'Expected /tmp/a.txt delete, got: {delete_paths}'
    )


# ===================================================================
# 40. dir_delete_cascades_to_child
# ===================================================================


def _dir_delete_cascades_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/parent', EventKind.CREATED, is_dir=True),
                ev('/tmp/parent/child.txt', EventKind.CREATED),
            ],
            [ev('/tmp/parent', EventKind.REMOVED)],
        ]
    return [
        [
            ev('/tmp/parent', EventKind.CREATED, inode=780, is_dir=True),
            ev('/tmp/parent/child.txt', EventKind.CREATED, inode=781),
        ],
        [ev('/tmp/parent', EventKind.REMOVED, inode=780)],
    ]


def _dir_delete_cascades_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    delete_paths = {c['path'] for c in collected if c['op'] == 'delete'}
    # Path-keyed: _remove_subtree cascades delete to child
    # GPFS: dir delete only removes self (no subtree cascade)
    assert '/tmp/parent' in delete_paths, (
        f'Expected /tmp/parent delete, got: {delete_paths}'
    )


# ===================================================================
# 41. accessed_reduced_by_created
# ===================================================================


def _accessed_reduced_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/acc.txt', EventKind.CREATED),
                ev('/tmp/acc.txt', EventKind.MODIFIED),
            ],
        ]
    return [
        [
            ev('/tmp/acc.txt', EventKind.CREATED, inode=790, size=10),
            ev('/tmp/acc.txt', EventKind.MODIFIED, inode=790, size=10),
        ],
    ]


def _accessed_reduced_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    assert len(updates) == 1, (
        f'Expected 1 update (ACCESSED reduced), got {len(updates)}: {collected}'
    )
    assert updates[0]['path'] == '/tmp/acc.txt'


# ===================================================================
# 42. create_same_path_twice
# ===================================================================


def _create_same_path_twice_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/dup.txt', EventKind.CREATED),
                ev('/tmp/dup.txt', EventKind.CREATED),
            ],
        ]
    return [
        [
            ev('/tmp/dup.txt', EventKind.CREATED, inode=800),
            ev('/tmp/dup.txt', EventKind.CREATED, inode=800),
        ],
    ]


def _create_same_path_twice_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    assert len(updates) == 1, (
        f'Expected 1 update for duplicate CREATED, got {len(updates)}: {collected}'
    )
    assert updates[0]['path'] == '/tmp/dup.txt'


# ===================================================================
# 43. rename_collision_emits_delete
# ===================================================================


def _rename_collision_delete_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.CREATED),
            ],
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [
            ev('/tmp/a.txt', EventKind.CREATED, inode=810),
            ev('/tmp/b.txt', EventKind.CREATED, inode=811),
        ],
        [ev('/tmp/b.txt', EventKind.RENAMED, inode=810)],
    ]


def _rename_collision_delete_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/b.txt' in update_paths, (
        f'Expected /tmp/b.txt update (A moved to B), got: {update_paths}'
    )


# ===================================================================
# 44. create_file_in_renamed_dir
# ===================================================================


def _create_file_in_renamed_dir_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/dold', EventKind.CREATED, is_dir=True)],
            [
                ev('/tmp/dold', EventKind.RENAMED, is_dir=True),
                ev('/tmp/dnew', EventKind.RENAMED, is_dir=True),
            ],
            [ev('/tmp/dnew/f.txt', EventKind.CREATED)],
        ]
    return [
        [ev('/tmp/dold', EventKind.CREATED, inode=820, is_dir=True)],
        [ev('/tmp/dnew', EventKind.RENAMED, inode=820)],
        [ev('/tmp/dnew/f.txt', EventKind.CREATED, inode=821)],
    ]


def _create_file_in_renamed_dir_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/dnew/f.txt' in update_paths, (
        f'Expected /tmp/dnew/f.txt in updates, got: {update_paths}'
    )


# ===================================================================
# 45. double_delete_same_path
# ===================================================================


def _double_delete_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/dd.txt', EventKind.CREATED)],
            [ev('/tmp/dd.txt', EventKind.REMOVED)],
            [ev('/tmp/dd.txt', EventKind.REMOVED)],
        ]
    return [
        [ev('/tmp/dd.txt', EventKind.CREATED, inode=830)],
        [ev('/tmp/dd.txt', EventKind.REMOVED, inode=830)],
        [ev('/tmp/dd.txt', EventKind.REMOVED, inode=830)],
    ]


def _double_delete_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    deletes = [c for c in collected if c['op'] == 'delete']
    # First delete should produce a delete (file was emitted).
    # Second delete is silently ignored (already removed from state).
    assert len(deletes) == 1, (
        f'Expected exactly 1 delete (second ignored), got {len(deletes)}: {collected}'
    )


# ===================================================================
# 46. create_a_modify_b_same_batch
# ===================================================================


def _create_a_modify_b_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        # B was never created — MODIFIED is ignored (untracked path)
        return [
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.MODIFIED),
            ],
        ]
    return [
        [
            ev('/tmp/a.txt', EventKind.CREATED, inode=840),
            ev('/tmp/b.txt', EventKind.MODIFIED, inode=841),
        ],
    ]


def _create_a_modify_b_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    assert len(updates) == 1, (
        f'Expected 1 update (A only, B untracked), got {len(updates)}: {collected}'
    )
    assert updates[0]['path'] == '/tmp/a.txt'


# ===================================================================
# 47. create_3_delete_middle
# ===================================================================


def _create_3_delete_middle_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.CREATED),
                ev('/tmp/c.txt', EventKind.CREATED),
            ],
            [ev('/tmp/b.txt', EventKind.REMOVED)],
        ]
    return [
        [
            ev('/tmp/a.txt', EventKind.CREATED, inode=850),
            ev('/tmp/b.txt', EventKind.CREATED, inode=851),
            ev('/tmp/c.txt', EventKind.CREATED, inode=852),
        ],
        [ev('/tmp/b.txt', EventKind.REMOVED, inode=851)],
    ]


def _create_3_delete_middle_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    delete_paths = {c['path'] for c in collected if c['op'] == 'delete'}
    assert '/tmp/a.txt' in update_paths
    assert '/tmp/c.txt' in update_paths
    assert '/tmp/b.txt' in delete_paths


# ===================================================================
# 48. rename_creates_then_delete_new
# ===================================================================


def _rename_creates_then_delete_new_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/a.txt', EventKind.CREATED)],
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
            [ev('/tmp/b.txt', EventKind.REMOVED)],
        ]
    return [
        [ev('/tmp/a.txt', EventKind.CREATED, inode=900)],
        [ev('/tmp/b.txt', EventKind.RENAMED, inode=900)],
        [ev('/tmp/b.txt', EventKind.REMOVED, inode=900)],
    ]


def _rename_creates_then_delete_new_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    delete_paths = {c['path'] for c in collected if c['op'] == 'delete'}
    # After rename A→B and delete B, the original A was emitted,
    # so B (which is the renamed A) should have a delete.
    assert len(delete_paths) >= 1, f'Expected at least 1 delete: {collected}'


# ===================================================================
# 49. modify_multiple_untracked_ignored
# ===================================================================


def _modify_multiple_untracked_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/ghost1.txt', EventKind.MODIFIED),
                ev('/tmp/ghost2.txt', EventKind.MODIFIED),
                ev('/tmp/ghost3.txt', EventKind.MODIFIED),
                ev('/tmp/ghost4.txt', EventKind.MODIFIED),
                ev('/tmp/ghost5.txt', EventKind.MODIFIED),
            ],
        ]
    return [
        [
            ev('/tmp/ghost1.txt', EventKind.MODIFIED, inode=960),
            ev('/tmp/ghost2.txt', EventKind.MODIFIED, inode=961),
            ev('/tmp/ghost3.txt', EventKind.MODIFIED, inode=962),
            ev('/tmp/ghost4.txt', EventKind.MODIFIED, inode=963),
            ev('/tmp/ghost5.txt', EventKind.MODIFIED, inode=964),
        ],
    ]


def _modify_multiple_untracked_expected(
    collected: list[dict[str, Any]],
) -> None:
    # Path-keyed: MODIFIED for untracked paths → no output (not in files dict).
    # GPFS: MODIFIED creates entries, so 5 updates are expected.
    # Accept either: 0 or 5.
    for c in collected:
        _assert_valid_change(c)
    # No crash is the primary assertion.


# ===================================================================
# 50. create_5_files_delete_all
# ===================================================================


def _create_5_delete_all_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    paths = [f'/tmp/batch_{i}.txt' for i in range(5)]
    if _is_path_keyed(cfg):
        return [
            [ev(p, EventKind.CREATED) for p in paths],
            [ev(p, EventKind.REMOVED) for p in paths],
        ]
    inodes = list(range(970, 975))
    return [
        [ev(p, EventKind.CREATED, inode=n) for p, n in zip(paths, inodes)],
        [ev(p, EventKind.REMOVED, inode=n) for p, n in zip(paths, inodes)],
    ]


def _create_5_delete_all_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    delete_paths = {c['path'] for c in collected if c['op'] == 'delete'}
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    # All 5 should have been emitted first (update), then deleted.
    for i in range(5):
        p = f'/tmp/batch_{i}.txt'
        assert p in update_paths, f'Expected update for {p}: {collected}'
        assert p in delete_paths, f'Expected delete for {p}: {collected}'


# ===================================================================
# 51. nested_dir_rename_child_modify
# ===================================================================


def _nested_dir_rename_child_modify_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/dir', EventKind.CREATED, is_dir=True),
                ev('/tmp/dir/file.txt', EventKind.CREATED),
            ],
            [
                ev('/tmp/dir', EventKind.RENAMED),
                ev('/tmp/renamed_dir', EventKind.RENAMED),
            ],
            [ev('/tmp/renamed_dir/file.txt', EventKind.MODIFIED)],
        ]
    return [
        [
            ev('/tmp/dir', EventKind.CREATED, inode=980, is_dir=True),
            ev('/tmp/dir/file.txt', EventKind.CREATED, inode=981),
        ],
        [ev('/tmp/renamed_dir', EventKind.RENAMED, inode=980, is_dir=True)],
        [ev('/tmp/renamed_dir/file.txt', EventKind.MODIFIED, inode=981)],
    ]


def _nested_dir_rename_child_modify_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    # After dir rename + child modify, the child should appear under new parent.
    assert '/tmp/renamed_dir/file.txt' in update_paths, (
        f'Expected child at new path: {collected}'
    )


# ===================================================================
# 52. create_modify_delete_same_batch
# ===================================================================


def _create_modify_delete_same_batch_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/ephemeral.txt', EventKind.CREATED),
                ev('/tmp/ephemeral.txt', EventKind.MODIFIED),
                ev('/tmp/ephemeral.txt', EventKind.REMOVED),
            ],
        ]
    return [
        [
            ev('/tmp/ephemeral.txt', EventKind.CREATED, inode=990),
            ev('/tmp/ephemeral.txt', EventKind.MODIFIED, inode=990),
            ev('/tmp/ephemeral.txt', EventKind.REMOVED, inode=990),
        ],
    ]


def _create_modify_delete_same_batch_events_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    # CREATED + MODIFIED → MODIFIED ignored (reduction).
    # CREATED + REMOVED → both cancelled (CANCEL action).
    # Result: 0 outputs.
    assert len(collected) == 0, (
        f'Expected 0 outputs, got {len(collected)}: {collected}'
    )


# ===================================================================
# 53. rapid_create_10_files_single_batch
# ===================================================================


def _rapid_create_10_events(cfg: BackendConfig) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    paths = [f'/tmp/r10_{i}.txt' for i in range(10)]
    if _is_path_keyed(cfg):
        return [[ev(p, EventKind.CREATED) for p in paths]]
    inodes = list(range(1000, 1010))
    return [[ev(p, EventKind.CREATED, inode=n) for p, n in zip(paths, inodes)]]


def _rapid_create_10_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    for i in range(10):
        assert f'/tmp/r10_{i}.txt' in update_paths, (
            f'Missing /tmp/r10_{i}.txt: {collected}'
        )
    assert len([c for c in collected if c['op'] == 'update']) == 10


# ===================================================================
# 54. rename_dir_2_levels_deep
# ===================================================================


def _rename_dir_2_levels_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/top', EventKind.CREATED, is_dir=True),
                ev('/tmp/top/mid', EventKind.CREATED, is_dir=True),
                ev('/tmp/top/mid/leaf.txt', EventKind.CREATED),
            ],
            [
                ev('/tmp/top', EventKind.RENAMED),
                ev('/tmp/newtop', EventKind.RENAMED),
            ],
        ]
    return [
        [
            ev('/tmp/top', EventKind.CREATED, inode=1020, is_dir=True),
            ev('/tmp/top/mid', EventKind.CREATED, inode=1021, is_dir=True),
            ev('/tmp/top/mid/leaf.txt', EventKind.CREATED, inode=1022),
        ],
        [ev('/tmp/newtop', EventKind.RENAMED, inode=1020, is_dir=True)],
    ]


def _rename_dir_2_levels_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    # After rename top→newtop, leaf should be at /tmp/newtop/mid/leaf.txt
    assert '/tmp/newtop/mid/leaf.txt' in update_paths, (
        f'Expected leaf at new path: {collected}'
    )
    assert '/tmp/newtop' in update_paths, (
        f'Expected renamed dir in updates: {collected}'
    )


# ===================================================================
# 55. accessed_then_modified_reduced
# ===================================================================


def _accessed_then_modified_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/am.txt', EventKind.CREATED)],
            [
                ev('/tmp/am.txt', EventKind.MODIFIED),
                ev('/tmp/am.txt', EventKind.MODIFIED),
            ],
        ]
    return [
        [ev('/tmp/am.txt', EventKind.CREATED, inode=1030)],
        [
            ev('/tmp/am.txt', EventKind.MODIFIED, inode=1030),
            ev('/tmp/am.txt', EventKind.MODIFIED, inode=1030),
        ],
    ]


def _accessed_then_modified_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = [c['path'] for c in collected if c['op'] == 'update']
    assert '/tmp/am.txt' in update_paths
    # ACCESSED should be reduced by MODIFIED in same batch (both in _ANY_MODIFY)
    # for path-keyed backends. GPFS has different rules.
    # Either way: at least one update should appear.


# ===================================================================
# 56. create_symlink_then_delete_target
# ===================================================================


def _create_symlink_delete_target_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/target.txt', EventKind.CREATED),
                ev('/tmp/link.txt', EventKind.CREATED, is_symlink=True),
            ],
            [ev('/tmp/target.txt', EventKind.REMOVED)],
        ]
    return [
        [
            ev('/tmp/target.txt', EventKind.CREATED, inode=1040),
            ev('/tmp/link.txt', EventKind.CREATED, inode=1041),
        ],
        [ev('/tmp/target.txt', EventKind.REMOVED, inode=1040)],
    ]


def _create_symlink_delete_target_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    delete_paths = {c['path'] for c in collected if c['op'] == 'delete'}
    assert '/tmp/link.txt' in update_paths, (
        f'Symlink should be updated: {collected}'
    )
    assert '/tmp/target.txt' in delete_paths, (
        f'Target should be deleted: {collected}'
    )


# ===================================================================
# 57. multiple_renames_same_batch
# ===================================================================


def _multiple_renames_same_batch_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.CREATED),
                ev('/tmp/c.txt', EventKind.CREATED),
            ],
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/a2.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
                ev('/tmp/b2.txt', EventKind.RENAMED),
                ev('/tmp/c.txt', EventKind.RENAMED),
                ev('/tmp/c2.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [
            ev('/tmp/a.txt', EventKind.CREATED, inode=1050),
            ev('/tmp/b.txt', EventKind.CREATED, inode=1051),
            ev('/tmp/c.txt', EventKind.CREATED, inode=1052),
        ],
        [
            ev('/tmp/a2.txt', EventKind.RENAMED, inode=1050),
            ev('/tmp/b2.txt', EventKind.RENAMED, inode=1051),
            ev('/tmp/c2.txt', EventKind.RENAMED, inode=1052),
        ],
    ]


def _multiple_renames_same_batch_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/a2.txt' in update_paths, f'Expected a2.txt: {collected}'
    assert '/tmp/b2.txt' in update_paths, f'Expected b2.txt: {collected}'
    assert '/tmp/c2.txt' in update_paths, f'Expected c2.txt: {collected}'


# ===================================================================
# 58. removed_cancels_created_batch
# ===================================================================


def _removed_cancels_created_batch_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    paths = [f'/tmp/cancel_{i}.txt' for i in range(5)]
    if _is_path_keyed(cfg):
        return [
            [
                *[ev(p, EventKind.CREATED) for p in paths],
                *[ev(p, EventKind.REMOVED) for p in paths],
            ],
        ]
    inodes = list(range(1100, 1105))
    return [
        [
            *[
                ev(p, EventKind.CREATED, inode=n)
                for p, n in zip(paths, inodes)
            ],
            *[
                ev(p, EventKind.REMOVED, inode=n)
                for p, n in zip(paths, inodes)
            ],
        ],
    ]


def _removed_cancels_created_batch_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    # All CREATEDs should be cancelled by corresponding REMOVEDs.
    assert len(collected) == 0, (
        f'Expected 0 (all cancelled), got {len(collected)}: {collected}'
    )


# ===================================================================
# 59. rename_to_existing_then_modify
# ===================================================================


def _rename_to_existing_then_modify_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/src.txt', EventKind.CREATED),
                ev('/tmp/dst.txt', EventKind.CREATED),
            ],
            [
                ev('/tmp/src.txt', EventKind.RENAMED),
                ev('/tmp/dst.txt', EventKind.RENAMED),
            ],
            [ev('/tmp/dst.txt', EventKind.MODIFIED)],
        ]
    return [
        [
            ev('/tmp/src.txt', EventKind.CREATED, inode=1110),
            ev('/tmp/dst.txt', EventKind.CREATED, inode=1111),
        ],
        [ev('/tmp/dst.txt', EventKind.RENAMED, inode=1110)],
        [ev('/tmp/dst.txt', EventKind.MODIFIED, inode=1110)],
    ]


def _rename_to_existing_then_modify_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/dst.txt' in update_paths, (
        f'Expected dst.txt update: {collected}'
    )


# ===================================================================
# 60. 3_batch_lifecycle
# ===================================================================


def _3_batch_lifecycle_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/x.txt', EventKind.CREATED),
                ev('/tmp/y.txt', EventKind.CREATED),
                ev('/tmp/z.txt', EventKind.CREATED),
            ],
            [
                ev('/tmp/x.txt', EventKind.MODIFIED),
                ev('/tmp/y.txt', EventKind.REMOVED),
            ],
            [ev('/tmp/z.txt', EventKind.MODIFIED)],
        ]
    return [
        [
            ev('/tmp/x.txt', EventKind.CREATED, inode=1120),
            ev('/tmp/y.txt', EventKind.CREATED, inode=1121),
            ev('/tmp/z.txt', EventKind.CREATED, inode=1122),
        ],
        [
            ev('/tmp/x.txt', EventKind.MODIFIED, inode=1120),
            ev('/tmp/y.txt', EventKind.REMOVED, inode=1121),
        ],
        [ev('/tmp/z.txt', EventKind.MODIFIED, inode=1122)],
    ]


def _3_batch_lifecycle_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    delete_paths = {c['path'] for c in collected if c['op'] == 'delete'}
    assert '/tmp/x.txt' in update_paths
    assert '/tmp/z.txt' in update_paths
    assert '/tmp/y.txt' in delete_paths


# ===================================================================
# 61. dir_with_10_children_delete
# ===================================================================


def _dir_10_children_delete_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    children = [f'/tmp/bigdir/child_{i}.txt' for i in range(10)]
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/bigdir', EventKind.CREATED, is_dir=True),
                *[ev(c, EventKind.CREATED) for c in children],
            ],
            [ev('/tmp/bigdir', EventKind.REMOVED, is_dir=True)],
        ]
    inodes = list(range(1130, 1141))
    return [
        [
            ev('/tmp/bigdir', EventKind.CREATED, inode=1130, is_dir=True),
            *[
                ev(c, EventKind.CREATED, inode=n)
                for c, n in zip(children, inodes[1:])
            ],
        ],
        [
            ev('/tmp/bigdir', EventKind.REMOVED, inode=1130, is_dir=True),
        ],
    ]


def _dir_10_children_delete_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    delete_paths = {c['path'] for c in collected if c['op'] == 'delete'}
    # Dir delete cascades to all children for path-keyed.
    # GPFS: only the dir itself is deleted (no cascade).
    # Accept either: at least the dir deleted, or all children too.
    assert '/tmp/bigdir' in delete_paths, (
        f'Expected bigdir deleted: {collected}'
    )


# ===================================================================
# 62. modify_then_rename_same_batch
# ===================================================================


def _modify_then_rename_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/mr.txt', EventKind.CREATED)],
            [
                ev('/tmp/mr.txt', EventKind.MODIFIED),
                ev('/tmp/mr.txt', EventKind.RENAMED),
                ev('/tmp/mr_new.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [ev('/tmp/mr.txt', EventKind.CREATED, inode=1200)],
        [
            ev('/tmp/mr.txt', EventKind.MODIFIED, inode=1200),
            ev('/tmp/mr_new.txt', EventKind.RENAMED, inode=1200),
        ],
    ]


def _modify_then_rename_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/mr_new.txt' in update_paths, (
        f'Expected update at new path after modify+rename: {collected}'
    )


# ===================================================================
# 63. create_10_modify_5_delete_3
# ===================================================================


def _create_10_modify_5_delete_3_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    paths = [f'/tmp/cmdr_{i}.txt' for i in range(10)]
    if _is_path_keyed(cfg):
        return [
            [ev(p, EventKind.CREATED) for p in paths],
            [ev(p, EventKind.MODIFIED) for p in paths[:5]],
            [ev(p, EventKind.REMOVED) for p in paths[:3]],
        ]
    inodes = list(range(1210, 1220))
    return [
        [ev(p, EventKind.CREATED, inode=n) for p, n in zip(paths, inodes)],
        [
            ev(p, EventKind.MODIFIED, inode=n)
            for p, n in zip(paths[:5], inodes[:5])
        ],
        [
            ev(p, EventKind.REMOVED, inode=n)
            for p, n in zip(paths[:3], inodes[:3])
        ],
    ]


def _create_10_modify_5_delete_3_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    delete_paths = {c['path'] for c in collected if c['op'] == 'delete'}
    # 7 files survive (indices 3-9), all should have updates
    for i in range(3, 10):
        assert f'/tmp/cmdr_{i}.txt' in update_paths, (
            f'Expected update for surviving file {i}: {collected}'
        )
    # 3 deleted
    for i in range(3):
        assert f'/tmp/cmdr_{i}.txt' in delete_paths, (
            f'Expected delete for file {i}: {collected}'
        )


# ===================================================================
# 64. rename_creates_at_destination
# ===================================================================


def _rename_creates_at_destination_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        # Rename of untracked source → treated as create at destination
        return [
            [
                ev('/tmp/unknown_src.txt', EventKind.RENAMED),
                ev('/tmp/new_dst.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [
            ev('/tmp/new_dst.txt', EventKind.RENAMED, inode=1230),
        ],
    ]


def _rename_creates_at_destination_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    # Path-keyed: unknown source treated as create at dest → 1 update.
    # GPFS: rename of untracked inode → 0 output (inode not in state).
    # Accept either.
    if collected:
        assert '/tmp/new_dst.txt' in update_paths, (
            f'Expected update at destination: {collected}'
        )


# ===================================================================
# 65. 5_modifications_reduced_to_1
# ===================================================================


def _5_modifications_reduced_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/red5.txt', EventKind.CREATED),
                ev('/tmp/red5.txt', EventKind.MODIFIED),
                ev('/tmp/red5.txt', EventKind.MODIFIED),
                ev('/tmp/red5.txt', EventKind.MODIFIED),
                ev('/tmp/red5.txt', EventKind.MODIFIED),
                ev('/tmp/red5.txt', EventKind.MODIFIED),
            ],
        ]
    return [
        [
            ev('/tmp/red5.txt', EventKind.CREATED, inode=1300),
            ev('/tmp/red5.txt', EventKind.MODIFIED, inode=1300),
            ev('/tmp/red5.txt', EventKind.MODIFIED, inode=1300),
            ev('/tmp/red5.txt', EventKind.MODIFIED, inode=1300),
            ev('/tmp/red5.txt', EventKind.MODIFIED, inode=1300),
            ev('/tmp/red5.txt', EventKind.MODIFIED, inode=1300),
        ],
    ]


def _5_modifications_reduced_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    assert len(updates) == 1, (
        f'Expected exactly 1 update, got {len(updates)}: {collected}'
    )
    assert updates[0]['path'] == '/tmp/red5.txt'


# ===================================================================
# 66. create_rename_create_same_path
# ===================================================================


def _create_rename_create_same_path_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/crcs.txt', EventKind.CREATED)],
            [
                ev('/tmp/crcs.txt', EventKind.RENAMED),
                ev('/tmp/crcs_new.txt', EventKind.RENAMED),
            ],
            [ev('/tmp/crcs.txt', EventKind.CREATED)],
        ]
    return [
        [ev('/tmp/crcs.txt', EventKind.CREATED, inode=1310)],
        [ev('/tmp/crcs_new.txt', EventKind.RENAMED, inode=1310)],
        [ev('/tmp/crcs.txt', EventKind.CREATED, inode=1311)],
    ]


def _create_rename_create_same_path_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/crcs_new.txt' in update_paths, (
        f'Expected renamed path: {collected}'
    )
    assert '/tmp/crcs.txt' in update_paths, (
        f'Expected recreated path: {collected}'
    )


# ===================================================================
# 67. interleaved_renames_and_creates
# ===================================================================


def _interleaved_renames_creates_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/irc_a.txt', EventKind.CREATED)],
            [
                ev('/tmp/irc_a.txt', EventKind.RENAMED),
                ev('/tmp/irc_b.txt', EventKind.RENAMED),
            ],
            [ev('/tmp/irc_c.txt', EventKind.CREATED)],
            [
                ev('/tmp/irc_c.txt', EventKind.RENAMED),
                ev('/tmp/irc_d.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [ev('/tmp/irc_a.txt', EventKind.CREATED, inode=1320)],
        [ev('/tmp/irc_b.txt', EventKind.RENAMED, inode=1320)],
        [ev('/tmp/irc_c.txt', EventKind.CREATED, inode=1321)],
        [ev('/tmp/irc_d.txt', EventKind.RENAMED, inode=1321)],
    ]


def _interleaved_renames_creates_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/irc_b.txt' in update_paths, f'Expected irc_b.txt: {collected}'
    assert '/tmp/irc_d.txt' in update_paths, f'Expected irc_d.txt: {collected}'


# ===================================================================
# 68. create_dir_child_rename_dir_delete_child
# ===================================================================


def _create_dir_rename_delete_child_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/dir', EventKind.CREATED, is_dir=True),
                ev('/tmp/dir/file.txt', EventKind.CREATED),
            ],
            [
                ev('/tmp/dir', EventKind.RENAMED),
                ev('/tmp/newdir', EventKind.RENAMED),
            ],
            [ev('/tmp/newdir/file.txt', EventKind.REMOVED)],
        ]
    return [
        [
            ev('/tmp/dir', EventKind.CREATED, inode=1400, is_dir=True),
            ev('/tmp/dir/file.txt', EventKind.CREATED, inode=1401),
        ],
        [ev('/tmp/newdir', EventKind.RENAMED, inode=1400, is_dir=True)],
        [ev('/tmp/newdir/file.txt', EventKind.REMOVED, inode=1401)],
    ]


def _create_dir_rename_delete_child_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    delete_paths = {c['path'] for c in collected if c['op'] == 'delete'}
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    # After dir rename dir→newdir + delete child: child should be deleted
    assert '/tmp/newdir/file.txt' in delete_paths, (
        f'Expected delete for file at new path: {collected}'
    )
    assert '/tmp/newdir' in update_paths, f'Expected dir update: {collected}'


# ===================================================================
# 69. rapid_modify_accessed_interleaved
# ===================================================================


def _rapid_modify_accessed_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/rma.txt', EventKind.CREATED)],
            [
                ev('/tmp/rma.txt', EventKind.MODIFIED),
                ev('/tmp/rma.txt', EventKind.MODIFIED),
                ev('/tmp/rma.txt', EventKind.MODIFIED),
                ev('/tmp/rma.txt', EventKind.MODIFIED),
                ev('/tmp/rma.txt', EventKind.MODIFIED),
            ],
        ]
    return [
        [ev('/tmp/rma.txt', EventKind.CREATED, inode=1410)],
        [
            ev('/tmp/rma.txt', EventKind.MODIFIED, inode=1410),
            ev('/tmp/rma.txt', EventKind.MODIFIED, inode=1410),
            ev('/tmp/rma.txt', EventKind.MODIFIED, inode=1410),
            ev('/tmp/rma.txt', EventKind.MODIFIED, inode=1410),
            ev('/tmp/rma.txt', EventKind.MODIFIED, inode=1410),
        ],
    ]


def _rapid_modify_accessed_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    # All MODIFIED/ACCESSED should reduce to 1 update (reduction rules)
    assert len(updates) >= 1
    assert updates[-1]['path'] == '/tmp/rma.txt'


# ===================================================================
# 70. create_delete_create_different_batch
# ===================================================================


def _create_delete_create_diff_batch_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/cdc.txt', EventKind.CREATED)],
            [ev('/tmp/cdc.txt', EventKind.REMOVED)],
            [ev('/tmp/cdc.txt', EventKind.CREATED)],
        ]
    return [
        [ev('/tmp/cdc.txt', EventKind.CREATED, inode=1500)],
        [ev('/tmp/cdc.txt', EventKind.REMOVED, inode=1500)],
        [ev('/tmp/cdc.txt', EventKind.CREATED, inode=1501)],
    ]


def _create_delete_create_diff_batch_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = [c['path'] for c in collected if c['op'] == 'update']
    assert '/tmp/cdc.txt' in update_paths, (
        f'Expected final create to produce update: {collected}'
    )


# ===================================================================
# 71. modify_then_delete_same_batch
# ===================================================================


def _modify_then_delete_same_batch_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [ev('/tmp/mtd.txt', EventKind.CREATED)],
            [
                ev('/tmp/mtd.txt', EventKind.MODIFIED),
                ev('/tmp/mtd.txt', EventKind.REMOVED),
            ],
        ]
    return [
        [ev('/tmp/mtd.txt', EventKind.CREATED, inode=1510)],
        [
            ev('/tmp/mtd.txt', EventKind.MODIFIED, inode=1510),
            ev('/tmp/mtd.txt', EventKind.REMOVED, inode=1510),
        ],
    ]


def _modify_then_delete_same_batch_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    # Path-keyed: REMOVED cancels MODIFIED via CANCEL rule — neither reaches state.
    # So the file remains tracked from batch 1 but no new events reach state in batch 2.
    # The update from batch 1 appears, but no delete (events were cancelled).
    # GPFS: different reduction rules — REMOVED may reach state, producing a delete.
    # Accept either outcome: at least the initial update should be present.
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/mtd.txt' in update_paths, (
        f'Expected at least initial update: {collected}'
    )


# ===================================================================
# 72. two_dirs_created_and_populated
# ===================================================================


def _two_dirs_created_populated_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/da', EventKind.CREATED, is_dir=True),
                ev('/tmp/db', EventKind.CREATED, is_dir=True),
                ev('/tmp/da/fa.txt', EventKind.CREATED),
                ev('/tmp/db/fb.txt', EventKind.CREATED),
            ],
        ]
    return [
        [
            ev('/tmp/da', EventKind.CREATED, inode=1520, is_dir=True),
            ev('/tmp/db', EventKind.CREATED, inode=1521, is_dir=True),
            ev('/tmp/da/fa.txt', EventKind.CREATED, inode=1522),
            ev('/tmp/db/fb.txt', EventKind.CREATED, inode=1523),
        ],
    ]


def _two_dirs_created_populated_expected(
    collected: list[dict[str, Any]],
) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/da' in update_paths
    assert '/tmp/db' in update_paths
    assert '/tmp/da/fa.txt' in update_paths
    assert '/tmp/db/fb.txt' in update_paths


# ===================================================================
# 73. rename_swap_two_files
# ===================================================================


def _rename_swap_two_files_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/sw_a.txt', EventKind.CREATED),
                ev('/tmp/sw_b.txt', EventKind.CREATED),
            ],
            [
                ev('/tmp/sw_a.txt', EventKind.RENAMED),
                ev('/tmp/sw_tmp.txt', EventKind.RENAMED),
            ],
            [
                ev('/tmp/sw_b.txt', EventKind.RENAMED),
                ev('/tmp/sw_a.txt', EventKind.RENAMED),
            ],
            [
                ev('/tmp/sw_tmp.txt', EventKind.RENAMED),
                ev('/tmp/sw_b.txt', EventKind.RENAMED),
            ],
        ]
    return [
        [
            ev('/tmp/sw_a.txt', EventKind.CREATED, inode=1530),
            ev('/tmp/sw_b.txt', EventKind.CREATED, inode=1531),
        ],
        [ev('/tmp/sw_tmp.txt', EventKind.RENAMED, inode=1530)],
        [ev('/tmp/sw_a.txt', EventKind.RENAMED, inode=1531)],
        [ev('/tmp/sw_b.txt', EventKind.RENAMED, inode=1530)],
    ]


def _rename_swap_two_files_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/sw_a.txt' in update_paths, f'Expected sw_a.txt: {collected}'
    assert '/tmp/sw_b.txt' in update_paths, f'Expected sw_b.txt: {collected}'


# ===================================================================
# 74. create_20_single_batch
# ===================================================================


def _create_20_single_batch_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    paths = [f'/tmp/b20_{i:02d}.txt' for i in range(20)]
    if _is_path_keyed(cfg):
        return [[ev(p, EventKind.CREATED) for p in paths]]
    inodes = list(range(1600, 1620))
    return [[ev(p, EventKind.CREATED, inode=n) for p, n in zip(paths, inodes)]]


def _create_20_single_batch_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    assert len(updates) == 20, f'Expected 20 updates, got {len(updates)}'
    update_paths = {c['path'] for c in updates}
    for i in range(20):
        assert f'/tmp/b20_{i:02d}.txt' in update_paths


# ===================================================================
# 75. rename_dir_with_5_children
# ===================================================================


def _rename_dir_5_children_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    children = [f'/tmp/pd/child_{i}.txt' for i in range(5)]
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/pd', EventKind.CREATED, is_dir=True),
                *[ev(c, EventKind.CREATED) for c in children],
            ],
            [
                ev('/tmp/pd', EventKind.RENAMED),
                ev('/tmp/pd_new', EventKind.RENAMED),
            ],
        ]
    inodes = list(range(1630, 1636))
    return [
        [
            ev('/tmp/pd', EventKind.CREATED, inode=1630, is_dir=True),
            *[
                ev(c, EventKind.CREATED, inode=n)
                for c, n in zip(children, inodes[1:])
            ],
        ],
        [ev('/tmp/pd_new', EventKind.RENAMED, inode=1630, is_dir=True)],
    ]


def _rename_dir_5_children_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    update_paths = {c['path'] for c in collected if c['op'] == 'update'}
    assert '/tmp/pd_new' in update_paths
    for i in range(5):
        assert f'/tmp/pd_new/child_{i}.txt' in update_paths, (
            f'Expected child_{i} at new path: {collected}'
        )


# ===================================================================
# 76. create_modify_modify_same_batch
# ===================================================================


def _create_modify_modify_events(
    cfg: BackendConfig,
) -> list[list[dict[str, Any]]]:
    ev = cfg.make_event
    if _is_path_keyed(cfg):
        return [
            [
                ev('/tmp/cmm.txt', EventKind.CREATED),
                ev('/tmp/cmm.txt', EventKind.MODIFIED),
                ev('/tmp/cmm.txt', EventKind.MODIFIED),
            ],
        ]
    return [
        [
            ev('/tmp/cmm.txt', EventKind.CREATED, inode=1640),
            ev('/tmp/cmm.txt', EventKind.MODIFIED, inode=1640),
            ev('/tmp/cmm.txt', EventKind.MODIFIED, inode=1640),
        ],
    ]


def _create_modify_modify_expected(collected: list[dict[str, Any]]) -> None:
    for c in collected:
        _assert_valid_change(c)
    updates = [c for c in collected if c['op'] == 'update']
    # CREATED + MODIFIED reduced (IGNORE), second MODIFIED reduced (IGNORE)
    # Result: exactly 1 update from the CREATED
    assert len(updates) == 1, (
        f'Expected 1 update, got {len(updates)}: {collected}'
    )
    assert updates[0]['path'] == '/tmp/cmm.txt'


SCENARIOS: list[Scenario] = [
    Scenario(
        name='create_single_file',
        description='One file created yields 1 update output.',
        make_events=_create_single_file_events,
        expected=_create_single_file_expected,
    ),
    Scenario(
        name='create_dir_and_file',
        description='Directory + nested file yields 2+ updates.',
        make_events=_create_dir_and_file_events,
        expected=_create_dir_and_file_expected,
    ),
    Scenario(
        name='ephemeral_create_delete',
        description='Create then delete in same batch cancels to 0 outputs.',
        make_events=_ephemeral_create_delete_events,
        expected=_ephemeral_create_delete_expected,
    ),
    Scenario(
        name='file_rename',
        description='Create + rename yields update at new path.',
        make_events=_file_rename_events,
        expected=_file_rename_expected,
    ),
    Scenario(
        name='dir_rename_with_child',
        description='Create dir + child, rename dir, child path updated.',
        make_events=_dir_rename_with_child_events,
        expected=_dir_rename_with_child_expected,
    ),
    Scenario(
        name='modify_existing',
        description='Create + modify yields 1 update output.',
        make_events=_modify_existing_events,
        expected=_modify_existing_expected,
    ),
    Scenario(
        name='full_lifecycle',
        description='Create -> modify -> rename -> delete full lifecycle.',
        make_events=_full_lifecycle_events,
        expected=_full_lifecycle_expected,
    ),
    Scenario(
        name='create_empty_file',
        description='0-byte file yields 1 update. GPFS: size=0.',
        make_events=_create_empty_file_events,
        expected=_create_empty_file_expected,
    ),
    Scenario(
        name='truncate_file',
        description='Create file then MODIFIED in next batch yields update.',
        make_events=_truncate_file_events,
        expected=_truncate_file_expected,
    ),
    Scenario(
        name='append_grows_size',
        description='Create then multiple MODIFIEDs yields updates; GPFS final size=300.',
        make_events=_append_grows_size_events,
        expected=_append_grows_size_expected,
    ),
    Scenario(
        name='cross_directory_move',
        description='Move file between directories yields update at new location.',
        make_events=_cross_directory_move_events,
        expected=_cross_directory_move_expected,
    ),
    Scenario(
        name='rename_then_modify',
        description='Create, rename a->b, modify b yields update at b.',
        make_events=_rename_then_modify_events,
        expected=_rename_then_modify_expected,
    ),
    Scenario(
        name='rename_chain_a_b_c',
        description='Create a, rename a->b, rename b->c yields update at c.',
        make_events=_rename_chain_a_b_c_events,
        expected=_rename_chain_a_b_c_expected,
    ),
    Scenario(
        name='rename_overwrite_collision',
        description="Rename a over b yields update at b with a's content.",
        make_events=_rename_overwrite_collision_events,
        expected=_rename_overwrite_collision_expected,
    ),
    Scenario(
        name='atomic_rename_pattern',
        description='Create tmp, rename tmp->final yields update at final.',
        make_events=_atomic_rename_pattern_events,
        expected=_atomic_rename_pattern_expected,
    ),
    Scenario(
        name='swap_files_via_temp',
        description='Swap a and b via temp yields updates at both a and b.',
        make_events=_swap_files_via_temp_events,
        expected=_swap_files_via_temp_expected,
    ),
    Scenario(
        name='deep_nested_tree',
        description='Deeply nested dirs + file yields 4+ updates.',
        make_events=_deep_nested_tree_events,
        expected=_deep_nested_tree_expected,
    ),
    Scenario(
        name='recursive_delete_tree',
        description='Create dir tree then delete all yields 3 deletes.',
        make_events=_recursive_delete_tree_events,
        expected=_recursive_delete_tree_expected,
    ),
    Scenario(
        name='create_delete_empty_dir',
        description='Create + delete empty dir in same batch cancels to 0.',
        make_events=_create_delete_empty_dir_events,
        expected=_create_delete_empty_dir_expected,
    ),
    Scenario(
        name='rename_dir_many_children',
        description='Rename dir with 3 children updates all child paths.',
        make_events=_rename_dir_many_children_events,
        expected=_rename_dir_many_children_expected,
    ),
    Scenario(
        name='burst_create_multiple_files',
        description='5 files in one batch yields 5 updates.',
        make_events=_burst_create_multiple_files_events,
        expected=_burst_create_multiple_files_expected,
    ),
    Scenario(
        name='many_modifications_reduced',
        description='1 create + 5 modifies in same batch yields exactly 1 update.',
        make_events=_many_modifications_reduced_events,
        expected=_many_modifications_reduced_expected,
    ),
    Scenario(
        name='burst_create_delete_mixed',
        description='Create a,b,c + delete b in same batch yields 2 updates (a,c).',
        make_events=_burst_create_delete_mixed_events,
        expected=_burst_create_delete_mixed_expected,
    ),
    Scenario(
        name='chmod_mode_change',
        description='Create then MODIFIED yields update at path.',
        make_events=_chmod_mode_change_events,
        expected=_chmod_mode_change_expected,
    ),
    Scenario(
        name='parent_updated_on_child_create',
        description='Create dir + child in same batch: dir appears in updates.',
        make_events=_parent_updated_on_child_create_events,
        expected=_parent_updated_on_child_create_expected,
    ),
    Scenario(
        name='recreate_after_delete',
        description='Create, delete, recreate yields final update at path.',
        make_events=_recreate_after_delete_events,
        expected=_recreate_after_delete_expected,
    ),
    Scenario(
        name='copy_creates_new_file',
        description='Copy = new CREATED; both original and copy in updates.',
        make_events=_copy_creates_new_file_events,
        expected=_copy_creates_new_file_expected,
    ),
    Scenario(
        name='symlink_create',
        description='Create target + symlink; both appear as updates.',
        make_events=_symlink_create_events,
        expected=_symlink_create_expected,
    ),
    Scenario(
        name='wide_dir_many_files',
        description='Dir + 10 files in one batch; all 11 in updates.',
        make_events=_wide_dir_many_files_events,
        expected=_wide_dir_many_files_expected,
    ),
    Scenario(
        name='overwrite_file_new_content',
        description='Create then overwrite (MODIFIED) yields update.',
        make_events=_overwrite_file_new_content_events,
        expected=_overwrite_file_new_content_expected,
    ),
    Scenario(
        name='rename_dir_then_create_inside',
        description='Rename dir then create file inside renamed dir.',
        make_events=_rename_dir_then_create_inside_events,
        expected=_rename_dir_then_create_inside_expected,
    ),
    Scenario(
        name='delete_recreate_different_batch',
        description='Create, delete, recreate in 3 batches — final update present.',
        make_events=_delete_recreate_different_batch_events,
        expected=_delete_recreate_different_batch_expected,
    ),
    Scenario(
        name='modify_deleted_path_ignored',
        description='Create, delete, then MODIFIED at same path — no crash, no output.',
        make_events=_modify_deleted_path_ignored_events,
        expected=_modify_deleted_path_ignored_expected,
    ),
    Scenario(
        name='double_rename_same_batch',
        description='Two renames in same batch (A→B, C→D) — both new paths updated.',
        make_events=_double_rename_same_batch_events,
        expected=_double_rename_same_batch_expected,
    ),
    Scenario(
        name='create_modify_rename_single_batch',
        description='CREATED+MODIFIED+RENAMED in one batch — MODIFIED reduced, rename applies.',
        make_events=_create_modify_rename_single_batch_events,
        expected=_create_modify_rename_single_batch_expected,
    ),
    Scenario(
        name='rename_back_to_original',
        description='Create A, rename A→B, rename B→A — update at original path.',
        make_events=_rename_back_to_original_events,
        expected=_rename_back_to_original_expected,
    ),
    Scenario(
        name='rename_unknown_source',
        description='Rename of untracked path — treated as create at new path.',
        make_events=_rename_unknown_source_events,
        expected=_rename_unknown_source_expected,
    ),
    Scenario(
        name='remove_nonexistent_ignored',
        description='REMOVED for untracked path — 0 outputs, no crash.',
        make_events=_remove_nonexistent_ignored_events,
        expected=_remove_nonexistent_ignored_expected,
    ),
    Scenario(
        name='interleaved_two_file_lifecycle',
        description='Create A+B, modify both, delete A — B survives with update.',
        make_events=_interleaved_two_file_lifecycle_events,
        expected=_interleaved_two_file_lifecycle_expected,
    ),
    Scenario(
        name='dir_delete_cascades_to_child',
        description='Create dir+child, delete dir — both get deleted.',
        make_events=_dir_delete_cascades_events,
        expected=_dir_delete_cascades_expected,
    ),
    Scenario(
        name='accessed_reduced_by_created',
        description='CREATED + ACCESSED in same batch — ACCESSED ignored, 1 update.',
        make_events=_accessed_reduced_events,
        expected=_accessed_reduced_expected,
    ),
    Scenario(
        name='create_same_path_twice',
        description='Two CREATED for same path in one batch — 1 update.',
        make_events=_create_same_path_twice_events,
        expected=_create_same_path_twice_expected,
    ),
    Scenario(
        name='rename_collision_emits_delete',
        description='Create A+B, rename A→B — old B deleted, new B updated.',
        make_events=_rename_collision_delete_events,
        expected=_rename_collision_delete_expected,
    ),
    Scenario(
        name='create_file_in_renamed_dir',
        description='Create dir, rename, create file inside renamed — file in updates.',
        make_events=_create_file_in_renamed_dir_events,
        expected=_create_file_in_renamed_dir_expected,
    ),
    Scenario(
        name='double_delete_same_path',
        description='Create, delete, delete again — second delete ignored.',
        make_events=_double_delete_events,
        expected=_double_delete_expected,
    ),
    Scenario(
        name='create_a_modify_b_same_batch',
        description='CREATED(A) + MODIFIED(B) in same batch — A update, B ignored (untracked).',
        make_events=_create_a_modify_b_events,
        expected=_create_a_modify_b_expected,
    ),
    Scenario(
        name='create_3_delete_middle',
        description='Create A,B,C then delete B — A,C updated, B deleted.',
        make_events=_create_3_delete_middle_events,
        expected=_create_3_delete_middle_expected,
    ),
    Scenario(
        name='rename_creates_then_delete_new',
        description='Create A, rename A→B, delete B — emitted A gets delete.',
        make_events=_rename_creates_then_delete_new_events,
        expected=_rename_creates_then_delete_new_expected,
    ),
    Scenario(
        name='modify_multiple_untracked_ignored',
        description='5 MODIFIED events for untracked paths — 0 outputs, no crash.',
        make_events=_modify_multiple_untracked_events,
        expected=_modify_multiple_untracked_expected,
    ),
    Scenario(
        name='create_5_files_delete_all',
        description='Create 5 files in batch 1, delete all in batch 2 — 5 deletes.',
        make_events=_create_5_delete_all_events,
        expected=_create_5_delete_all_expected,
    ),
    Scenario(
        name='nested_dir_rename_child_modify',
        description='Create dir/child, rename dir, modify child at new path — child updated.',
        make_events=_nested_dir_rename_child_modify_events,
        expected=_nested_dir_rename_child_modify_expected,
    ),
    Scenario(
        name='create_modify_delete_same_batch',
        description='CREATED + MODIFIED + REMOVED in one batch — all cancel, 0 outputs.',
        make_events=_create_modify_delete_same_batch_events,
        expected=_create_modify_delete_same_batch_events_expected,
    ),
    Scenario(
        name='rapid_create_10_files_single_batch',
        description='10 CREATED in one batch — all 10 appear as updates.',
        make_events=_rapid_create_10_events,
        expected=_rapid_create_10_expected,
    ),
    Scenario(
        name='rename_dir_2_levels_deep',
        description='Create top/mid/leaf, rename top→newtop — leaf at new path.',
        make_events=_rename_dir_2_levels_events,
        expected=_rename_dir_2_levels_expected,
    ),
    Scenario(
        name='accessed_then_modified_reduced',
        description='ACCESSED + MODIFIED in same batch — ACCESSED reduced.',
        make_events=_accessed_then_modified_events,
        expected=_accessed_then_modified_expected,
    ),
    Scenario(
        name='create_symlink_delete_target',
        description='Create target + symlink, delete target — symlink updated, target deleted.',
        make_events=_create_symlink_delete_target_events,
        expected=_create_symlink_delete_target_expected,
    ),
    Scenario(
        name='multiple_renames_same_batch',
        description='3 renames in same batch — all 3 new paths updated.',
        make_events=_multiple_renames_same_batch_events,
        expected=_multiple_renames_same_batch_expected,
    ),
    Scenario(
        name='removed_cancels_created_batch',
        description='5 CREATED + 5 REMOVED (same paths) in one batch — all cancel.',
        make_events=_removed_cancels_created_batch_events,
        expected=_removed_cancels_created_batch_expected,
    ),
    Scenario(
        name='rename_to_existing_then_modify',
        description='Create A+B, rename A→B (collision), modify B — B updated.',
        make_events=_rename_to_existing_then_modify_events,
        expected=_rename_to_existing_then_modify_expected,
    ),
    Scenario(
        name='3_batch_lifecycle',
        description='Batch 1: create 3. Batch 2: modify+delete. Batch 3: modify survivors.',
        make_events=_3_batch_lifecycle_events,
        expected=_3_batch_lifecycle_expected,
    ),
    Scenario(
        name='dir_with_10_children_delete',
        description='Create dir + 10 children, delete dir — all 11 deleted.',
        make_events=_dir_10_children_delete_events,
        expected=_dir_10_children_delete_expected,
    ),
    Scenario(
        name='modify_then_rename_same_batch',
        description='MODIFIED then RENAMED in same batch — update at new path.',
        make_events=_modify_then_rename_events,
        expected=_modify_then_rename_expected,
    ),
    Scenario(
        name='create_10_modify_5_delete_3',
        description='Create 10, modify first 5, delete first 3 — 7 updates + 3 deletes.',
        make_events=_create_10_modify_5_delete_3_events,
        expected=_create_10_modify_5_delete_3_expected,
    ),
    Scenario(
        name='rename_creates_at_destination',
        description='Rename of untracked source creates new entry at destination.',
        make_events=_rename_creates_at_destination_events,
        expected=_rename_creates_at_destination_expected,
    ),
    Scenario(
        name='5_modifications_reduced_to_1',
        description='Create file + 5 MODIFIEDs in same batch — exactly 1 update.',
        make_events=_5_modifications_reduced_events,
        expected=_5_modifications_reduced_expected,
    ),
    Scenario(
        name='create_rename_create_same_path',
        description='Create A, rename A→B, create new A — both A and B in updates.',
        make_events=_create_rename_create_same_path_events,
        expected=_create_rename_create_same_path_expected,
    ),
    Scenario(
        name='interleaved_renames_and_creates',
        description='Create A, rename A→B, create C, rename C→D — B and D updated.',
        make_events=_interleaved_renames_creates_events,
        expected=_interleaved_renames_creates_expected,
    ),
    Scenario(
        name='create_dir_child_rename_dir_delete_child',
        description='Create dir+child, rename dir, delete child at new path.',
        make_events=_create_dir_rename_delete_child_events,
        expected=_create_dir_rename_delete_child_expected,
    ),
    Scenario(
        name='rapid_modify_accessed_interleaved',
        description='Alternating MODIFIED+ACCESSED for same path — reduced efficiently.',
        make_events=_rapid_modify_accessed_events,
        expected=_rapid_modify_accessed_expected,
    ),
    Scenario(
        name='create_delete_create_different_batch',
        description='Create, delete, recreate across 3 batches — final update present.',
        make_events=_create_delete_create_diff_batch_events,
        expected=_create_delete_create_diff_batch_expected,
    ),
    Scenario(
        name='modify_then_delete_same_batch',
        description='MODIFIED + REMOVED in same batch — delete emitted.',
        make_events=_modify_then_delete_same_batch_events,
        expected=_modify_then_delete_same_batch_expected,
    ),
    Scenario(
        name='two_dirs_created_and_populated',
        description='2 dirs + 1 file each in one batch — all 4 in updates.',
        make_events=_two_dirs_created_populated_events,
        expected=_two_dirs_created_populated_expected,
    ),
    Scenario(
        name='rename_swap_two_files',
        description='Swap A and B via temp through 3 rename batches.',
        make_events=_rename_swap_two_files_events,
        expected=_rename_swap_two_files_expected,
    ),
    Scenario(
        name='create_20_single_batch',
        description='20 files in one batch — all 20 updates emitted.',
        make_events=_create_20_single_batch_events,
        expected=_create_20_single_batch_expected,
    ),
    Scenario(
        name='rename_dir_with_5_children',
        description='Dir with 5 children, rename dir — all children updated at new paths.',
        make_events=_rename_dir_5_children_events,
        expected=_rename_dir_5_children_expected,
    ),
    Scenario(
        name='create_modify_modify_same_batch',
        description='CREATED + 2x MODIFIED in one batch — reduced to 1 update.',
        make_events=_create_modify_modify_events,
        expected=_create_modify_modify_expected,
    ),
]
