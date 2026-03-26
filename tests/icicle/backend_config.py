"""Backend configurations for parameterized unified scenario testing.

Provides BackendConfig objects for fswatch, lustre, and gpfs backends,
each with the correct event factory, reduction rules, slot key, state
manager factory, and rename behaviour.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.icicle.batch import ReductionAction
from src.icicle.events import EventKind
from src.icicle.fswatch_events import STAT_REDUCTION_RULES
from src.icicle.gpfs_events import GPFS_REDUCTION_RULES
from src.icicle.gpfs_state import GPFSStateManager
from src.icicle.state import BaseStateManager
from src.icicle.state import PathStateManager

# ---------------------------------------------------------------------------
# Fake stat for PathStateManager (fswatch / lustre)
# ---------------------------------------------------------------------------


class _FakeStat:
    """Minimal os.stat_result stand-in for tests."""

    st_size: int = 100
    st_uid: int = 1000
    st_gid: int = 1000
    st_mode: int = 0o100644
    st_atime: float = 1000.0
    st_mtime: float = 2000.0
    st_ctime: float = 3000.0


def _fake_stat_fn(_path: str) -> os.stat_result:  # type: ignore[return-value]
    return _FakeStat()  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# BackendConfig dataclass
# ---------------------------------------------------------------------------


@dataclass
class BackendConfig:
    """Encapsulates everything needed to run a scenario against one backend."""

    name: str
    make_event: Callable[..., dict[str, Any]]
    reduction_rules: dict[EventKind, tuple[ReductionAction, set[EventKind]]]
    slot_key: str
    state_factory: Callable[[], BaseStateManager]
    bypass_rename: bool


# ---------------------------------------------------------------------------
# Event factories
# ---------------------------------------------------------------------------


def _path_event(
    path: str,
    kind: EventKind,
    **kw: Any,
) -> dict[str, Any]:
    """Create a path-keyed event (fswatch / lustre format)."""
    return {
        'path': path,
        'kind': kind,
        'ts': kw.get('ts', 0.0),
        'is_dir': kw.get('is_dir', False),
        'is_symlink': kw.get('is_symlink', False),
        **{
            k: v
            for k, v in kw.items()
            if k not in ('ts', 'is_dir', 'is_symlink')
        },
    }


class _GPFSEventFactory:
    """Callable that produces GPFS inode-keyed events.

    Auto-increments a default inode counter when *inode* is not supplied
    explicitly via keyword arguments.
    """

    def __init__(self) -> None:
        self._next_inode: int = 1

    def __call__(
        self,
        path: str,
        kind: EventKind,
        **kw: Any,
    ) -> dict[str, Any]:
        if 'inode' in kw:
            inode = kw.pop('inode')
        else:
            inode = self._next_inode
            self._next_inode += 1
        return {
            'inode': inode,
            'path': path,
            'kind': kind,
            'is_dir': kw.get('is_dir', False),
            'size': kw.get('size', 100),
            'atime': kw.get('atime', 0),
            'ctime': kw.get('ctime', 0),
            'mtime': kw.get('mtime', 0),
            'uid': kw.get('uid', 1000),
            'gid': kw.get('gid', 1000),
            'permissions': kw.get('permissions', '0o100644'),
        }


# ---------------------------------------------------------------------------
# Pre-built backend configs
# ---------------------------------------------------------------------------

FSWATCH_BACKEND = BackendConfig(
    name='fswatch',
    make_event=_path_event,
    reduction_rules=STAT_REDUCTION_RULES,
    slot_key='path',
    state_factory=lambda: PathStateManager(stat_fn=_fake_stat_fn),
    bypass_rename=True,
)

LUSTRE_BACKEND = BackendConfig(
    name='lustre',
    make_event=_path_event,
    reduction_rules=STAT_REDUCTION_RULES,
    slot_key='path',
    state_factory=lambda: PathStateManager(stat_fn=_fake_stat_fn),
    bypass_rename=True,
)

GPFS_BACKEND = BackendConfig(
    name='gpfs',
    make_event=_GPFSEventFactory(),
    reduction_rules=GPFS_REDUCTION_RULES,
    slot_key='inode',
    state_factory=GPFSStateManager,
    bypass_rename=False,
)

ALL_BACKENDS: list[BackendConfig] = [
    FSWATCH_BACKEND,
    LUSTRE_BACKEND,
    GPFS_BACKEND,
]
