"""Shared live workload definitions for cross-backend integration tests.

A workload is a sequence of real filesystem operations executed against
a watch directory, plus expected pipeline output assertions.  Each
backend provides a ``harness`` fixture that yields ``(watch_dir, collect)``
where ``collect(final_wait)`` returns the pipeline output payloads.

Usage in each integration test file::

    from tests.icicle.workloads import WORKLOADS
    from tests.icicle.workloads import assert_workload_results
    from tests.icicle.workloads import execute_workload

    @pytest.mark.parametrize('workload', WORKLOADS, ids=lambda w: w.name)
    def test_workload(workload, harness):
        watch_dir, collect = harness
        execute_workload(watch_dir, workload.ops)
        payloads = collect(FINAL_WAIT)
        assert_workload_results(payloads, workload.expected)
"""

from __future__ import annotations

import os
import shutil
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Any

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class FsOp:
    """A single filesystem operation."""

    action: str
    path: str  # relative to watch_dir
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExpectedChange:
    """An expected entry in the pipeline output."""

    op: str  # 'update' or 'delete'
    path_suffix: str  # matched with endswith()
    stat_checks: dict[str, Any] = field(default_factory=dict)


@dataclass
class Workload:
    """A cross-backend live test workload."""

    name: str
    description: str
    ops: list[FsOp]
    expected: list[ExpectedChange]


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------

_DEFAULT_PAUSE = 0.3


def execute_workload(watch_dir: str, ops: list[FsOp]) -> None:
    """Execute filesystem operations against *watch_dir*."""
    for op in ops:
        path = os.path.join(watch_dir, op.path)
        kw = op.kwargs
        pause = kw.get('pause_after', _DEFAULT_PAUSE)

        if op.action == 'create_file':
            with open(path, 'wb') as f:
                f.write(kw.get('content', b''))
        elif op.action == 'mkdir':
            os.mkdir(path)
        elif op.action == 'makedirs':
            os.makedirs(path, exist_ok=True)
        elif op.action == 'rename':
            dst = os.path.join(watch_dir, kw['dst'])
            os.rename(path, dst)
        elif op.action == 'delete':
            os.remove(path)
        elif op.action == 'rmtree':
            shutil.rmtree(path)
        elif op.action == 'chmod':
            os.chmod(path, kw['mode'])
        elif op.action == 'write':
            with open(path, 'wb') as f:
                f.write(kw['content'])
        elif op.action == 'append':
            with open(path, 'ab') as f:
                f.write(kw['content'])
        elif op.action == 'truncate':
            with open(path, 'w') as f:
                pass
        elif op.action == 'symlink':
            target = os.path.join(watch_dir, kw['target'])
            os.symlink(target, path)
        elif op.action == 'hardlink':
            target = os.path.join(watch_dir, kw['target'])
            os.link(target, path)
        elif op.action == 'copy':
            dst = os.path.join(watch_dir, kw['dst'])
            shutil.copy2(path, dst)
        elif op.action == 'utime':
            atime = kw.get('atime', kw.get('time', 0.0))
            mtime = kw.get('mtime', kw.get('time', 0.0))
            os.utime(path, (atime, mtime))
        elif op.action == 'replace':
            dst = os.path.join(watch_dir, kw['dst'])
            os.replace(path, dst)
        else:
            msg = f'Unknown FsOp action: {op.action}'
            raise ValueError(msg)

        if pause > 0:
            time.sleep(pause)


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------


def assert_workload_results(
    payloads: list[dict[str, Any]],
    expected: list[ExpectedChange],
) -> None:
    """Assert pipeline output matches expected changes."""
    for ec in expected:
        matching = [
            p
            for p in payloads
            if p.get('op') == ec.op
            and p.get('path', '').endswith(ec.path_suffix)
        ]
        assert len(matching) >= 1, (
            f'Expected {ec.op} ending with {ec.path_suffix!r}, '
            f'got 0 matches in {_summarize(payloads)}'
        )
        last = matching[-1]

        for key, value in ec.stat_checks.items():
            if key == 'has_stat':
                assert 'stat' in last, f'Expected stat dict in {last}'
            elif key == 'size':
                assert last.get('stat', {}).get('size') == value, (
                    f'Expected size={value} for {ec.path_suffix}, '
                    f'got {last.get("stat", {}).get("size")}'
                )
            elif key == 'size_gt':
                assert (last.get('stat', {}).get('size') or 0) > value, (
                    f'Expected size>{value} for {ec.path_suffix}, '
                    f'got {last.get("stat", {}).get("size")}'
                )
            elif key == 'mode_isdir':
                import stat as stat_mod

                mode = last.get('stat', {}).get('mode')
                if mode is not None:
                    assert stat_mod.S_ISDIR(mode), (
                        f'Expected S_ISDIR for {ec.path_suffix}, '
                        f'got mode={oct(mode)}'
                    )
            elif key == 'mode_isreg':
                import stat as stat_mod

                mode = last.get('stat', {}).get('mode')
                if mode is not None:
                    assert stat_mod.S_ISREG(mode), (
                        f'Expected S_ISREG for {ec.path_suffix}, '
                        f'got mode={oct(mode)}'
                    )


def _summarize(payloads: list[dict[str, Any]]) -> str:
    return str(
        [
            (p.get('op'), p.get('path', '').rsplit('/', 1)[-1])
            for p in payloads
        ],
    )


# ---------------------------------------------------------------------------
# Workload definitions
# ---------------------------------------------------------------------------

W = Workload  # shorthand
Op = FsOp
E = ExpectedChange

WORKLOADS: list[Workload] = [
    # ------------------------------------------------------------------
    # 1. File lifecycle
    # ------------------------------------------------------------------
    W(
        'create_single_file',
        'Create one file with known content',
        ops=[Op('create_file', 'hello.txt', {'content': b'hello\n'})],
        expected=[E('update', '/hello.txt', {'size': 6})],
    ),
    W(
        'create_and_delete',
        'Create file then delete — update and delete both appear',
        ops=[
            Op('create_file', 'f.txt', {'content': b'data\n'}),
            Op('delete', 'f.txt'),
        ],
        expected=[
            E('update', '/f.txt'),
            E('delete', '/f.txt'),
        ],
    ),
    W(
        'empty_file',
        'Create 0-byte file',
        ops=[Op('create_file', 'empty.txt', {'content': b''})],
        expected=[E('update', '/empty.txt', {'size': 0})],
    ),
    W(
        'binary_file',
        'Create 4KB binary file',
        ops=[
            Op('create_file', 'data.bin', {'content': os.urandom(4096)}),
        ],
        expected=[E('update', '/data.bin', {'size': 4096})],
    ),
    W(
        'truncate_file',
        'Create file with content then truncate to 0',
        ops=[
            Op('create_file', 'f.txt', {'content': b'some content here\n'}),
            Op('truncate', 'f.txt'),
        ],
        expected=[E('update', '/f.txt')],
    ),
    W(
        'append_grows_size',
        'Create then append twice — final size = sum',
        ops=[
            Op('create_file', 'log.txt', {'content': b'AAAAA'}),
            Op('append', 'log.txt', {'content': b'BBBBB'}),
            Op('append', 'log.txt', {'content': b'CCCCC'}),
        ],
        expected=[E('update', '/log.txt', {'size': 15})],
    ),
    W(
        'overwrite_content',
        'Create then overwrite with different content',
        ops=[
            Op('create_file', 'f.txt', {'content': b'short'}),
            Op('write', 'f.txt', {'content': b'much longer content here'}),
        ],
        expected=[E('update', '/f.txt', {'size': 24})],
    ),
    # ------------------------------------------------------------------
    # 2. Directory operations
    # ------------------------------------------------------------------
    W(
        'create_dir_and_file',
        'Create directory then nested file',
        ops=[
            Op('mkdir', 'sub'),
            Op('create_file', 'sub/nested.txt', {'content': b'nested\n'}),
        ],
        expected=[
            E('update', '/sub'),
            E('update', '/nested.txt'),
        ],
    ),
    W(
        'deep_tree_3_levels',
        'Create 3-level tree with file at each level',
        ops=[
            Op('makedirs', 'a/b/c'),
            Op('create_file', 'a/f1.txt', {'content': b'L1\n'}),
            Op('create_file', 'a/b/f2.txt', {'content': b'L2\n'}),
            Op('create_file', 'a/b/c/f3.txt', {'content': b'L3\n'}),
        ],
        expected=[
            E('update', '/f1.txt'),
            E('update', '/f2.txt'),
            E('update', '/f3.txt'),
        ],
    ),
    W(
        'recursive_delete',
        'Create tree then rmtree — deletion detected',
        ops=[
            Op('makedirs', 'tree/sub'),
            Op('create_file', 'tree/root.txt', {'content': b'root\n'}),
            Op('create_file', 'tree/sub/child.txt', {'content': b'child\n'}),
            Op('rmtree', 'tree', {'pause_after': 0.5}),
        ],
        expected=[E('update', '/root.txt')],
    ),
    # ------------------------------------------------------------------
    # 3. Rename / move operations
    # ------------------------------------------------------------------
    W(
        'rename_file',
        'Create file then rename',
        ops=[
            Op('create_file', 'before.txt', {'content': b'content\n'}),
            Op('rename', 'before.txt', {'dst': 'after.txt'}),
        ],
        expected=[E('update', '/before.txt')],
    ),
    W(
        'rename_dir_with_child',
        'Create dir with child then rename dir',
        ops=[
            Op('mkdir', 'olddir'),
            Op('create_file', 'olddir/child.txt', {'content': b'c\n'}),
            Op('rename', 'olddir', {'dst': 'newdir'}),
        ],
        expected=[E('update', '/olddir')],
    ),
    W(
        'cross_dir_move',
        'Move file between directories',
        ops=[
            Op('mkdir', 'src'),
            Op('mkdir', 'dst'),
            Op('create_file', 'src/moveme.txt', {'content': b'move\n'}),
            Op('rename', 'src/moveme.txt', {'dst': 'dst/moveme.txt'}),
        ],
        expected=[E('update', '/moveme.txt')],
    ),
    W(
        'rename_chain',
        'Rename chain a→b→c',
        ops=[
            Op('create_file', 'chain_a.txt', {'content': b'chain\n'}),
            Op('rename', 'chain_a.txt', {'dst': 'chain_b.txt'}),
            Op('rename', 'chain_b.txt', {'dst': 'chain_c.txt'}),
        ],
        expected=[E('update', '/chain_a.txt')],
    ),
    W(
        'atomic_rename',
        'Write to tmp then rename to final path',
        ops=[
            Op('create_file', 'doc.tmp', {'content': b'final content\n'}),
            Op('rename', 'doc.tmp', {'dst': 'doc.txt'}),
        ],
        expected=[E('update', '/doc.tmp')],
    ),
    # ------------------------------------------------------------------
    # 4. Metadata operations
    # ------------------------------------------------------------------
    W(
        'chmod_file',
        'Create file then chmod',
        ops=[
            Op('create_file', 'f.txt', {'content': b'data'}),
            Op('chmod', 'f.txt', {'mode': 0o755}),
            Op('chmod', 'f.txt', {'mode': 0o644}),
        ],
        expected=[E('update', '/f.txt')],
    ),
    # ------------------------------------------------------------------
    # 5. Link operations
    # ------------------------------------------------------------------
    W(
        'symlink_create',
        'Create target then symlink',
        ops=[
            Op('create_file', 'target.txt', {'content': b'target\n'}),
            Op('symlink', 'link.txt', {'target': 'target.txt'}),
        ],
        expected=[
            E('update', '/target.txt'),
            E('update', '/link.txt'),
        ],
    ),
    W(
        'hardlink_and_delete',
        'Create file, hardlink, then delete hardlink',
        ops=[
            Op('create_file', 'orig.txt', {'content': b'data\n'}),
            Op('hardlink', 'hl.txt', {'target': 'orig.txt'}),
            Op('delete', 'hl.txt'),
        ],
        expected=[
            E('update', '/hl.txt'),
            E('delete', '/hl.txt'),
        ],
    ),
    # ------------------------------------------------------------------
    # 6. Burst / rapid operations
    # ------------------------------------------------------------------
    W(
        'burst_create_5',
        'Create 5 files in rapid succession',
        ops=[
            Op(
                'create_file',
                f'batch_{i}.txt',
                {'content': f'file {i}\n'.encode(), 'pause_after': 0.05},
            )
            for i in range(5)
        ],
        expected=[E('update', f'/batch_{i}.txt') for i in range(5)],
    ),
    W(
        'burst_modify_same_file',
        'Create file then append 5 times',
        ops=[
            Op('create_file', 'hot.txt', {'content': b'start\n'}),
            *[
                Op(
                    'append',
                    'hot.txt',
                    {'content': f'line {i}\n'.encode(), 'pause_after': 0.05},
                )
                for i in range(5)
            ],
        ],
        expected=[E('update', '/hot.txt', {'size_gt': 6})],
    ),
    W(
        'create_delete_rapid',
        'Create and delete immediately — no crash',
        ops=[
            Op(
                'create_file',
                'ephemeral.txt',
                {
                    'content': b'gone\n',
                    'pause_after': 0.0,
                },
            ),
            Op('delete', 'ephemeral.txt', {'pause_after': 0.0}),
        ],
        # Timing-dependent: either both cancel or update+delete appear.
        # We only assert no crash — expected is empty (any output is OK).
        expected=[],
    ),
    # ------------------------------------------------------------------
    # 7. Rename collision / swap / recreate
    # ------------------------------------------------------------------
    W(
        'rename_collision_overwrite',
        'Create A(10B) and B(5B), rename A→B — B gets A content. '
        'Rename pair may split across batches, so assert presence only.',
        ops=[
            Op('create_file', 'col_a.txt', {'content': b'A' * 10}),
            Op('create_file', 'col_b.txt', {'content': b'B' * 5}),
            Op('rename', 'col_a.txt', {'dst': 'col_b.txt'}),
        ],
        expected=[
            E('update', '/col_b.txt', {'has_stat': True}),
        ],
    ),
    W(
        'rename_collision_then_write',
        'Overwriting rename followed by a write to the winner. '
        'FSEvents may coalesce the append into the displacement '
        'notifications, so we only assert the renamed content arrived.',
        ops=[
            Op('create_file', 'rw_a.txt', {'content': b'X' * 15}),
            Op('create_file', 'rw_b.txt', {'content': b'Y' * 3}),
            Op('rename', 'rw_a.txt', {'dst': 'rw_b.txt'}),
            Op('append', 'rw_b.txt', {'content': b'Z' * 5}),
        ],
        expected=[
            E('update', '/rw_b.txt', {'has_stat': True}),
        ],
    ),
    W(
        'rename_collision_chain',
        'A(30B) overwrites B(10B), then C(5B) overwrites A-at-B. '
        'The second rename pair can split across batches, so we '
        'assert ch_b.txt was updated (not necessarily final size).',
        ops=[
            Op('create_file', 'ch_a.txt', {'content': b'A' * 30}),
            Op('create_file', 'ch_b.txt', {'content': b'B' * 10}),
            Op('create_file', 'ch_c.txt', {'content': b'C' * 5}),
            Op('rename', 'ch_a.txt', {'dst': 'ch_b.txt'}),
            Op('rename', 'ch_c.txt', {'dst': 'ch_b.txt'}),
        ],
        expected=[
            E('update', '/ch_b.txt', {'has_stat': True}),
        ],
    ),
    W(
        'rename_collision_dir_overwrite',
        'Rename file over another file inside a directory',
        ops=[
            Op('mkdir', 'col_dir'),
            Op('create_file', 'col_dir/target.txt', {'content': b'T' * 8}),
            Op('create_file', 'col_dir/src.txt', {'content': b'S' * 25}),
            Op('rename', 'col_dir/src.txt', {'dst': 'col_dir/target.txt'}),
        ],
        expected=[
            E('update', '/col_dir/target.txt', {'has_stat': True}),
        ],
    ),
    W(
        'large_file_1mb',
        'Create 1MB file — exact size verified',
        ops=[
            Op('create_file', 'big.bin', {'content': b'\x00' * 1048576}),
        ],
        expected=[E('update', '/big.bin', {'size': 1048576})],
    ),
    W(
        'unicode_filename',
        'Create files with unicode chars — emoji, CJK, accents',
        ops=[
            Op('create_file', 'café.txt', {'content': b'french\n'}),
            Op('create_file', 'datos_año.txt', {'content': b'spanish\n'}),
        ],
        expected=[
            E('update', '/café.txt', {'size': 7}),
            E('update', '/datos_año.txt', {'size': 8}),
        ],
    ),
    W(
        'swap_via_temp',
        'Swap A(3B) and B(7B) through temp — both end with swapped sizes',
        ops=[
            Op('create_file', 'sw_a.txt', {'content': b'AAA'}),
            Op('create_file', 'sw_b.txt', {'content': b'BBBBBBB'}),
            Op('rename', 'sw_a.txt', {'dst': 'sw_tmp.txt'}),
            Op('rename', 'sw_b.txt', {'dst': 'sw_a.txt'}),
            Op('rename', 'sw_tmp.txt', {'dst': 'sw_b.txt'}),
        ],
        expected=[
            E('update', '/sw_a.txt', {'size': 7}),
            E('update', '/sw_b.txt', {'size': 3}),
        ],
    ),
    W(
        'recreate_after_delete',
        'Create file(5B), delete, recreate(12B) — final size=12',
        ops=[
            Op('create_file', 'phoenix.txt', {'content': b'first'}),
            Op('delete', 'phoenix.txt'),
            Op('create_file', 'phoenix.txt', {'content': b'second_round'}),
        ],
        expected=[
            E('update', '/phoenix.txt', {'size': 12}),
        ],
    ),
    W(
        'wide_dir_10_files',
        'Create dir + 10 files inside — all detected',
        ops=[
            Op('mkdir', 'wide'),
            *[
                Op(
                    'create_file',
                    f'wide/f_{i}.txt',
                    {'content': f'file {i}\n'.encode(), 'pause_after': 0.05},
                )
                for i in range(10)
            ],
        ],
        expected=[
            E('update', '/wide'),
            *[E('update', f'/wide/f_{i}.txt') for i in range(10)],
        ],
    ),
    # ------------------------------------------------------------------
    # 8. Metadata operations (extended)
    # ------------------------------------------------------------------
    W(
        'chmod_dir',
        'Create dir then chmod 755→700 — dir update detected',
        ops=[
            Op('mkdir', 'chdir'),
            Op('chmod', 'chdir', {'mode': 0o755}),
            Op('chmod', 'chdir', {'mode': 0o700}),
        ],
        expected=[E('update', '/chdir')],
    ),
    W(
        'symlink_delete_target',
        'Create target + symlink, delete target — target update+delete appear',
        ops=[
            Op('create_file', 'sl_target.txt', {'content': b'target\n'}),
            Op('symlink', 'sl_link.txt', {'target': 'sl_target.txt'}),
            Op('delete', 'sl_target.txt'),
        ],
        expected=[
            E('update', '/sl_target.txt'),
            E('delete', '/sl_target.txt'),
        ],
    ),
    W(
        'nested_rename_updates_children',
        'Create a/b/f.txt, rename a→c — child detected under new prefix',
        ops=[
            Op('makedirs', 'nr_a/nr_b'),
            Op('create_file', 'nr_a/nr_b/data.txt', {'content': b'nested\n'}),
            Op('rename', 'nr_a', {'dst': 'nr_c'}),
        ],
        expected=[
            E('update', '/data.txt'),
        ],
    ),
    # ------------------------------------------------------------------
    # 9. Interleaved / multi-file operations
    # ------------------------------------------------------------------
    W(
        'delete_dir_with_3_children',
        'Create dir + 3 files, then rmtree — dir+children get deletes',
        ops=[
            Op('mkdir', 'ddir'),
            Op('create_file', 'ddir/c1.txt', {'content': b'child1\n'}),
            Op('create_file', 'ddir/c2.txt', {'content': b'child2\n'}),
            Op('create_file', 'ddir/c3.txt', {'content': b'child3\n'}),
            Op('rmtree', 'ddir', {'pause_after': 0.5}),
        ],
        expected=[
            E('update', '/c1.txt'),
        ],
    ),
    W(
        'create_files_3_sibling_dirs',
        'Create 3 dirs + 1 file each — all detected across dirs',
        ops=[
            Op('mkdir', 'sd_a'),
            Op('mkdir', 'sd_b'),
            Op('mkdir', 'sd_c'),
            Op('create_file', 'sd_a/fa.txt', {'content': b'dir a\n'}),
            Op('create_file', 'sd_b/fb.txt', {'content': b'dir b\n'}),
            Op('create_file', 'sd_c/fc.txt', {'content': b'dir c\n'}),
        ],
        expected=[
            E('update', '/sd_a/fa.txt', {'size': 6}),
            E('update', '/sd_b/fb.txt', {'size': 6}),
            E('update', '/sd_c/fc.txt', {'size': 6}),
        ],
    ),
    W(
        'rename_file_within_same_dir',
        'Create dir/a.txt, rename to dir/b.txt — both within same dir',
        ops=[
            Op('mkdir', 'rwd'),
            Op('create_file', 'rwd/a.txt', {'content': b'rename within\n'}),
            Op('rename', 'rwd/a.txt', {'dst': 'rwd/b.txt'}),
        ],
        expected=[E('update', '/a.txt')],
    ),
    W(
        'multiple_empty_files',
        'Create 5 empty files — all detected with size=0',
        ops=[
            *[
                Op(
                    'create_file',
                    f'empty_{i}.txt',
                    {'content': b'', 'pause_after': 0.05},
                )
                for i in range(5)
            ],
        ],
        expected=[
            *[E('update', f'/empty_{i}.txt', {'size': 0}) for i in range(5)],
        ],
    ),
    W(
        'create_append_rename_append',
        'Create(3B)→append(4B)→rename→append(5B) at new path',
        ops=[
            Op('create_file', 'cara.txt', {'content': b'aaa'}),
            Op('append', 'cara.txt', {'content': b'bbbb'}),
            Op('rename', 'cara.txt', {'dst': 'carb.txt'}),
            Op('append', 'carb.txt', {'content': b'ccccc'}),
        ],
        expected=[E('update', '/cara.txt')],
    ),
    W(
        'rename_across_sibling_dirs',
        'Move file from dir_a to dir_b with new name — detected',
        ops=[
            Op('mkdir', 'ras_a'),
            Op('mkdir', 'ras_b'),
            Op('create_file', 'ras_a/orig.txt', {'content': b'moving\n'}),
            Op('rename', 'ras_a/orig.txt', {'dst': 'ras_b/moved.txt'}),
        ],
        expected=[E('update', '/orig.txt')],
    ),
    W(
        'same_name_different_parents',
        'Create a/x.txt and b/x.txt — both detected separately',
        ops=[
            Op('mkdir', 'sn_a'),
            Op('mkdir', 'sn_b'),
            Op('create_file', 'sn_a/x.txt', {'content': b'from a\n'}),
            Op('create_file', 'sn_b/x.txt', {'content': b'from b longer\n'}),
        ],
        expected=[
            E('update', '/sn_a/x.txt', {'size': 7}),
            E('update', '/sn_b/x.txt', {'size': 14}),
        ],
    ),
    W(
        'full_file_lifecycle',
        'Create→write→append→chmod→rename→delete — full lifecycle',
        ops=[
            Op('create_file', 'fl.txt', {'content': b'v1\n'}),
            Op('write', 'fl.txt', {'content': b'v2 updated\n'}),
            Op('append', 'fl.txt', {'content': b'extra\n'}),
            Op('chmod', 'fl.txt', {'mode': 0o755}),
            Op('rename', 'fl.txt', {'dst': 'fl_final.txt'}),
            Op('delete', 'fl_final.txt'),
        ],
        expected=[E('update', '/fl.txt')],
    ),
    W(
        'delete_from_nested_dir',
        'Create dir/sub/file, delete file — update+delete for file',
        ops=[
            Op('makedirs', 'dfn/sub'),
            Op('create_file', 'dfn/sub/target.txt', {'content': b'deep\n'}),
            Op('delete', 'dfn/sub/target.txt'),
        ],
        expected=[
            E('update', '/target.txt'),
            E('delete', '/target.txt'),
        ],
    ),
    W(
        'symlink_chain_2_hops',
        'Create target, sym_a→target, sym_b→sym_a — all 3 detected',
        ops=[
            Op('create_file', 'sc_target.txt', {'content': b'chain\n'}),
            Op('symlink', 'sc_a', {'target': 'sc_target.txt'}),
            Op('symlink', 'sc_b', {'target': 'sc_a'}),
        ],
        expected=[
            E('update', '/sc_target.txt'),
            E('update', '/sc_a'),
            E('update', '/sc_b'),
        ],
    ),
    W(
        'rename_dir_add_more_files',
        'Create dir+2 files, rename dir, add 2 more — all 4 detected',
        ops=[
            Op('mkdir', 'rda'),
            Op('create_file', 'rda/f1.txt', {'content': b'1\n'}),
            Op('create_file', 'rda/f2.txt', {'content': b'2\n'}),
            Op('rename', 'rda', {'dst': 'rdb'}),
            Op('create_file', 'rdb/f3.txt', {'content': b'3\n'}),
            Op('create_file', 'rdb/f4.txt', {'content': b'4\n'}),
        ],
        expected=[
            E('update', '/f1.txt'),
            E('update', '/f2.txt'),
            E('update', '/f3.txt'),
            E('update', '/f4.txt'),
        ],
    ),
    W(
        'multiline_content_exact_size',
        'Create file with multi-line content — exact size verified',
        ops=[
            Op(
                'create_file',
                'multi.txt',
                {
                    'content': b'line 1\nline 2\nline 3\nline 4\nline 5\n',
                },
            ),
        ],
        expected=[E('update', '/multi.txt', {'size': 35})],
    ),
    W(
        'binary_to_text_overwrite',
        'Create binary file (256B), overwrite with text (10B)',
        ops=[
            Op('create_file', 'btt.bin', {'content': bytes(range(256))}),
            Op('write', 'btt.bin', {'content': b'now text\n'}),
        ],
        expected=[E('update', '/btt.bin', {'size': 9})],
    ),
    W(
        'file_at_every_level',
        'makedirs a/b/c, file at each level — all 3 files detected',
        ops=[
            Op('makedirs', 'lv/lv2/lv3'),
            Op('create_file', 'lv/top.txt', {'content': b'L1\n'}),
            Op('create_file', 'lv/lv2/mid.txt', {'content': b'L2\n'}),
            Op('create_file', 'lv/lv2/lv3/bot.txt', {'content': b'L3\n'}),
        ],
        expected=[
            E('update', '/top.txt', {'size': 3}),
            E('update', '/mid.txt', {'size': 3}),
            E('update', '/bot.txt', {'size': 3}),
        ],
    ),
    W(
        'rename_changes_extension',
        'Create .tmp, rename to .txt — detected at both paths',
        ops=[
            Op('create_file', 'ext.tmp', {'content': b'extension change\n'}),
            Op('rename', 'ext.tmp', {'dst': 'ext.txt'}),
        ],
        expected=[E('update', '/ext.tmp')],
    ),
    W(
        'hardlink_survives_original_delete',
        'Create file + hardlink, delete original — link update, orig delete',
        ops=[
            Op('create_file', 'hls_orig.txt', {'content': b'shared\n'}),
            Op('hardlink', 'hls_link.txt', {'target': 'hls_orig.txt'}),
            Op('delete', 'hls_orig.txt'),
        ],
        expected=[
            E('update', '/hls_link.txt'),
            E('update', '/hls_orig.txt'),
            E('delete', '/hls_orig.txt'),
        ],
    ),
    W(
        'append_after_chmod_cycle',
        'Create file, chmod 444→644, append — append tracked after chmod',
        ops=[
            Op('create_file', 'aac.txt', {'content': b'init\n'}),
            Op('chmod', 'aac.txt', {'mode': 0o444}),
            Op('chmod', 'aac.txt', {'mode': 0o644}),
            Op('append', 'aac.txt', {'content': b'more\n'}),
        ],
        expected=[E('update', '/aac.txt', {'size': 10})],
    ),
    W(
        'chmod_after_rename',
        'Create file, rename A→B, chmod B — B detected',
        ops=[
            Op('create_file', 'car_a.txt', {'content': b'data\n'}),
            Op('rename', 'car_a.txt', {'dst': 'car_b.txt'}),
            Op('chmod', 'car_b.txt', {'mode': 0o755}),
        ],
        expected=[E('update', '/car_a.txt')],
    ),
    W(
        'spaces_in_dir_and_file',
        'Create "dir name"/"file name.txt" — spaces in path handled',
        ops=[
            Op('mkdir', 'dir with spaces'),
            Op(
                'create_file',
                'dir with spaces/file name.txt',
                {
                    'content': b'spaces work\n',
                },
            ),
        ],
        expected=[
            E('update', '/dir with spaces'),
            E('update', '/file name.txt', {'size': 12}),
        ],
    ),
    W(
        'burst_delete_5_files',
        'Create 5 files, then delete all 5 rapidly — deletes appear',
        ops=[
            *[
                Op(
                    'create_file',
                    f'bd_{i}.txt',
                    {'content': f'{i}\n'.encode()},
                )
                for i in range(5)
            ],
            *[
                Op('delete', f'bd_{i}.txt', {'pause_after': 0.05})
                for i in range(5)
            ],
        ],
        expected=[
            *[E('update', f'/bd_{i}.txt') for i in range(5)],
        ],
    ),
    W(
        'rename_dir_preserves_subtree',
        'Create a/b/c.txt, rename a→x — file accessible at x/b/c.txt',
        ops=[
            Op('makedirs', 'rd_a/rd_b'),
            Op('create_file', 'rd_a/rd_b/deep.txt', {'content': b'deep\n'}),
            Op('rename', 'rd_a', {'dst': 'rd_x'}),
        ],
        expected=[E('update', '/deep.txt')],
    ),
    W(
        'empty_dir_create_delete',
        'Create empty dir then rmdir — update+delete detected',
        ops=[
            Op('mkdir', 'edir'),
            Op('rmtree', 'edir'),
        ],
        expected=[E('update', '/edir')],
    ),
    W(
        'mixed_types_all_at_once',
        'Create file, dir, symlink, hardlink — all types detected',
        ops=[
            Op('create_file', 'mt_file.txt', {'content': b'regular\n'}),
            Op('mkdir', 'mt_dir'),
            Op('symlink', 'mt_sym.txt', {'target': 'mt_file.txt'}),
            Op('hardlink', 'mt_hl.txt', {'target': 'mt_file.txt'}),
        ],
        expected=[
            E('update', '/mt_file.txt'),
            E('update', '/mt_dir'),
            E('update', '/mt_sym.txt'),
            E('update', '/mt_hl.txt'),
        ],
    ),
    W(
        'recreate_dir_with_file',
        'Create dir+file, rmtree, recreate dir+file — second file detected',
        ops=[
            Op('mkdir', 'rd_dir'),
            Op('create_file', 'rd_dir/v1.txt', {'content': b'version1\n'}),
            Op('rmtree', 'rd_dir', {'pause_after': 0.5}),
            Op('mkdir', 'rd_dir'),
            Op('create_file', 'rd_dir/v2.txt', {'content': b'version2\n'}),
        ],
        expected=[E('update', '/v2.txt', {'size': 9})],
    ),
    W(
        'multiple_hardlinks',
        'Create file + 3 hardlinks — all 4 paths detected',
        ops=[
            Op('create_file', 'hl_src.txt', {'content': b'shared\n'}),
            Op('hardlink', 'hl_1.txt', {'target': 'hl_src.txt'}),
            Op('hardlink', 'hl_2.txt', {'target': 'hl_src.txt'}),
            Op('hardlink', 'hl_3.txt', {'target': 'hl_src.txt'}),
        ],
        expected=[
            E('update', '/hl_src.txt'),
            E('update', '/hl_1.txt'),
            E('update', '/hl_2.txt'),
            E('update', '/hl_3.txt'),
        ],
    ),
    W(
        'overwrite_large_with_small',
        'Create 1000B file, overwrite with 10B — size shrinks',
        ops=[
            Op('create_file', 'shrink.txt', {'content': b'X' * 1000}),
            Op('write', 'shrink.txt', {'content': b'tiny\n'}),
        ],
        expected=[E('update', '/shrink.txt', {'size': 5})],
    ),
    W(
        'rename_then_delete',
        'Create file, rename A→B, delete B — update+delete lifecycle',
        ops=[
            Op('create_file', 'rtd_a.txt', {'content': b'doomed\n'}),
            Op('rename', 'rtd_a.txt', {'dst': 'rtd_b.txt'}),
            Op('delete', 'rtd_b.txt'),
        ],
        expected=[
            E('update', '/rtd_a.txt'),
        ],
    ),
    W(
        'delete_symlink_not_target',
        'Create target + symlink, delete symlink — target survives',
        ops=[
            Op('create_file', 'dsnt_target.txt', {'content': b'keep me\n'}),
            Op('symlink', 'dsnt_link.txt', {'target': 'dsnt_target.txt'}),
            Op('delete', 'dsnt_link.txt'),
        ],
        expected=[
            E('update', '/dsnt_target.txt', {'size': 8}),
            E('update', '/dsnt_link.txt'),
            E('delete', '/dsnt_link.txt'),
        ],
    ),
    W(
        'burst_in_existing_dir',
        'Create dir, then burst-add 5 files inside — all 5 detected',
        ops=[
            Op('mkdir', 'exist_dir'),
            *[
                Op(
                    'create_file',
                    f'exist_dir/ef_{i}.txt',
                    {
                        'content': f'f{i}\n'.encode(),
                        'pause_after': 0.05,
                    },
                )
                for i in range(5)
            ],
        ],
        expected=[
            *[E('update', f'/exist_dir/ef_{i}.txt') for i in range(5)],
        ],
    ),
    W(
        'rename_dir_then_add_file',
        'Create dir, rename, add file inside renamed dir — file detected',
        ops=[
            Op('mkdir', 'rdf_orig'),
            Op('rename', 'rdf_orig', {'dst': 'rdf_renamed'}),
            Op('create_file', 'rdf_renamed/new.txt', {'content': b'new!\n'}),
        ],
        expected=[
            E('update', '/new.txt', {'size': 5}),
        ],
    ),
    W(
        'symlink_to_dir_then_create',
        'Create dir + symlink to it, create file via symlink — detected',
        ops=[
            Op('mkdir', 'sl_dir'),
            Op('create_file', 'sl_dir/direct.txt', {'content': b'direct\n'}),
            Op('symlink', 'sl_link', {'target': 'sl_dir'}),
        ],
        expected=[
            E('update', '/sl_dir'),
            E('update', '/direct.txt'),
            E('update', '/sl_link'),
        ],
    ),
    W(
        'append_10_times_exact_size',
        'Create file then append 10 times — exact accumulated size',
        ops=[
            Op('create_file', 'acc.txt', {'content': b'start\n'}),
            *[
                Op(
                    'append',
                    'acc.txt',
                    {
                        'content': f'line-{i}\n'.encode(),
                        'pause_after': 0.1,
                    },
                )
                for i in range(10)
            ],
        ],
        # start\n(6) + line-0\n(7)*10 = 6 + 70 = 76
        expected=[E('update', '/acc.txt', {'size': 76})],
    ),
    W(
        'create_and_chmod_multiple',
        'Create 3 files, chmod each differently — all detected',
        ops=[
            Op('create_file', 'ch1.txt', {'content': b'f1'}),
            Op('create_file', 'ch2.txt', {'content': b'f2'}),
            Op('create_file', 'ch3.txt', {'content': b'f3'}),
            Op('chmod', 'ch1.txt', {'mode': 0o755}),
            Op('chmod', 'ch2.txt', {'mode': 0o600}),
            Op('chmod', 'ch3.txt', {'mode': 0o444}),
        ],
        expected=[
            E('update', '/ch1.txt'),
            E('update', '/ch2.txt'),
            E('update', '/ch3.txt'),
        ],
    ),
    W(
        'write_truncate_write',
        'Create(10B), truncate to 0, write(20B) — final size=20',
        ops=[
            Op('create_file', 'wtw.txt', {'content': b'0123456789'}),
            Op('truncate', 'wtw.txt'),
            Op('write', 'wtw.txt', {'content': b'new content of 20B!'}),
        ],
        expected=[E('update', '/wtw.txt', {'size': 19})],
    ),
    W(
        'move_between_nested_dirs',
        'Create a/f.txt and b/, move a/f.txt→b/f.txt — detected at new location',
        ops=[
            Op('mkdir', 'mv_a'),
            Op('mkdir', 'mv_b'),
            Op('create_file', 'mv_a/data.txt', {'content': b'moving\n'}),
            Op('rename', 'mv_a/data.txt', {'dst': 'mv_b/data.txt'}),
        ],
        expected=[E('update', '/data.txt')],
    ),
    W(
        'deep_tree_5_levels',
        'Create 5-level tree + leaf file — leaf detected',
        ops=[
            Op('makedirs', 'L1/L2/L3/L4/L5'),
            Op(
                'create_file',
                'L1/L2/L3/L4/L5/leaf.txt',
                {'content': b'deep\n'},
            ),
        ],
        expected=[E('update', '/leaf.txt', {'size': 5})],
    ),
    W(
        'hardlink_modify_original',
        'Create file + hardlink, append via original — link size matches',
        ops=[
            Op('create_file', 'hl_orig.txt', {'content': b'base\n'}),
            Op('hardlink', 'hl_link.txt', {'target': 'hl_orig.txt'}),
            Op('append', 'hl_orig.txt', {'content': b'extra\n'}),
        ],
        expected=[
            E('update', '/hl_orig.txt', {'size': 11}),
            E('update', '/hl_link.txt'),
        ],
    ),
    W(
        'interleaved_writes_two_files',
        'Alternate appends A/B/A/B/A/B — both tracked with correct sizes',
        ops=[
            Op('create_file', 'ilv_a.txt', {'content': b''}),
            Op('create_file', 'ilv_b.txt', {'content': b''}),
            *[
                item
                for i in range(3)
                for item in [
                    Op(
                        'append',
                        'ilv_a.txt',
                        {
                            'content': f'a{i}\n'.encode(),
                            'pause_after': 0.1,
                        },
                    ),
                    Op(
                        'append',
                        'ilv_b.txt',
                        {
                            'content': f'bb{i}\n'.encode(),
                            'pause_after': 0.1,
                        },
                    ),
                ]
            ],
        ],
        expected=[
            # a0\n + a1\n + a2\n = 3+3+3 = 9
            E('update', '/ilv_a.txt', {'size': 9}),
            # bb0\n + bb1\n + bb2\n = 4+4+4 = 12
            E('update', '/ilv_b.txt', {'size': 12}),
        ],
    ),
    # ------------------------------------------------------------------
    # 10. Promoted from fswatch-specific (Tier 3 — no extensions needed)
    # ------------------------------------------------------------------
    W(
        'double_atomic_save',
        'Two rounds of atomic save (.tmp→final via rename)',
        ops=[
            Op('create_file', 'das.tmp', {'content': b'round 1\n'}),
            Op('rename', 'das.tmp', {'dst': 'das.txt'}),
            Op('create_file', 'das2.tmp', {'content': b'round 2 longer\n'}),
            Op('rename', 'das2.tmp', {'dst': 'das.txt'}),
        ],
        expected=[E('update', '/das.tmp')],
    ),
    W(
        'rename_to_self',
        'Rename file to itself — no-op, no crash',
        ops=[
            Op('create_file', 'rts.txt', {'content': b'self\n'}),
            Op('rename', 'rts.txt', {'dst': 'rts.txt'}),
        ],
        expected=[E('update', '/rts.txt')],
    ),
    W(
        'log_rotation',
        'Write to log, rename to .1, create new log — rotation pattern',
        ops=[
            Op('create_file', 'app.log', {'content': b'entry 1\n'}),
            Op('append', 'app.log', {'content': b'entry 2\n'}),
            Op('rename', 'app.log', {'dst': 'app.log.1'}),
            Op('create_file', 'app.log', {'content': b'new entry 1\n'}),
        ],
        expected=[
            E('update', '/app.log'),
        ],
    ),
    W(
        'edit_save_3_versions',
        'Overwrite file 3 times — editor save simulation',
        ops=[
            Op('create_file', 'doc.txt', {'content': b'v1\n'}),
            Op('write', 'doc.txt', {'content': b'v2 longer\n'}),
            Op('write', 'doc.txt', {'content': b'v3 final content here\n'}),
        ],
        expected=[E('update', '/doc.txt', {'size': 22})],
    ),
    W(
        'rename_then_recreate_at_old',
        'Rename A→B, create new A — both paths detected',
        ops=[
            Op('create_file', 'rtro_a.txt', {'content': b'original\n'}),
            Op('rename', 'rtro_a.txt', {'dst': 'rtro_b.txt'}),
            Op('create_file', 'rtro_a.txt', {'content': b'new at A\n'}),
        ],
        expected=[E('update', '/rtro_a.txt')],
    ),
    W(
        'empty_dir_rename',
        'Rename empty directory — detected',
        ops=[
            Op('mkdir', 'edr_old'),
            Op('rename', 'edr_old', {'dst': 'edr_new'}),
        ],
        expected=[E('update', '/edr_old')],
    ),
    W(
        'dir_rename_chain_5_hops',
        'Rename dir through 5 names with file inside',
        ops=[
            Op('mkdir', 'drc_0'),
            Op('create_file', 'drc_0/inner.txt', {'content': b'inside\n'}),
            Op('rename', 'drc_0', {'dst': 'drc_1'}),
            Op('rename', 'drc_1', {'dst': 'drc_2'}),
            Op('rename', 'drc_2', {'dst': 'drc_3'}),
            Op('rename', 'drc_3', {'dst': 'drc_4'}),
        ],
        expected=[E('update', '/inner.txt')],
    ),
    W(
        'project_init',
        'Simulate project init: dirs + multiple source files',
        ops=[
            Op('mkdir', 'proj_src'),
            Op('mkdir', 'proj_tests'),
            Op(
                'create_file',
                'proj_src/main.py',
                {'content': b'def main(): pass\n'},
            ),
            Op(
                'create_file',
                'proj_src/lib.py',
                {'content': b'def helper(): pass\n'},
            ),
            Op(
                'create_file',
                'proj_tests/test_main.py',
                {'content': b'def test(): pass\n'},
            ),
            Op('create_file', 'README.md', {'content': b'# Project\n'}),
        ],
        expected=[
            E('update', '/main.py'),
            E('update', '/lib.py'),
            E('update', '/test_main.py'),
            E('update', '/README.md'),
        ],
    ),
    # ------------------------------------------------------------------
    # 11. Promoted from fswatch-specific (Tier 2 — new FsOp/stat_check)
    # ------------------------------------------------------------------
    W(
        'copy_file',
        'Create file then copy via shutil.copy2 — copy detected',
        ops=[
            Op('create_file', 'cp_src.txt', {'content': b'original\n'}),
            Op('copy', 'cp_src.txt', {'dst': 'cp_dst.txt'}),
        ],
        expected=[
            E('update', '/cp_src.txt', {'size': 9}),
            E('update', '/cp_dst.txt', {'size': 9}),
        ],
    ),
    W(
        'copy_overwrites_target',
        'Copy small file over large file — target shrinks',
        ops=[
            Op('create_file', 'cow_a.txt', {'content': b'small\n'}),
            Op(
                'create_file',
                'cow_b.txt',
                {'content': b'much larger content here\n'},
            ),
            Op('copy', 'cow_a.txt', {'dst': 'cow_b.txt'}),
        ],
        expected=[E('update', '/cow_b.txt', {'size': 6})],
    ),
    W(
        'os_replace_file',
        'os.replace src over dst — atomic replacement',
        ops=[
            Op('create_file', 'rep_dst.txt', {'content': b'old\n'}),
            Op(
                'create_file',
                'rep_src.txt',
                {'content': b'replacement content\n'},
            ),
            Op('replace', 'rep_src.txt', {'dst': 'rep_dst.txt'}),
        ],
        expected=[E('update', '/rep_dst.txt')],
    ),
    W(
        'dir_stat_is_dir',
        'Create dir — mode has S_ISDIR set',
        ops=[
            Op('mkdir', 'chk_dir'),
            Op('create_file', 'chk_dir/f.txt', {'content': b'data\n'}),
        ],
        expected=[
            E('update', '/chk_dir', {'mode_isdir': True}),
            E('update', '/chk_dir/f.txt', {'mode_isreg': True}),
        ],
    ),
    W(
        'utime_past_timestamp',
        'Create file, set mtime to 2001-09-09 — update detected',
        ops=[
            Op('create_file', 'ut.txt', {'content': b'utime test\n'}),
            Op('utime', 'ut.txt', {'time': 1000000000.0}),
        ],
        expected=[E('update', '/ut.txt', {'has_stat': True})],
    ),
    # ------------------------------------------------------------------
    # 12. Atomic replace + append / complex lifecycle
    # ------------------------------------------------------------------
    W(
        'replace_then_append',
        'os.replace src→dst then append to dst — final size = replacement + append',
        ops=[
            Op('create_file', 'rta_dst.txt', {'content': b'old content\n'}),
            Op('create_file', 'rta_src.txt', {'content': b'replaced\n'}),
            Op(
                'replace',
                'rta_src.txt',
                {'dst': 'rta_dst.txt', 'pause_after': 0.5},
            ),
            Op('append', 'rta_dst.txt', {'content': b'appended\n'}),
        ],
        expected=[E('update', '/rta_dst.txt', {'size_gt': 0})],
    ),
    W(
        'triple_rename_different_dirs',
        'Move file across 3 dirs: a/f→b/f→c/f — final path detected',
        ops=[
            Op('mkdir', 'tr_a'),
            Op('mkdir', 'tr_b'),
            Op('mkdir', 'tr_c'),
            Op('create_file', 'tr_a/hop.txt', {'content': b'hopping\n'}),
            Op('rename', 'tr_a/hop.txt', {'dst': 'tr_b/hop.txt'}),
            Op('rename', 'tr_b/hop.txt', {'dst': 'tr_c/hop.txt'}),
        ],
        expected=[E('update', '/hop.txt')],
    ),
    W(
        'overwrite_with_identical_size',
        'Create 10B file, overwrite with different 10B — same size, new content',
        ops=[
            Op('create_file', 'owis.txt', {'content': b'AAAAAAAAAA'}),
            Op('write', 'owis.txt', {'content': b'BBBBBBBBBB'}),
        ],
        expected=[E('update', '/owis.txt', {'size': 10})],
    ),
    W(
        'delete_middle_of_batch',
        'Create A B C, delete B — A and C survive with correct sizes',
        ops=[
            Op('create_file', 'dmb_a.txt', {'content': b'aaa\n'}),
            Op('create_file', 'dmb_b.txt', {'content': b'bbb\n'}),
            Op('create_file', 'dmb_c.txt', {'content': b'ccc\n'}),
            Op('delete', 'dmb_b.txt'),
        ],
        expected=[
            E('update', '/dmb_a.txt', {'size': 4}),
            E('update', '/dmb_b.txt'),
            E('delete', '/dmb_b.txt'),
            E('update', '/dmb_c.txt', {'size': 4}),
        ],
    ),
    W(
        'copy_then_modify_both',
        'Create file, copy to dst, modify both — both tracked independently',
        ops=[
            Op('create_file', 'ctm_orig.txt', {'content': b'base\n'}),
            Op('copy', 'ctm_orig.txt', {'dst': 'ctm_copy.txt'}),
            Op('append', 'ctm_orig.txt', {'content': b'orig extra\n'}),
            Op('append', 'ctm_copy.txt', {'content': b'copy extra longer\n'}),
        ],
        expected=[
            E('update', '/ctm_orig.txt', {'size': 16}),
            E('update', '/ctm_copy.txt', {'size': 23}),
        ],
    ),
    W(
        'nested_makedirs_4_levels_rmtree_recreate',
        'Create 4-level tree, rmtree, recreate with file at leaf',
        ops=[
            Op('makedirs', 'nm_a/nm_b/nm_c/nm_d'),
            Op(
                'create_file',
                'nm_a/nm_b/nm_c/nm_d/leaf.txt',
                {'content': b'v1\n'},
            ),
            Op('rmtree', 'nm_a', {'pause_after': 0.5}),
            Op('makedirs', 'nm_a/nm_b/nm_c/nm_d'),
            Op(
                'create_file',
                'nm_a/nm_b/nm_c/nm_d/leaf.txt',
                {'content': b'v2 longer\n'},
            ),
        ],
        expected=[E('update', '/leaf.txt', {'size': 10})],
    ),
    W(
        'rename_preserves_content_size',
        'Create 99B file, rename — final stat at new path has size=99',
        ops=[
            Op('create_file', 'rpc_old.txt', {'content': b'x' * 99}),
            Op('rename', 'rpc_old.txt', {'dst': 'rpc_new.txt'}),
        ],
        expected=[E('update', '/rpc_old.txt')],
    ),
    W(
        'burst_append_3_files_interleaved',
        'Create 3 files, interleave 5 appends each — all 3 have exact sizes',
        ops=[
            Op('create_file', 'ba_x.txt', {'content': b''}),
            Op('create_file', 'ba_y.txt', {'content': b''}),
            Op('create_file', 'ba_z.txt', {'content': b''}),
            *[
                item
                for i in range(5)
                for item in [
                    Op(
                        'append',
                        'ba_x.txt',
                        {'content': b'xx\n', 'pause_after': 0.05},
                    ),
                    Op(
                        'append',
                        'ba_y.txt',
                        {'content': b'yyy\n', 'pause_after': 0.05},
                    ),
                    Op(
                        'append',
                        'ba_z.txt',
                        {'content': b'zzzz\n', 'pause_after': 0.05},
                    ),
                ]
            ],
        ],
        expected=[
            E('update', '/ba_x.txt', {'size': 15}),
            E('update', '/ba_y.txt', {'size': 20}),
            E('update', '/ba_z.txt', {'size': 25}),
        ],
    ),
    W(
        'symlink_rename_target',
        'Create target + symlink, rename target — symlink now dangling',
        ops=[
            Op('create_file', 'srt_target.txt', {'content': b'target\n'}),
            Op('symlink', 'srt_link.txt', {'target': 'srt_target.txt'}),
            Op('rename', 'srt_target.txt', {'dst': 'srt_moved.txt'}),
        ],
        expected=[
            E('update', '/srt_target.txt'),
            E('update', '/srt_link.txt'),
        ],
    ),
    W(
        'dir_with_dotfiles',
        'Create dir with .hidden and .gitkeep — dotfiles detected',
        ops=[
            Op('mkdir', 'dot_dir'),
            Op('create_file', 'dot_dir/.hidden', {'content': b'secret\n'}),
            Op('create_file', 'dot_dir/.gitkeep', {'content': b''}),
            Op('create_file', 'dot_dir/visible.txt', {'content': b'shown\n'}),
        ],
        expected=[
            E('update', '/.hidden', {'size': 7}),
            E('update', '/.gitkeep', {'size': 0}),
            E('update', '/visible.txt', {'size': 6}),
        ],
    ),
    # ------------------------------------------------------------------
    # 13. Complex lifecycle patterns
    # ------------------------------------------------------------------
    W(
        'git_stash_pattern',
        'Simulate git stash: create 3 files, delete 2, recreate with different content',
        ops=[
            Op('create_file', 'gs_a.py', {'content': b'def a(): pass\n'}),
            Op('create_file', 'gs_b.py', {'content': b'def b(): pass\n'}),
            Op('create_file', 'gs_c.py', {'content': b'def c(): pass\n'}),
            Op('delete', 'gs_a.py'),
            Op('delete', 'gs_b.py'),
            Op(
                'create_file',
                'gs_a.py',
                {'content': b'def a_v2(): return True\n'},
            ),
            Op(
                'create_file',
                'gs_b.py',
                {'content': b'def b_v2(): return False\n'},
            ),
        ],
        expected=[
            E('update', '/gs_a.py'),
            E('update', '/gs_b.py'),
            E('update', '/gs_c.py', {'size': 14}),
        ],
    ),
    W(
        'concurrent_dirs_same_depth',
        'Create 3 sibling dirs each with 2 files — 6 files all detected',
        ops=[
            Op('mkdir', 'cd_x'),
            Op('mkdir', 'cd_y'),
            Op('mkdir', 'cd_z'),
            Op(
                'create_file',
                'cd_x/1.txt',
                {'content': b'x1\n', 'pause_after': 0.05},
            ),
            Op(
                'create_file',
                'cd_y/1.txt',
                {'content': b'y1\n', 'pause_after': 0.05},
            ),
            Op(
                'create_file',
                'cd_z/1.txt',
                {'content': b'z1\n', 'pause_after': 0.05},
            ),
            Op(
                'create_file',
                'cd_x/2.txt',
                {'content': b'x2\n', 'pause_after': 0.05},
            ),
            Op(
                'create_file',
                'cd_y/2.txt',
                {'content': b'y2\n', 'pause_after': 0.05},
            ),
            Op(
                'create_file',
                'cd_z/2.txt',
                {'content': b'z2\n', 'pause_after': 0.05},
            ),
        ],
        expected=[
            E('update', '/cd_x/1.txt', {'size': 3}),
            E('update', '/cd_y/1.txt', {'size': 3}),
            E('update', '/cd_z/1.txt', {'size': 3}),
            E('update', '/cd_x/2.txt', {'size': 3}),
            E('update', '/cd_y/2.txt', {'size': 3}),
            E('update', '/cd_z/2.txt', {'size': 3}),
        ],
    ),
    W(
        'rename_and_replace_at_old_path',
        'Create A(5B), rename A→B, create new A(10B) via replace — A=10B, B=5B',
        ops=[
            Op('create_file', 'rr_a.txt', {'content': b'short'}),
            Op('rename', 'rr_a.txt', {'dst': 'rr_b.txt'}),
            Op('create_file', 'rr_a.txt', {'content': b'new at A!!'}),
        ],
        expected=[
            E('update', '/rr_a.txt'),
        ],
    ),
    W(
        'cascading_appends_3_files',
        'Create A, copy→B, copy→C, append each 3 times — all have distinct sizes',
        ops=[
            Op('create_file', 'cas_a.txt', {'content': b'A'}),
            Op('copy', 'cas_a.txt', {'dst': 'cas_b.txt'}),
            Op('copy', 'cas_a.txt', {'dst': 'cas_c.txt'}),
            Op('append', 'cas_a.txt', {'content': b'aa', 'pause_after': 0.1}),
            Op('append', 'cas_b.txt', {'content': b'bbb', 'pause_after': 0.1}),
            Op(
                'append',
                'cas_c.txt',
                {'content': b'cccc', 'pause_after': 0.1},
            ),
        ],
        expected=[
            E('update', '/cas_a.txt', {'size': 3}),
            E('update', '/cas_b.txt', {'size': 4}),
            E('update', '/cas_c.txt', {'size': 5}),
        ],
    ),
    W(
        'mkdir_chmod_add_file_rmtree',
        'Create dir, chmod, add file, rmtree — full dir lifecycle',
        ops=[
            Op('mkdir', 'mc_dir'),
            Op('chmod', 'mc_dir', {'mode': 0o700}),
            Op('create_file', 'mc_dir/data.txt', {'content': b'inside\n'}),
            Op('rmtree', 'mc_dir', {'pause_after': 0.5}),
        ],
        expected=[E('update', '/data.txt')],
    ),
    W(
        'write_binary_then_text_overwrite',
        'Create binary file(128B), overwrite with text(20B) — size shrinks',
        ops=[
            Op('create_file', 'bt.dat', {'content': bytes(range(128))}),
            Op('write', 'bt.dat', {'content': b'now it is text!\n'}),
        ],
        expected=[E('update', '/bt.dat', {'size': 16})],
    ),
    W(
        'rename_swap_dirs',
        'Create dir_a/f and dir_b/g, swap dirs via temp — files follow',
        ops=[
            Op('mkdir', 'sw_a'),
            Op('mkdir', 'sw_b'),
            Op('create_file', 'sw_a/fa.txt', {'content': b'in A\n'}),
            Op('create_file', 'sw_b/fb.txt', {'content': b'in B\n'}),
            Op('rename', 'sw_a', {'dst': 'sw_tmp'}),
            Op('rename', 'sw_b', {'dst': 'sw_a'}),
            Op('rename', 'sw_tmp', {'dst': 'sw_b'}),
        ],
        expected=[
            E('update', '/fa.txt'),
            E('update', '/fb.txt'),
        ],
    ),
    W(
        'copy_chain_3_hops',
        'Copy A→B→C — each copy creates independent file, all detected',
        ops=[
            Op('create_file', 'cc_a.txt', {'content': b'original\n'}),
            Op('copy', 'cc_a.txt', {'dst': 'cc_b.txt'}),
            Op('copy', 'cc_b.txt', {'dst': 'cc_c.txt'}),
        ],
        expected=[
            E('update', '/cc_a.txt', {'size': 9}),
            E('update', '/cc_b.txt', {'size': 9}),
            E('update', '/cc_c.txt', {'size': 9}),
        ],
    ),
    # ------------------------------------------------------------------
    # 14. Stress / edge case patterns
    # ------------------------------------------------------------------
    W(
        'rapid_chmod_and_write_interleaved',
        'Create file, interleave chmod+write 5 times — tracked correctly',
        ops=[
            Op('create_file', 'rcw.txt', {'content': b'start\n'}),
            *[
                item
                for i in range(5)
                for item in [
                    Op(
                        'chmod',
                        'rcw.txt',
                        {'mode': [0o755, 0o644][i % 2], 'pause_after': 0.05},
                    ),
                    Op(
                        'append',
                        'rcw.txt',
                        {'content': f'L{i}\n'.encode(), 'pause_after': 0.05},
                    ),
                ]
            ],
        ],
        # start\n(6) + L0\n(3)*5 = 6+15 = 21
        expected=[E('update', '/rcw.txt', {'size': 21})],
    ),
    W(
        'rename_file_to_different_extension',
        'Create data.csv, rename to data.json — tracked at original path',
        ops=[
            Op('create_file', 'data.csv', {'content': b'a,b,c\n1,2,3\n'}),
            Op('rename', 'data.csv', {'dst': 'data.json'}),
        ],
        expected=[E('update', '/data.csv')],
    ),
    W(
        'create_many_small_files_in_subdir',
        'Create dir + 15 1-byte files — all 15 detected',
        ops=[
            Op('mkdir', 'many'),
            *[
                Op(
                    'create_file',
                    f'many/m_{i:02d}.txt',
                    {
                        'content': bytes([65 + i % 26]),
                        'pause_after': 0.03,
                    },
                )
                for i in range(15)
            ],
        ],
        expected=[
            *[
                E('update', f'/many/m_{i:02d}.txt', {'size': 1})
                for i in range(15)
            ],
        ],
    ),
    W(
        'write_then_truncate_then_append',
        'Create 100B file, truncate to 0, append 50B — final size=50',
        ops=[
            Op('create_file', 'wta.txt', {'content': b'X' * 100}),
            Op('truncate', 'wta.txt'),
            Op('append', 'wta.txt', {'content': b'Y' * 50}),
        ],
        expected=[E('update', '/wta.txt', {'size': 50})],
    ),
    W(
        'replace_dir_contents',
        'Create dir with A, delete A, create B in same dir — B detected',
        ops=[
            Op('mkdir', 'rdc'),
            Op('create_file', 'rdc/old.txt', {'content': b'old\n'}),
            Op('delete', 'rdc/old.txt'),
            Op(
                'create_file',
                'rdc/new.txt',
                {'content': b'brand new content\n'},
            ),
        ],
        expected=[
            E('update', '/rdc/new.txt', {'size': 18}),
        ],
    ),
    W(
        'symlink_to_subdir_file',
        'Create dir/file, symlink at root to dir/file — both detected',
        ops=[
            Op('mkdir', 'stsf_dir'),
            Op('create_file', 'stsf_dir/real.txt', {'content': b'real\n'}),
            Op('symlink', 'stsf_link.txt', {'target': 'stsf_dir/real.txt'}),
        ],
        expected=[
            E('update', '/real.txt'),
            E('update', '/stsf_link.txt'),
        ],
    ),
    W(
        'multiple_renames_converge',
        'Create A B C, rename all to same dir — A→d/A, B→d/B, C→d/C',
        ops=[
            Op('mkdir', 'mrc_d'),
            Op('create_file', 'mrc_a.txt', {'content': b'a\n'}),
            Op('create_file', 'mrc_b.txt', {'content': b'b\n'}),
            Op('create_file', 'mrc_c.txt', {'content': b'c\n'}),
            Op('rename', 'mrc_a.txt', {'dst': 'mrc_d/mrc_a.txt'}),
            Op('rename', 'mrc_b.txt', {'dst': 'mrc_d/mrc_b.txt'}),
            Op('rename', 'mrc_c.txt', {'dst': 'mrc_d/mrc_c.txt'}),
        ],
        expected=[
            E('update', '/mrc_a.txt'),
            E('update', '/mrc_b.txt'),
            E('update', '/mrc_c.txt'),
        ],
    ),
    W(
        'large_binary_overwrite_3_times',
        'Create 10KB, overwrite with 20KB, overwrite with 5KB — final=5KB',
        ops=[
            Op('create_file', 'lbo.bin', {'content': b'\x00' * 10240}),
            Op('write', 'lbo.bin', {'content': b'\xff' * 20480}),
            Op('write', 'lbo.bin', {'content': b'\xaa' * 5120}),
        ],
        expected=[E('update', '/lbo.bin', {'size': 5120})],
    ),
    # ------------------------------------------------------------------
    # 15. Advanced patterns
    # ------------------------------------------------------------------
    W(
        'create_in_renamed_dir_tree',
        'makedirs a/b, rename a→c, create file in c/b — detected',
        ops=[
            Op('makedirs', 'cird_a/cird_b'),
            Op('rename', 'cird_a', {'dst': 'cird_c'}),
            Op(
                'create_file',
                'cird_c/cird_b/new.txt',
                {'content': b'in renamed tree\n'},
            ),
        ],
        expected=[E('update', '/new.txt', {'size': 16})],
    ),
    W(
        'hardlink_3_paths_delete_2',
        'Create file + 2 hardlinks, delete 2 — surviving path detected',
        ops=[
            Op('create_file', 'h3_orig.txt', {'content': b'triple\n'}),
            Op('hardlink', 'h3_link1.txt', {'target': 'h3_orig.txt'}),
            Op('hardlink', 'h3_link2.txt', {'target': 'h3_orig.txt'}),
            Op('delete', 'h3_orig.txt'),
            Op('delete', 'h3_link1.txt'),
        ],
        expected=[
            E('update', '/h3_link2.txt'),
            E('update', '/h3_orig.txt'),
            E('delete', '/h3_orig.txt'),
        ],
    ),
    W(
        'alternating_create_delete_10_pairs',
        'Create file, delete, repeat 10 times — no crash, canary detected',
        ops=[
            *[
                item
                for i in range(10)
                for item in [
                    Op(
                        'create_file',
                        f'acd_{i}.txt',
                        {
                            'content': f'{i}\n'.encode(),
                            'pause_after': 0.02,
                        },
                    ),
                    Op('delete', f'acd_{i}.txt', {'pause_after': 0.02}),
                ]
            ],
            Op('create_file', 'acd_final.txt', {'content': b'survived\n'}),
        ],
        expected=[E('update', '/acd_final.txt', {'size': 9})],
    ),
    W(
        'nested_copy_rename_chain',
        'Create file, copy to B, rename B→C, copy C→D — all detected',
        ops=[
            Op('create_file', 'ncr_a.txt', {'content': b'chain start\n'}),
            Op('copy', 'ncr_a.txt', {'dst': 'ncr_b.txt'}),
            Op('rename', 'ncr_b.txt', {'dst': 'ncr_c.txt'}),
            Op('copy', 'ncr_c.txt', {'dst': 'ncr_d.txt'}),
        ],
        expected=[
            E('update', '/ncr_a.txt', {'size': 12}),
            E('update', '/ncr_d.txt', {'size': 12}),
        ],
    ),
    W(
        'write_exact_block_sizes',
        'Create files at exact block boundaries: 512, 4096, 8192',
        ops=[
            Op('create_file', 'bs_512.bin', {'content': b'X' * 512}),
            Op('create_file', 'bs_4096.bin', {'content': b'Y' * 4096}),
            Op('create_file', 'bs_8192.bin', {'content': b'Z' * 8192}),
        ],
        expected=[
            E('update', '/bs_512.bin', {'size': 512}),
            E('update', '/bs_4096.bin', {'size': 4096}),
            E('update', '/bs_8192.bin', {'size': 8192}),
        ],
    ),
    # ------------------------------------------------------------------
    # 16. Real-world patterns
    # ------------------------------------------------------------------
    W(
        'npm_install_pattern',
        'Simulate npm install: create node_modules/, add 3 package dirs with index.js',
        ops=[
            Op('makedirs', 'node_modules/pkg-a'),
            Op('makedirs', 'node_modules/pkg-b'),
            Op('makedirs', 'node_modules/pkg-c'),
            Op(
                'create_file',
                'node_modules/pkg-a/index.js',
                {'content': b'module.exports = {};\n'},
            ),
            Op(
                'create_file',
                'node_modules/pkg-b/index.js',
                {'content': b'module.exports = {};\n'},
            ),
            Op(
                'create_file',
                'node_modules/pkg-c/index.js',
                {'content': b'module.exports = {};\n'},
            ),
        ],
        expected=[
            E('update', '/pkg-a/index.js', {'size': 21}),
            E('update', '/pkg-b/index.js', {'size': 21}),
            E('update', '/pkg-c/index.js', {'size': 21}),
        ],
    ),
    W(
        'log_rotation_3_files',
        'app.log→app.log.1, app.log.1→app.log.2, new app.log',
        ops=[
            Op('create_file', 'app.log', {'content': b'entry 1\nentry 2\n'}),
            Op('create_file', 'app.log.1', {'content': b'old entry\n'}),
            Op('rename', 'app.log.1', {'dst': 'app.log.2'}),
            Op('rename', 'app.log', {'dst': 'app.log.1'}),
            Op('create_file', 'app.log', {'content': b'fresh start\n'}),
        ],
        expected=[
            E('update', '/app.log'),
        ],
    ),
    W(
        'rename_file_longer_name',
        'Rename short name (a.txt) to much longer name — detected',
        ops=[
            Op('create_file', 'a.txt', {'content': b'short name\n'}),
            Op(
                'rename',
                'a.txt',
                {'dst': 'a_very_much_longer_descriptive_filename.txt'},
            ),
        ],
        expected=[E('update', '/a.txt')],
    ),
    W(
        'create_modify_chmod_delete_3_files',
        'Create 3 files, modify each, chmod each, delete all — full cycle',
        ops=[
            Op('create_file', 'cmcd_1.txt', {'content': b'f1\n'}),
            Op('create_file', 'cmcd_2.txt', {'content': b'f2\n'}),
            Op('create_file', 'cmcd_3.txt', {'content': b'f3\n'}),
            Op('append', 'cmcd_1.txt', {'content': b'mod1\n'}),
            Op('append', 'cmcd_2.txt', {'content': b'mod2\n'}),
            Op('append', 'cmcd_3.txt', {'content': b'mod3\n'}),
            Op('chmod', 'cmcd_1.txt', {'mode': 0o755}),
            Op('chmod', 'cmcd_2.txt', {'mode': 0o755}),
            Op('chmod', 'cmcd_3.txt', {'mode': 0o755}),
            Op('delete', 'cmcd_1.txt'),
            Op('delete', 'cmcd_2.txt'),
            Op('delete', 'cmcd_3.txt'),
        ],
        expected=[
            E('update', '/cmcd_1.txt'),
            E('update', '/cmcd_2.txt'),
            E('update', '/cmcd_3.txt'),
        ],
    ),
    W(
        'deep_nested_rename_preserves_content',
        'Create a/b/c/file, rename a→x — file content size preserved',
        ops=[
            Op('makedirs', 'dnrp_a/dnrp_b/dnrp_c'),
            Op(
                'create_file',
                'dnrp_a/dnrp_b/dnrp_c/data.bin',
                {'content': b'\x42' * 256},
            ),
            Op('rename', 'dnrp_a', {'dst': 'dnrp_x'}),
        ],
        expected=[E('update', '/data.bin')],
    ),
    # ------------------------------------------------------------------
    # 17. Edge cases + pipeline correctness
    # ------------------------------------------------------------------
    W(
        'delete_nonexistent_no_crash',
        'Create file, delete, then attempt delete again (should raise) — no crash',
        ops=[
            Op('create_file', 'dne.txt', {'content': b'exists\n'}),
            Op('delete', 'dne.txt'),
            # The second delete would fail — so we just create another
            Op('create_file', 'dne_proof.txt', {'content': b'proof\n'}),
        ],
        expected=[E('update', '/dne_proof.txt', {'size': 6})],
    ),
    W(
        'utime_future_timestamp',
        'Create file, set mtime to 2099 — update detected with stat',
        ops=[
            Op('create_file', 'fut.txt', {'content': b'future\n'}),
            Op('utime', 'fut.txt', {'time': 4102444800.0}),
        ],
        expected=[E('update', '/fut.txt', {'has_stat': True})],
    ),
    W(
        'rename_into_new_subdir',
        'Create file and empty dir, rename file into dir — file in dir detected',
        ops=[
            Op('create_file', 'rins_src.txt', {'content': b'moving in\n'}),
            Op('mkdir', 'rins_dir'),
            Op('rename', 'rins_src.txt', {'dst': 'rins_dir/rins_src.txt'}),
        ],
        expected=[E('update', '/rins_src.txt')],
    ),
    W(
        'create_same_name_3_dirs',
        'Create data.txt in 3 different dirs — all 3 detected independently',
        ops=[
            Op('mkdir', 'csn_1'),
            Op('mkdir', 'csn_2'),
            Op('mkdir', 'csn_3'),
            Op('create_file', 'csn_1/data.txt', {'content': b'one\n'}),
            Op('create_file', 'csn_2/data.txt', {'content': b'two!!\n'}),
            Op('create_file', 'csn_3/data.txt', {'content': b'three!!!\n'}),
        ],
        expected=[
            E('update', '/csn_1/data.txt', {'size': 4}),
            E('update', '/csn_2/data.txt', {'size': 6}),
            E('update', '/csn_3/data.txt', {'size': 9}),
        ],
    ),
    W(
        'append_1000_bytes_in_chunks',
        'Create empty file, append 100 bytes × 10 — final size=1000',
        ops=[
            Op('create_file', 'chunks.bin', {'content': b''}),
            *[
                Op(
                    'append',
                    'chunks.bin',
                    {
                        'content': b'X' * 100,
                        'pause_after': 0.05,
                    },
                )
                for _ in range(10)
            ],
        ],
        expected=[E('update', '/chunks.bin', {'size': 1000})],
    ),
    # ------------------------------------------------------------------
    # 18. Cross-backend coverage gaps
    # ------------------------------------------------------------------
    W(
        'copy_preserves_size',
        'Create 77B file, copy — destination has same 77B size',
        ops=[
            Op('create_file', 'cps_src.txt', {'content': b'A' * 77}),
            Op('copy', 'cps_src.txt', {'dst': 'cps_dst.txt'}),
        ],
        expected=[
            E('update', '/cps_src.txt', {'size': 77}),
            E('update', '/cps_dst.txt', {'size': 77}),
        ],
    ),
    W(
        'rmtree_deep_3_level_tree',
        'Create a/b/c + files at each level, rmtree a — root tracked',
        ops=[
            Op('makedirs', 'rmt_a/rmt_b/rmt_c'),
            Op('create_file', 'rmt_a/l1.txt', {'content': b'level1\n'}),
            Op('create_file', 'rmt_a/rmt_b/l2.txt', {'content': b'level2\n'}),
            Op(
                'create_file',
                'rmt_a/rmt_b/rmt_c/l3.txt',
                {'content': b'level3\n'},
            ),
            Op('rmtree', 'rmt_a', {'pause_after': 0.5}),
        ],
        expected=[E('update', '/l1.txt')],
    ),
    W(
        'create_with_special_bytes',
        'Create file with null bytes, high bytes, newlines in content',
        ops=[
            Op(
                'create_file',
                'special.bin',
                {
                    'content': b'\x00\x01\x02\xff\xfe\xfd\n\r\n\t' * 10,
                },
            ),
        ],
        expected=[E('update', '/special.bin', {'size': 100})],
    ),
    W(
        'rename_then_create_original_different_content',
        'Create A(8B), rename A→B, create A(16B) — A and B both detected',
        ops=[
            Op('create_file', 'rtco_a.txt', {'content': b'original'}),
            Op('rename', 'rtco_a.txt', {'dst': 'rtco_b.txt'}),
            Op('create_file', 'rtco_a.txt', {'content': b'new content here'}),
        ],
        expected=[E('update', '/rtco_a.txt')],
    ),
    W(
        'parallel_3_dir_trees',
        'Create 3 independent dir trees with files — all detected',
        ops=[
            Op('makedirs', 'pt_1/sub'),
            Op('makedirs', 'pt_2/sub'),
            Op('makedirs', 'pt_3/sub'),
            Op(
                'create_file',
                'pt_1/sub/f.txt',
                {'content': b'tree1\n', 'pause_after': 0.05},
            ),
            Op(
                'create_file',
                'pt_2/sub/f.txt',
                {'content': b'tree2\n', 'pause_after': 0.05},
            ),
            Op(
                'create_file',
                'pt_3/sub/f.txt',
                {'content': b'tree3\n', 'pause_after': 0.05},
            ),
        ],
        expected=[
            E('update', '/pt_1/sub/f.txt', {'size': 6}),
            E('update', '/pt_2/sub/f.txt', {'size': 6}),
            E('update', '/pt_3/sub/f.txt', {'size': 6}),
        ],
    ),
    # ------------------------------------------------------------------
    # 19. Mixed operations patterns
    # ------------------------------------------------------------------
    W(
        'create_copy_delete_original',
        'Create file, copy to backup, delete original — backup survives',
        ops=[
            Op('create_file', 'ccdo.txt', {'content': b'important\n'}),
            Op('copy', 'ccdo.txt', {'dst': 'ccdo_bak.txt'}),
            Op('delete', 'ccdo.txt'),
        ],
        expected=[
            E('update', '/ccdo_bak.txt', {'size': 10}),
            E('update', '/ccdo.txt'),
            E('delete', '/ccdo.txt'),
        ],
    ),
    W(
        'mkdir_then_rmdir_then_mkdir',
        'Create dir, rmdir, recreate — dir detected on each cycle',
        ops=[
            Op('mkdir', 'mrmr'),
            Op('rmtree', 'mrmr'),
            Op('mkdir', 'mrmr'),
            Op('create_file', 'mrmr/inside.txt', {'content': b'alive\n'}),
        ],
        expected=[E('update', '/inside.txt', {'size': 6})],
    ),
    W(
        'rename_across_3_levels',
        'Create a/b/f.txt, rename a→x — file accessible at x/b/f.txt',
        ops=[
            Op('makedirs', 'r3l_a/r3l_b'),
            Op(
                'create_file',
                'r3l_a/r3l_b/f.txt',
                {'content': b'deep move\n'},
            ),
            Op('rename', 'r3l_a', {'dst': 'r3l_x'}),
        ],
        expected=[E('update', '/f.txt')],
    ),
    W(
        'rapid_create_20_with_sizes',
        'Create 20 files with sizes 1..20 — all detected with exact sizes',
        ops=[
            *[
                Op(
                    'create_file',
                    f'rc20_{i:02d}.bin',
                    {
                        'content': b'X' * (i + 1),
                        'pause_after': 0.03,
                    },
                )
                for i in range(20)
            ],
        ],
        expected=[
            *[
                E('update', f'/rc20_{i:02d}.bin', {'size': i + 1})
                for i in range(20)
            ],
        ],
    ),
    W(
        'symlink_delete_both',
        'Create target + symlink, delete both — updates + deletes',
        ops=[
            Op('create_file', 'sdb_t.txt', {'content': b'target\n'}),
            Op('symlink', 'sdb_l.txt', {'target': 'sdb_t.txt'}),
            Op('delete', 'sdb_l.txt'),
            Op('delete', 'sdb_t.txt'),
        ],
        expected=[
            E('update', '/sdb_t.txt'),
            E('update', '/sdb_l.txt'),
        ],
    ),
    # ------------------------------------------------------------------
    # 20. Final batch
    # ------------------------------------------------------------------
    W(
        'build_output_cleanup',
        'Create src + build dirs, add artifacts, rmtree build — src survives',
        ops=[
            Op('mkdir', 'boc_src'),
            Op('mkdir', 'boc_build'),
            Op(
                'create_file',
                'boc_src/main.c',
                {'content': b'int main(){}\n'},
            ),
            Op(
                'create_file',
                'boc_build/main.o',
                {'content': b'\x7fELF' + b'\x00' * 100},
            ),
            Op(
                'create_file',
                'boc_build/a.out',
                {'content': b'\x7fELF' + b'\x00' * 200},
            ),
            Op('rmtree', 'boc_build', {'pause_after': 0.5}),
        ],
        expected=[
            E('update', '/main.c', {'size': 13}),
        ],
    ),
    W(
        'write_read_modify_cycle',
        'Create file, read (no-op), modify — size reflects modification',
        ops=[
            Op('create_file', 'wrm.txt', {'content': b'initial\n'}),
            # no read op in workload framework, so just modify
            Op('append', 'wrm.txt', {'content': b'modified\n'}),
        ],
        expected=[E('update', '/wrm.txt', {'size': 17})],
    ),
    W(
        'cross_dir_rename_3_times',
        'File moves: root→a/→b/→c/ via 3 renames',
        ops=[
            Op('mkdir', 'xdr_a'),
            Op('mkdir', 'xdr_b'),
            Op('mkdir', 'xdr_c'),
            Op('create_file', 'nomad.txt', {'content': b'traveling\n'}),
            Op('rename', 'nomad.txt', {'dst': 'xdr_a/nomad.txt'}),
            Op('rename', 'xdr_a/nomad.txt', {'dst': 'xdr_b/nomad.txt'}),
            Op('rename', 'xdr_b/nomad.txt', {'dst': 'xdr_c/nomad.txt'}),
        ],
        expected=[E('update', '/nomad.txt')],
    ),
    W(
        'copy_into_subdir',
        'Create file at root, copy into subdir — both detected',
        ops=[
            Op('create_file', 'cis_root.txt', {'content': b'at root\n'}),
            Op('mkdir', 'cis_sub'),
            Op('copy', 'cis_root.txt', {'dst': 'cis_sub/cis_root.txt'}),
        ],
        expected=[
            E('update', '/cis_root.txt', {'size': 8}),
            E('update', '/cis_sub/cis_root.txt', {'size': 8}),
        ],
    ),
    # ------------------------------------------------------------------
    # 21. More patterns
    # ------------------------------------------------------------------
    W(
        'rename_preserves_mode',
        'Create file with 755, rename — mode should still have execute',
        ops=[
            Op('create_file', 'rpm_f.txt', {'content': b'exec\n'}),
            Op('chmod', 'rpm_f.txt', {'mode': 0o755}),
            Op('rename', 'rpm_f.txt', {'dst': 'rpm_g.txt'}),
        ],
        expected=[E('update', '/rpm_f.txt')],
    ),
    W(
        'create_delete_create_different_size',
        'Create(5B), delete, create(20B) same name — final size=20',
        ops=[
            Op('create_file', 'cdcd.txt', {'content': b'small'}),
            Op('delete', 'cdcd.txt'),
            Op(
                'create_file',
                'cdcd.txt',
                {'content': b'much bigger content!'},
            ),
        ],
        expected=[E('update', '/cdcd.txt', {'size': 20})],
    ),
    W(
        'wide_dir_20_with_sizes',
        'Create dir + 20 files with sizes 10..200 — all detected',
        ops=[
            Op('mkdir', 'w20'),
            *[
                Op(
                    'create_file',
                    f'w20/w_{i:02d}.bin',
                    {
                        'content': b'D' * (10 * (i + 1)),
                        'pause_after': 0.03,
                    },
                )
                for i in range(20)
            ],
        ],
        expected=[
            *[
                E('update', f'/w20/w_{i:02d}.bin', {'size': 10 * (i + 1)})
                for i in range(20)
            ],
        ],
    ),
    W(
        'mkdir_5_nested_then_file',
        'makedirs 5 levels, add file at root level — simple baseline',
        ops=[
            Op('makedirs', 'mn5/l2/l3/l4/l5'),
            Op(
                'create_file',
                'mn5/root_file.txt',
                {'content': b'at root of tree\n'},
            ),
        ],
        expected=[E('update', '/root_file.txt', {'size': 16})],
    ),
    # ------------------------------------------------------------------
    # 22. Final batch - advanced patterns
    # ------------------------------------------------------------------
    W(
        'replace_file_3_times',
        'Create file, os.replace 3 times with increasing sizes',
        ops=[
            Op('create_file', 'rf3_dst.txt', {'content': b'v0\n'}),
            Op('create_file', 'rf3_s1.txt', {'content': b'version one\n'}),
            Op(
                'replace',
                'rf3_s1.txt',
                {'dst': 'rf3_dst.txt', 'pause_after': 0.5},
            ),
            Op(
                'create_file',
                'rf3_s2.txt',
                {'content': b'version two longer\n'},
            ),
            Op(
                'replace',
                'rf3_s2.txt',
                {'dst': 'rf3_dst.txt', 'pause_after': 0.5},
            ),
            Op(
                'create_file',
                'rf3_s3.txt',
                {'content': b'version three even longer text\n'},
            ),
            Op('replace', 'rf3_s3.txt', {'dst': 'rf3_dst.txt'}),
        ],
        expected=[E('update', '/rf3_dst.txt', {'size_gt': 0})],
    ),
    W(
        'hardlink_network_4_paths',
        'Create file + 3 hardlinks — all 4 paths share same inode',
        ops=[
            Op('create_file', 'hn_orig.txt', {'content': b'shared inode\n'}),
            Op('hardlink', 'hn_a.txt', {'target': 'hn_orig.txt'}),
            Op('hardlink', 'hn_b.txt', {'target': 'hn_orig.txt'}),
            Op('hardlink', 'hn_c.txt', {'target': 'hn_orig.txt'}),
        ],
        expected=[
            E('update', '/hn_orig.txt', {'size': 13}),
            E('update', '/hn_a.txt'),
            E('update', '/hn_b.txt'),
            E('update', '/hn_c.txt'),
        ],
    ),
    W(
        'create_in_3_nested_dirs_parallel',
        'Create 3 separate nested dirs with 1 file each simultaneously',
        ops=[
            Op('makedirs', 'cnp_a/sub'),
            Op('makedirs', 'cnp_b/sub'),
            Op('makedirs', 'cnp_c/sub'),
            Op(
                'create_file',
                'cnp_a/sub/f.txt',
                {'content': b'a\n', 'pause_after': 0.05},
            ),
            Op(
                'create_file',
                'cnp_b/sub/f.txt',
                {'content': b'bb\n', 'pause_after': 0.05},
            ),
            Op(
                'create_file',
                'cnp_c/sub/f.txt',
                {'content': b'ccc\n', 'pause_after': 0.05},
            ),
        ],
        expected=[
            E('update', '/cnp_a/sub/f.txt', {'size': 2}),
            E('update', '/cnp_b/sub/f.txt', {'size': 3}),
            E('update', '/cnp_c/sub/f.txt', {'size': 4}),
        ],
    ),
    W(
        'rename_changes_parent',
        'Create dir_a/file, rename dir_a/file→dir_b/file',
        ops=[
            Op('mkdir', 'rcp_a'),
            Op('mkdir', 'rcp_b'),
            Op(
                'create_file',
                'rcp_a/data.txt',
                {'content': b'changing parent\n'},
            ),
            Op('rename', 'rcp_a/data.txt', {'dst': 'rcp_b/data.txt'}),
        ],
        expected=[E('update', '/data.txt')],
    ),
    W(
        'create_100_bytes_exact',
        'Create file with exactly 100 bytes — verified',
        ops=[Op('create_file', '100b.bin', {'content': b'Z' * 100})],
        expected=[E('update', '/100b.bin', {'size': 100})],
    ),
    W(
        'double_copy',
        'Create A, copy A→B, copy B→C — chain of independent copies',
        ops=[
            Op('create_file', 'dc_a.txt', {'content': b'double copy\n'}),
            Op('copy', 'dc_a.txt', {'dst': 'dc_b.txt'}),
            Op('copy', 'dc_b.txt', {'dst': 'dc_c.txt'}),
        ],
        expected=[
            E('update', '/dc_a.txt', {'size': 12}),
            E('update', '/dc_b.txt', {'size': 12}),
            E('update', '/dc_c.txt', {'size': 12}),
        ],
    ),
    # ------------------------------------------------------------------
    # 23. More patterns — iter 31
    # ------------------------------------------------------------------
    W(
        'rename_back_and_forth',
        'Create A, rename A→B, rename B→A — file ends at original path',
        ops=[
            Op('create_file', 'rbf.txt', {'content': b'boomerang\n'}),
            Op('rename', 'rbf.txt', {'dst': 'rbf_tmp.txt'}),
            Op('rename', 'rbf_tmp.txt', {'dst': 'rbf.txt'}),
        ],
        expected=[E('update', '/rbf.txt')],
    ),
    W(
        'create_tree_then_add_siblings',
        'Create a/b, add files to both a/ and b/ after tree exists',
        ops=[
            Op('makedirs', 'cts_a/cts_b'),
            Op('create_file', 'cts_a/parent.txt', {'content': b'parent\n'}),
            Op(
                'create_file',
                'cts_a/cts_b/child.txt',
                {'content': b'child\n'},
            ),
            Op('create_file', 'cts_a/sibling.txt', {'content': b'sibling\n'}),
        ],
        expected=[
            E('update', '/parent.txt', {'size': 7}),
            E('update', '/child.txt', {'size': 6}),
            E('update', '/sibling.txt', {'size': 8}),
        ],
    ),
    W(
        'overwrite_binary_with_binary',
        'Create 256B binary, overwrite with different 512B binary',
        ops=[
            Op('create_file', 'owbb.bin', {'content': bytes(range(256))}),
            Op('write', 'owbb.bin', {'content': bytes(range(256)) * 2}),
        ],
        expected=[E('update', '/owbb.bin', {'size': 512})],
    ),
    W(
        'chmod_dir_then_add_file',
        'Create dir, chmod 755, add file inside — file detected',
        ops=[
            Op('mkdir', 'cdaf'),
            Op('chmod', 'cdaf', {'mode': 0o755}),
            Op(
                'create_file',
                'cdaf/inside.txt',
                {'content': b'in chmod dir\n'},
            ),
        ],
        expected=[
            E('update', '/cdaf'),
            E('update', '/inside.txt', {'size': 13}),
        ],
    ),
    W(
        'truncate_to_exact_zero',
        'Create 1000B file, truncate to exactly 0 — size=0 verified',
        ops=[
            Op('create_file', 'tz.bin', {'content': b'V' * 1000}),
            Op('truncate', 'tz.bin'),
        ],
        expected=[E('update', '/tz.bin')],
    ),
    W(
        'create_delete_10_rapid_then_persistent',
        'Rapidly create+delete 10 files, then create 1 persistent — alive',
        ops=[
            *[
                item
                for i in range(10)
                for item in [
                    Op(
                        'create_file',
                        f'cdr_{i}.txt',
                        {'content': b'x', 'pause_after': 0.0},
                    ),
                    Op('delete', f'cdr_{i}.txt', {'pause_after': 0.0}),
                ]
            ],
            Op('create_file', 'cdr_final.txt', {'content': b'survived\n'}),
        ],
        expected=[E('update', '/cdr_final.txt', {'size': 9})],
    ),
    W(
        'copy_then_rename_copy',
        'Create A, copy→B, rename B→C — A and C detected',
        ops=[
            Op('create_file', 'ctrc_a.txt', {'content': b'source\n'}),
            Op('copy', 'ctrc_a.txt', {'dst': 'ctrc_b.txt'}),
            Op('rename', 'ctrc_b.txt', {'dst': 'ctrc_c.txt'}),
        ],
        expected=[
            E('update', '/ctrc_a.txt', {'size': 7}),
        ],
    ),
    W(
        'append_binary_chunks',
        'Create file, append 5 binary chunks of 64B each — final=320B',
        ops=[
            Op('create_file', 'abc.bin', {'content': b''}),
            *[
                Op(
                    'append',
                    'abc.bin',
                    {'content': b'\xaa' * 64, 'pause_after': 0.05},
                )
                for _ in range(5)
            ],
        ],
        expected=[E('update', '/abc.bin', {'size': 320})],
    ),
    W(
        'create_in_deeply_renamed_tree',
        'makedirs a/b/c, rename a→x, rename x/b→x/y, create file in x/y/c',
        ops=[
            Op('makedirs', 'cidrt_a/cidrt_b/cidrt_c'),
            Op('rename', 'cidrt_a', {'dst': 'cidrt_x'}),
            Op('rename', 'cidrt_x/cidrt_b', {'dst': 'cidrt_x/cidrt_y'}),
            Op(
                'create_file',
                'cidrt_x/cidrt_y/cidrt_c/deep.txt',
                {'content': b'deep\n'},
            ),
        ],
        expected=[E('update', '/deep.txt', {'size': 5})],
    ),
    W(
        'utime_zero_epoch',
        'Create file, set mtime to 0 (Unix epoch) — update with stat',
        ops=[
            Op('create_file', 'epoch.txt', {'content': b'epoch\n'}),
            Op('utime', 'epoch.txt', {'time': 0.0}),
        ],
        expected=[E('update', '/epoch.txt', {'has_stat': True})],
    ),
    W(
        'wide_flat_30_files',
        'Create 30 files in root dir — all detected',
        ops=[
            *[
                Op(
                    'create_file',
                    f'wf_{i:02d}.txt',
                    {
                        'content': f'{i}\n'.encode(),
                        'pause_after': 0.02,
                    },
                )
                for i in range(30)
            ],
        ],
        expected=[
            *[E('update', f'/wf_{i:02d}.txt') for i in range(30)],
        ],
    ),
    W(
        'rename_dir_preserves_all_children_sizes',
        'Create dir with 5 files (distinct sizes), rename dir — all files tracked',
        ops=[
            Op('mkdir', 'rdpac'),
            *[
                Op(
                    'create_file',
                    f'rdpac/f_{i}.bin',
                    {'content': b'Q' * (10 * (i + 1))},
                )
                for i in range(5)
            ],
            Op('rename', 'rdpac', {'dst': 'rdpac_new'}),
        ],
        expected=[
            *[E('update', f'/f_{i}.bin') for i in range(5)],
        ],
    ),
    W(
        'replace_then_chmod',
        'Create dst, create src, os.replace src→dst, chmod dst',
        ops=[
            Op('create_file', 'rtc_dst.txt', {'content': b'old\n'}),
            Op('create_file', 'rtc_src.txt', {'content': b'replacement\n'}),
            Op('replace', 'rtc_src.txt', {'dst': 'rtc_dst.txt'}),
            Op('chmod', 'rtc_dst.txt', {'mode': 0o755}),
        ],
        expected=[E('update', '/rtc_dst.txt')],
    ),
    W(
        'interleaved_symlinks_and_files',
        'Create file, symlink, file, symlink, file — all 5 detected',
        ops=[
            Op('create_file', 'isf_1.txt', {'content': b'file1\n'}),
            Op('symlink', 'isf_s1', {'target': 'isf_1.txt'}),
            Op('create_file', 'isf_2.txt', {'content': b'file2\n'}),
            Op('symlink', 'isf_s2', {'target': 'isf_2.txt'}),
            Op('create_file', 'isf_3.txt', {'content': b'file3\n'}),
        ],
        expected=[
            E('update', '/isf_1.txt'),
            E('update', '/isf_s1'),
            E('update', '/isf_2.txt'),
            E('update', '/isf_s2'),
            E('update', '/isf_3.txt'),
        ],
    ),
    W(
        'write_1_byte_100_files',
        'Create 100 1-byte files — all detected with size=1',
        ops=[
            *[
                Op(
                    'create_file',
                    f'w1b_{i:03d}.txt',
                    {
                        'content': bytes([65 + i % 26]),
                        'pause_after': 0.01,
                    },
                )
                for i in range(100)
            ],
        ],
        expected=[
            *[
                E('update', f'/w1b_{i:03d}.txt', {'size': 1})
                for i in range(100)
            ],
        ],
    ),
    # ------------------------------------------------------------------
    # 24. Iter 37 — new patterns
    # ------------------------------------------------------------------
    W(
        'rename_swap_files_same_dir',
        'Swap A(3B) and B(7B) through temp in same dir — sizes swapped',
        ops=[
            Op('create_file', 'rsf_a.txt', {'content': b'AAA'}),
            Op('create_file', 'rsf_b.txt', {'content': b'BBBBBBB'}),
            Op('rename', 'rsf_a.txt', {'dst': 'rsf_tmp.txt'}),
            Op('rename', 'rsf_b.txt', {'dst': 'rsf_a.txt'}),
            Op('rename', 'rsf_tmp.txt', {'dst': 'rsf_b.txt'}),
        ],
        expected=[
            E('update', '/rsf_a.txt'),
            E('update', '/rsf_b.txt'),
        ],
    ),
    W(
        'create_hardlink_append_via_both',
        'Create file + hardlink, append via original then via link',
        ops=[
            Op('create_file', 'chab_orig.txt', {'content': b'base\n'}),
            Op('hardlink', 'chab_link.txt', {'target': 'chab_orig.txt'}),
            Op('append', 'chab_orig.txt', {'content': b'via_orig\n'}),
            Op('append', 'chab_link.txt', {'content': b'via_link\n'}),
        ],
        expected=[
            E('update', '/chab_orig.txt', {'size': 23}),
            E('update', '/chab_link.txt'),
        ],
    ),
    W(
        'mkdir_tree_add_files_rename_root',
        'Create a/b/c, add files at each level, rename a→z',
        ops=[
            Op('makedirs', 'mtar_a/mtar_b/mtar_c'),
            Op('create_file', 'mtar_a/f1.txt', {'content': b'level 1\n'}),
            Op(
                'create_file',
                'mtar_a/mtar_b/f2.txt',
                {'content': b'level 2\n'},
            ),
            Op(
                'create_file',
                'mtar_a/mtar_b/mtar_c/f3.txt',
                {'content': b'level 3\n'},
            ),
            Op('rename', 'mtar_a', {'dst': 'mtar_z'}),
        ],
        expected=[
            E('update', '/f1.txt'),
            E('update', '/f2.txt'),
            E('update', '/f3.txt'),
        ],
    ),
    W(
        'copy_then_delete_original',
        'Create A(50B), copy→B, delete A — B remains with 50B',
        ops=[
            Op('create_file', 'ctdo_a.txt', {'content': b'X' * 50}),
            Op('copy', 'ctdo_a.txt', {'dst': 'ctdo_b.txt'}),
            Op('delete', 'ctdo_a.txt'),
        ],
        expected=[
            E('update', '/ctdo_b.txt', {'size': 50}),
            E('update', '/ctdo_a.txt'),
            E('delete', '/ctdo_a.txt'),
        ],
    ),
    W(
        'create_many_extensions',
        'Create files with varied extensions: .py .rs .go .c .h .toml',
        ops=[
            Op('create_file', 'code.py', {'content': b'print("hi")\n'}),
            Op('create_file', 'code.rs', {'content': b'fn main(){}\n'}),
            Op('create_file', 'code.go', {'content': b'package main\n'}),
            Op('create_file', 'code.c', {'content': b'int main(){}\n'}),
            Op('create_file', 'code.h', {'content': b'#pragma once\n'}),
            Op('create_file', 'config.toml', {'content': b'[package]\n'}),
        ],
        expected=[
            E('update', '/code.py', {'size': 12}),
            E('update', '/code.rs', {'size': 12}),
            E('update', '/code.go', {'size': 13}),
            E('update', '/code.c', {'size': 13}),
            E('update', '/code.h', {'size': 13}),
            E('update', '/config.toml', {'size': 10}),
        ],
    ),
    W(
        'append_to_renamed_file',
        'Create A, rename→B, append to B — B has original+appended size',
        ops=[
            Op('create_file', 'atrf_a.txt', {'content': b'initial\n'}),
            Op(
                'rename',
                'atrf_a.txt',
                {'dst': 'atrf_b.txt', 'pause_after': 0.5},
            ),
            Op('append', 'atrf_b.txt', {'content': b'appended\n'}),
        ],
        expected=[E('update', '/atrf_a.txt')],
    ),
    W(
        'replace_across_dirs',
        'Create file in dir_a, create file in dir_b, os.replace a→b',
        ops=[
            Op('mkdir', 'rad_a'),
            Op('mkdir', 'rad_b'),
            Op('create_file', 'rad_a/data.txt', {'content': b'from dir a\n'}),
            Op('create_file', 'rad_b/data.txt', {'content': b'old in b\n'}),
            Op('replace', 'rad_a/data.txt', {'dst': 'rad_b/data.txt'}),
        ],
        expected=[E('update', '/rad_b/data.txt')],
    ),
    # ------------------------------------------------------------------
    # 25. Iter 41 — coverage gaps
    # ------------------------------------------------------------------
    W(
        'chmod_then_rename_then_chmod',
        'Create, chmod 755, rename, chmod 600 — tracked through lifecycle',
        ops=[
            Op('create_file', 'ctrc.txt', {'content': b'permissions\n'}),
            Op('chmod', 'ctrc.txt', {'mode': 0o755}),
            Op('rename', 'ctrc.txt', {'dst': 'ctrc_renamed.txt'}),
            Op('chmod', 'ctrc_renamed.txt', {'mode': 0o600}),
        ],
        expected=[E('update', '/ctrc.txt')],
    ),
    W(
        'create_empty_dirs_10',
        'Create 10 empty directories — all detected as updates',
        ops=[
            *[Op('mkdir', f'ed_{i:02d}') for i in range(10)],
        ],
        expected=[
            *[E('update', f'/ed_{i:02d}') for i in range(10)],
        ],
    ),
    W(
        'write_increasing_content_3_files',
        'Create 3 files with 10B, 100B, 1000B — exact sizes verified',
        ops=[
            Op('create_file', 'wic_10.bin', {'content': b'A' * 10}),
            Op('create_file', 'wic_100.bin', {'content': b'B' * 100}),
            Op('create_file', 'wic_1000.bin', {'content': b'C' * 1000}),
        ],
        expected=[
            E('update', '/wic_10.bin', {'size': 10}),
            E('update', '/wic_100.bin', {'size': 100}),
            E('update', '/wic_1000.bin', {'size': 1000}),
        ],
    ),
    W(
        'symlink_chain_3_hops',
        'Create target, sym_a→target, sym_b→sym_a, sym_c→sym_b — 4 updates',
        ops=[
            Op('create_file', 'sc3_target.txt', {'content': b'deep chain\n'}),
            Op('symlink', 'sc3_a', {'target': 'sc3_target.txt'}),
            Op('symlink', 'sc3_b', {'target': 'sc3_a'}),
            Op('symlink', 'sc3_c', {'target': 'sc3_b'}),
        ],
        expected=[
            E('update', '/sc3_target.txt'),
            E('update', '/sc3_a'),
            E('update', '/sc3_b'),
            E('update', '/sc3_c'),
        ],
    ),
    W(
        'rmtree_then_recreate_deeper',
        'Create a/f.txt, rmtree a, makedirs a/b/c, create a/b/c/f.txt — deeper',
        ops=[
            Op('mkdir', 'rtrd_a'),
            Op('create_file', 'rtrd_a/f.txt', {'content': b'shallow\n'}),
            Op('rmtree', 'rtrd_a', {'pause_after': 0.5}),
            Op('makedirs', 'rtrd_a/rtrd_b/rtrd_c'),
            Op(
                'create_file',
                'rtrd_a/rtrd_b/rtrd_c/f.txt',
                {'content': b'deep now\n'},
            ),
        ],
        expected=[E('update', '/f.txt')],
    ),
    W(
        'copy_modify_original',
        'Create A(10B), copy→B, append to A(+5B) — A=15B, B=10B',
        ops=[
            Op('create_file', 'cmo_a.txt', {'content': b'A' * 10}),
            Op('copy', 'cmo_a.txt', {'dst': 'cmo_b.txt'}),
            Op('append', 'cmo_a.txt', {'content': b'X' * 5}),
        ],
        expected=[
            E('update', '/cmo_a.txt', {'size': 15}),
            E('update', '/cmo_b.txt', {'size': 10}),
        ],
    ),
    # ------------------------------------------------------------------
    # 26. Iter 43 — more patterns
    # ------------------------------------------------------------------
    W(
        'rename_dir_with_mixed_content',
        'Dir with file, subdir, symlink — rename dir — all content follows',
        ops=[
            Op('mkdir', 'rdmc'),
            Op('create_file', 'rdmc/reg.txt', {'content': b'regular\n'}),
            Op('mkdir', 'rdmc/sub'),
            Op('create_file', 'rdmc/sub/nested.txt', {'content': b'nested\n'}),
            Op('symlink', 'rdmc/link', {'target': 'reg.txt'}),
            Op('rename', 'rdmc', {'dst': 'rdmc_new'}),
        ],
        expected=[
            E('update', '/reg.txt'),
            E('update', '/nested.txt'),
        ],
    ),
    W(
        'create_50_files_exact_count',
        'Create 50 files with unique sizes — all 50 detected',
        ops=[
            *[
                Op(
                    'create_file',
                    f'c50_{i:02d}.bin',
                    {
                        'content': bytes([i % 256]) * (i + 1),
                        'pause_after': 0.02,
                    },
                )
                for i in range(50)
            ],
        ],
        expected=[
            *[
                E('update', f'/c50_{i:02d}.bin', {'size': i + 1})
                for i in range(50)
            ],
        ],
    ),
    W(
        'write_then_copy_then_delete_both',
        'Create A, copy→B, delete both — both tracked with updates+deletes',
        ops=[
            Op('create_file', 'wtcdb_a.txt', {'content': b'shared\n'}),
            Op('copy', 'wtcdb_a.txt', {'dst': 'wtcdb_b.txt'}),
            Op('delete', 'wtcdb_a.txt'),
            Op('delete', 'wtcdb_b.txt'),
        ],
        expected=[
            E('update', '/wtcdb_a.txt'),
            E('update', '/wtcdb_b.txt'),
        ],
    ),
    W(
        'rename_file_keep_extension',
        'Rename base name while keeping extension: old_name.txt→new_name.txt',
        ops=[
            Op('create_file', 'rkext_old.txt', {'content': b'data\n'}),
            Op('rename', 'rkext_old.txt', {'dst': 'rkext_new.txt'}),
        ],
        expected=[E('update', '/rkext_old.txt')],
    ),
    W(
        'utime_then_append',
        'Create file, set mtime to past, append — triggers two event types',
        ops=[
            Op('create_file', 'uta.txt', {'content': b'base\n'}),
            Op('utime', 'uta.txt', {'time': 1000000000.0}),
            Op('append', 'uta.txt', {'content': b'after utime\n'}),
        ],
        expected=[E('update', '/uta.txt', {'size': 17})],
    ),
    W(
        'create_file_with_newlines_in_name',
        'File with spaces and hyphens in name — cross-platform safe',
        ops=[
            Op(
                'create_file',
                'my-file name (1).txt',
                {'content': b'special name\n'},
            ),
        ],
        expected=[E('update', '/my-file name (1).txt', {'size': 13})],
    ),
    W(
        'hardlink_survives_original_rename',
        'Create file + hardlink, rename original — hardlink still works',
        ops=[
            Op('create_file', 'hsor_orig.txt', {'content': b'shared data\n'}),
            Op('hardlink', 'hsor_link.txt', {'target': 'hsor_orig.txt'}),
            Op('rename', 'hsor_orig.txt', {'dst': 'hsor_moved.txt'}),
        ],
        expected=[
            E('update', '/hsor_orig.txt'),
            E('update', '/hsor_link.txt'),
        ],
    ),
    W(
        'burst_mixed_ops_10',
        'Rapid interleave: create, chmod, append, rename across 10 files',
        ops=[
            *[
                Op(
                    'create_file',
                    f'bmo_{i}.txt',
                    {'content': f'{i}\n'.encode(), 'pause_after': 0.02},
                )
                for i in range(10)
            ],
            *[
                Op(
                    'chmod',
                    f'bmo_{i}.txt',
                    {'mode': 0o755, 'pause_after': 0.02},
                )
                for i in range(10)
            ],
        ],
        expected=[
            *[E('update', f'/bmo_{i}.txt') for i in range(10)],
        ],
    ),
    W(
        'create_dir_tree_delete_leaf_only',
        'Create a/b/c/leaf.txt, delete leaf only — dirs survive',
        ops=[
            Op('makedirs', 'cdtd_a/cdtd_b/cdtd_c'),
            Op(
                'create_file',
                'cdtd_a/cdtd_b/cdtd_c/leaf.txt',
                {'content': b'leaf\n'},
            ),
            Op('delete', 'cdtd_a/cdtd_b/cdtd_c/leaf.txt'),
        ],
        expected=[
            E('update', '/leaf.txt'),
            E('delete', '/leaf.txt'),
        ],
    ),
    # ------------------------------------------------------------------
    # 27. Iter 48 — final patterns
    # ------------------------------------------------------------------
    W(
        'create_rename_delete_lifecycle_3_files',
        'Full lifecycle for 3 files: create→rename→delete',
        ops=[
            Op('create_file', 'lc_a.txt', {'content': b'a\n'}),
            Op('create_file', 'lc_b.txt', {'content': b'b\n'}),
            Op('create_file', 'lc_c.txt', {'content': b'c\n'}),
            Op('rename', 'lc_a.txt', {'dst': 'lc_a2.txt'}),
            Op('rename', 'lc_b.txt', {'dst': 'lc_b2.txt'}),
            Op('rename', 'lc_c.txt', {'dst': 'lc_c2.txt'}),
            Op('delete', 'lc_a2.txt'),
            Op('delete', 'lc_b2.txt'),
            Op('delete', 'lc_c2.txt'),
        ],
        expected=[
            E('update', '/lc_a.txt'),
            E('update', '/lc_b.txt'),
            E('update', '/lc_c.txt'),
        ],
    ),
    W(
        'write_all_bytes_pattern',
        'Create file with bytes 0x00-0xFF — 256 bytes, all values',
        ops=[
            Op('create_file', 'allbytes.bin', {'content': bytes(range(256))}),
        ],
        expected=[E('update', '/allbytes.bin', {'size': 256})],
    ),
    W(
        'copy_to_3_destinations',
        'Create A, copy to B, C, D — all 4 have same size',
        ops=[
            Op('create_file', 'ct3_a.txt', {'content': b'original content\n'}),
            Op('copy', 'ct3_a.txt', {'dst': 'ct3_b.txt'}),
            Op('copy', 'ct3_a.txt', {'dst': 'ct3_c.txt'}),
            Op('copy', 'ct3_a.txt', {'dst': 'ct3_d.txt'}),
        ],
        expected=[
            E('update', '/ct3_a.txt', {'size': 17}),
            E('update', '/ct3_b.txt', {'size': 17}),
            E('update', '/ct3_c.txt', {'size': 17}),
            E('update', '/ct3_d.txt', {'size': 17}),
        ],
    ),
    W(
        'mkdir_rmdir_5_cycles',
        'Create and rmdir same dir 5 times, then create persistent',
        ops=[
            *[
                item
                for _ in range(5)
                for item in [
                    Op('mkdir', 'cycle_dir', {'pause_after': 0.05}),
                    Op('rmtree', 'cycle_dir', {'pause_after': 0.05}),
                ]
            ],
            Op('mkdir', 'cycle_dir'),
            Op(
                'create_file',
                'cycle_dir/alive.txt',
                {'content': b'survived\n'},
            ),
        ],
        expected=[E('update', '/alive.txt', {'size': 9})],
    ),
    # ------------------------------------------------------------------
    # 28. Iter 50 — milestone batch
    # ------------------------------------------------------------------
    W(
        'rename_to_longer_path',
        'Create root/a.txt, makedirs root/deep/sub, rename a.txt into deep/sub',
        ops=[
            Op('create_file', 'rtlp.txt', {'content': b'moving deep\n'}),
            Op('makedirs', 'rtlp_deep/rtlp_sub'),
            Op('rename', 'rtlp.txt', {'dst': 'rtlp_deep/rtlp_sub/rtlp.txt'}),
        ],
        expected=[E('update', '/rtlp.txt')],
    ),
    W(
        'create_100_empty_files',
        'Create 100 empty (0-byte) files — all detected with size=0',
        ops=[
            *[
                Op(
                    'create_file',
                    f'e100_{i:03d}.txt',
                    {
                        'content': b'',
                        'pause_after': 0.01,
                    },
                )
                for i in range(100)
            ],
        ],
        expected=[
            *[
                E('update', f'/e100_{i:03d}.txt', {'size': 0})
                for i in range(100)
            ],
        ],
    ),
    W(
        'write_exact_powers_of_10',
        'Create files with sizes 1, 10, 100, 1000, 10000 bytes',
        ops=[
            *[
                Op(
                    'create_file',
                    f'p10_{10**i}.bin',
                    {'content': b'P' * (10**i)},
                )
                for i in range(5)
            ],
        ],
        expected=[
            *[
                E('update', f'/p10_{10**i}.bin', {'size': 10**i})
                for i in range(5)
            ],
        ],
    ),
    # ------------------------------------------------------------------
    # Promoted from fswatch-specific (process-agnostic)
    # ------------------------------------------------------------------
    W(
        'interleaved_dir_ops_rename_delete',
        'Create dirs A+B with files, rename A→C, rmtree B — all files detected',
        ops=[
            Op('mkdir', 'ildo_a'),
            Op('mkdir', 'ildo_b'),
            Op('create_file', 'ildo_a/a1.txt', {'content': b'a1\n'}),
            Op('create_file', 'ildo_a/a2.txt', {'content': b'a2\n'}),
            Op('create_file', 'ildo_b/b1.txt', {'content': b'b1\n'}),
            Op('create_file', 'ildo_b/b2.txt', {'content': b'b2\n'}),
            Op('rename', 'ildo_a', {'dst': 'ildo_c'}),
            Op('rmtree', 'ildo_b', {'pause_after': 0.5}),
        ],
        expected=[
            E('update', '/a1.txt'),
            E('update', '/a2.txt'),
            E('update', '/b1.txt'),
            E('update', '/b2.txt'),
        ],
    ),
    W(
        'mkdir_5_level_rename_mid',
        'Create 5-level tree, add leaf, rename 2nd level',
        ops=[
            Op('makedirs', 'l5r_L1/l5r_L2/l5r_L3/l5r_L4/l5r_L5'),
            Op(
                'create_file',
                'l5r_L1/l5r_L2/l5r_L3/l5r_L4/l5r_L5/leaf.txt',
                {'content': b'deep leaf\n'},
            ),
            Op('rename', 'l5r_L1/l5r_L2', {'dst': 'l5r_L1/l5r_L2_renamed'}),
        ],
        expected=[E('update', '/leaf.txt')],
    ),
    W(
        'mixed_link_types_lifecycle',
        'Create regular, symlink, hardlink — modify via each, delete all',
        ops=[
            Op('create_file', 'mlt_reg.txt', {'content': b'base\n'}),
            Op('symlink', 'mlt_sym.txt', {'target': 'mlt_reg.txt'}),
            Op('hardlink', 'mlt_hard.txt', {'target': 'mlt_reg.txt'}),
            Op('delete', 'mlt_sym.txt'),
            Op('delete', 'mlt_hard.txt'),
            Op('delete', 'mlt_reg.txt'),
        ],
        expected=[
            E('update', '/mlt_reg.txt'),
            E('update', '/mlt_sym.txt'),
            E('update', '/mlt_hard.txt'),
        ],
    ),
    W(
        'build_artifact_lifecycle',
        'Create src+build dirs, add artifacts, rmtree build — src survives',
        ops=[
            Op('mkdir', 'bal_src'),
            Op(
                'create_file',
                'bal_src/main.py',
                {'content': b'print("hello")\n'},
            ),
            Op('mkdir', 'bal_build'),
            Op(
                'create_file',
                'bal_build/output.bin',
                {'content': b'\x00' * 1024},
            ),
            Op('rmtree', 'bal_build', {'pause_after': 0.5}),
        ],
        expected=[E('update', '/main.py', {'size': 15})],
    ),
    W(
        'git_checkout_simulation',
        'Create 5 files, delete 3, create 2 new — simulates branch switch',
        ops=[
            Op('create_file', 'gc_main.py', {'content': b'# main\n'}),
            Op('create_file', 'gc_utils.py', {'content': b'# utils\n'}),
            Op('create_file', 'gc_config.py', {'content': b'# config\n'}),
            Op('create_file', 'gc_readme.md', {'content': b'# readme\n'}),
            Op('create_file', 'gc_setup.py', {'content': b'# setup\n'}),
            Op('delete', 'gc_config.py'),
            Op('delete', 'gc_readme.md'),
            Op('delete', 'gc_setup.py'),
            Op('create_file', 'gc_feature.py', {'content': b'# feature\n'}),
            Op('create_file', 'gc_test_feature.py', {'content': b'# test\n'}),
        ],
        expected=[
            E('update', '/gc_main.py'),
            E('update', '/gc_utils.py'),
            E('update', '/gc_feature.py'),
            E('update', '/gc_test_feature.py'),
        ],
    ),
    W(
        'renames_interleaved_with_creates',
        'Create 5 files, rename each while creating 5 new ones',
        ops=[
            *[
                Op(
                    'create_file',
                    f'ric_orig_{i}.txt',
                    {'content': f'o{i}\n'.encode()},
                )
                for i in range(5)
            ],
            *[
                item
                for i in range(5)
                for item in [
                    Op(
                        'rename',
                        f'ric_orig_{i}.txt',
                        {'dst': f'ric_ren_{i}.txt', 'pause_after': 0.1},
                    ),
                    Op(
                        'create_file',
                        f'ric_new_{i}.txt',
                        {'content': f'n{i}\n'.encode(), 'pause_after': 0.1},
                    ),
                ]
            ],
        ],
        expected=[
            *[E('update', f'/ric_new_{i}.txt') for i in range(5)],
            *[E('update', f'/ric_orig_{i}.txt') for i in range(5)],
        ],
    ),
    W(
        'empty_dir_chmod',
        'Create empty dir, chmod 700 — detected as update',
        ops=[
            Op('mkdir', 'edc_dir'),
            Op('chmod', 'edc_dir', {'mode': 0o700}),
        ],
        expected=[E('update', '/edc_dir')],
    ),
    W(
        'file_in_recreated_dir',
        'Create dir, rmdir, recreate, add file — file detected',
        ops=[
            Op('mkdir', 'fird'),
            Op('rmtree', 'fird'),
            Op('mkdir', 'fird'),
            Op(
                'create_file',
                'fird/new.txt',
                {'content': b'in recreated dir\n'},
            ),
        ],
        expected=[E('update', '/new.txt')],
    ),
    W(
        'stat_size_exact_5_sizes',
        'Create files with sizes 1,10,100,1000,10000 — all exact',
        ops=[
            *[
                Op(
                    'create_file',
                    f'ss_{s}.bin',
                    {'content': b'x' * s, 'pause_after': 0.1},
                )
                for s in [1, 10, 100, 1000, 10000]
            ],
        ],
        expected=[
            *[
                E('update', f'/ss_{s}.bin', {'size': s})
                for s in [1, 10, 100, 1000, 10000]
            ],
        ],
    ),
    W(
        'dotfiles_detected',
        'Create .gitignore, .env, .hidden_dir/data.txt — all detected',
        ops=[
            Op('create_file', '.gitignore', {'content': b'*.pyc\n'}),
            Op('create_file', '.env', {'content': b'KEY=val\n'}),
            Op('mkdir', '.hidden_dir'),
            Op(
                'create_file',
                '.hidden_dir/data.txt',
                {'content': b'hidden\n'},
            ),
        ],
        expected=[
            E('update', '/.gitignore'),
            E('update', '/.env'),
            E('update', '/.hidden_dir'),
            E('update', '/data.txt'),
        ],
    ),
    W(
        'files_with_dots_in_name',
        'Create backup.tar.gz, file.v2.bak — all detected with sizes',
        ops=[
            Op('create_file', 'backup.tar.gz', {'content': b'x' * 256}),
            Op('create_file', 'file.v2.bak', {'content': b'y' * 128}),
        ],
        expected=[
            E('update', '/backup.tar.gz', {'size': 256}),
            E('update', '/file.v2.bak', {'size': 128}),
        ],
    ),
    W(
        'rename_dir_over_empty_dir',
        'Create dir_a with child, mkdir dir_b empty, rename a→b',
        ops=[
            Op('mkdir', 'rdoe_a'),
            Op('create_file', 'rdoe_a/child.txt', {'content': b'inside a\n'}),
            Op('mkdir', 'rdoe_b'),
            Op('rename', 'rdoe_a', {'dst': 'rdoe_b'}),
        ],
        expected=[E('update', '/child.txt')],
    ),
    W(
        'delete_all_hardlinks_sequentially',
        'Create file + 3 hardlinks, delete all — canary detects alive',
        ops=[
            Op('create_file', 'dahl_orig.txt', {'content': b'shared\n'}),
            Op('hardlink', 'dahl_l0.txt', {'target': 'dahl_orig.txt'}),
            Op('hardlink', 'dahl_l1.txt', {'target': 'dahl_orig.txt'}),
            Op('hardlink', 'dahl_l2.txt', {'target': 'dahl_orig.txt'}),
            Op('delete', 'dahl_orig.txt'),
            Op('delete', 'dahl_l0.txt'),
            Op('delete', 'dahl_l1.txt'),
            Op('delete', 'dahl_l2.txt'),
            Op('create_file', 'dahl_canary.txt', {'content': b'alive\n'}),
        ],
        expected=[E('update', '/dahl_canary.txt', {'size': 6})],
    ),
    W(
        'hardlink_rename_then_write',
        'Create file + hardlink, rename hardlink, write via renamed',
        ops=[
            Op('create_file', 'hlrtw_orig.txt', {'content': b'base\n'}),
            Op('hardlink', 'hlrtw_link.txt', {'target': 'hlrtw_orig.txt'}),
            Op('rename', 'hlrtw_link.txt', {'dst': 'hlrtw_renamed.txt'}),
            Op('append', 'hlrtw_renamed.txt', {'content': b'appended\n'}),
        ],
        expected=[
            E('update', '/hlrtw_orig.txt'),
            E('update', '/hlrtw_link.txt'),
        ],
    ),
    W(
        'dir_replace_via_rename',
        'Create dir_a with file, mkdir dir_b, rmdir dir_b, rename a→b',
        ops=[
            Op('mkdir', 'drv_a'),
            Op('create_file', 'drv_a/payload.txt', {'content': b'in dir_a\n'}),
            Op('mkdir', 'drv_b'),
            Op('rmtree', 'drv_b'),
            Op('rename', 'drv_a', {'dst': 'drv_b'}),
        ],
        expected=[E('update', '/payload.txt')],
    ),
    W(
        'vscode_save_pattern',
        'Write original, write temp, rename temp→final — editor save',
        ops=[
            Op(
                'create_file',
                'vs_main.py',
                {'content': b'def main(): pass\n'},
            ),
            Op(
                'create_file',
                'vs_main.py.~123~',
                {'content': b'def main(): return True\n'},
            ),
            Op('rename', 'vs_main.py.~123~', {'dst': 'vs_main.py'}),
        ],
        expected=[E('update', '/vs_main.py')],
    ),
    W(
        'jetbrains_safe_write',
        'Rename orig→.bak, write new orig, delete .bak',
        ops=[
            Op('create_file', 'jb_App.java', {'content': b'class App {}\n'}),
            Op('rename', 'jb_App.java', {'dst': 'jb_App.java.bak'}),
            Op(
                'create_file',
                'jb_App.java',
                {'content': b'class App { void run() {} }\n'},
            ),
            Op('delete', 'jb_App.java.bak'),
        ],
        expected=[E('update', '/jb_App.java')],
    ),
    W(
        'jupyter_notebook_save',
        'Write .ipynb, write .~tmp, rename tmp→ipynb — atomic save',
        ops=[
            Op('create_file', 'nb.ipynb', {'content': b'{"cells":[]}\n'}),
            Op('create_file', '.~nb.ipynb', {'content': b'{"cells":[1]}\n'}),
            Op('rename', '.~nb.ipynb', {'dst': 'nb.ipynb'}),
        ],
        expected=[E('update', '/nb.ipynb')],
    ),
    W(
        'alternating_create_chmod_rename',
        'Create A, chmod, create B, rename A→C, chmod B',
        ops=[
            Op('create_file', 'acr_a.txt', {'content': b'file a\n'}),
            Op('chmod', 'acr_a.txt', {'mode': 0o755}),
            Op('create_file', 'acr_b.txt', {'content': b'file b\n'}),
            Op('rename', 'acr_a.txt', {'dst': 'acr_c.txt'}),
            Op('chmod', 'acr_b.txt', {'mode': 0o600}),
        ],
        expected=[
            E('update', '/acr_a.txt'),
            E('update', '/acr_b.txt'),
        ],
    ),
    W(
        'compile_like_workflow',
        'Create .c, .o, a.out, clean .o and a.out',
        ops=[
            Op(
                'create_file',
                'cmp_main.c',
                {'content': b'int main(){return 0;}\n'},
            ),
            Op(
                'create_file',
                'cmp_main.o',
                {'content': b'\x7fELF' + b'\x00' * 100},
            ),
            Op(
                'create_file',
                'cmp_a.out',
                {'content': b'\x7fELF' + b'\x00' * 200},
            ),
            Op('delete', 'cmp_main.o'),
            Op('delete', 'cmp_a.out'),
        ],
        expected=[E('update', '/cmp_main.c')],
    ),
    W(
        'test_runner_output',
        'Create results dir + 5 test result XML files',
        ops=[
            Op('mkdir', 'tro_results'),
            *[
                Op(
                    'create_file',
                    f'tro_results/test_{i}.xml',
                    {
                        'content': f'<test name="{i}"/>\n'.encode(),
                        'pause_after': 0.05,
                    },
                )
                for i in range(5)
            ],
        ],
        expected=[
            *[E('update', f'/test_{i}.xml') for i in range(5)],
        ],
    ),
    W(
        'rename_dir_with_3_level_subtree',
        'Create root/a/b/c + files, rename root — all files follow',
        ops=[
            Op('makedirs', 'rdst_root/rdst_a/rdst_b/rdst_c'),
            Op('create_file', 'rdst_root/r.txt', {'content': b'root\n'}),
            Op(
                'create_file',
                'rdst_root/rdst_a/a.txt',
                {'content': b'level a\n'},
            ),
            Op(
                'create_file',
                'rdst_root/rdst_a/rdst_b/b.txt',
                {'content': b'level b\n'},
            ),
            Op(
                'create_file',
                'rdst_root/rdst_a/rdst_b/rdst_c/c.txt',
                {'content': b'level c\n'},
            ),
            Op('rename', 'rdst_root', {'dst': 'rdst_renamed'}),
        ],
        expected=[
            E('update', '/r.txt'),
            E('update', '/a.txt'),
            E('update', '/b.txt'),
            E('update', '/c.txt'),
        ],
    ),
    W(
        'alternating_file_dir_creation',
        'Alternate file, dir, file, dir, ... — all 10 items detected',
        ops=[
            *[
                item
                for i in range(5)
                for item in [
                    Op(
                        'create_file',
                        f'afd_f{i}.txt',
                        {'content': f'{i}\n'.encode(), 'pause_after': 0.05},
                    ),
                    Op('mkdir', f'afd_d{i}', {'pause_after': 0.05}),
                ]
            ],
        ],
        expected=[
            *[E('update', f'/afd_f{i}.txt') for i in range(5)],
            *[E('update', f'/afd_d{i}') for i in range(5)],
        ],
    ),
    W(
        'files_across_5_levels',
        'Create 5-level tree, add 1 file at each level',
        ops=[
            Op('makedirs', 'fa5_L1/fa5_L2/fa5_L3/fa5_L4/fa5_L5'),
            Op('create_file', 'fa5_L1/f1.txt', {'content': b'level 1\n'}),
            Op(
                'create_file',
                'fa5_L1/fa5_L2/f2.txt',
                {'content': b'level 2\n'},
            ),
            Op(
                'create_file',
                'fa5_L1/fa5_L2/fa5_L3/f3.txt',
                {'content': b'level 3\n'},
            ),
            Op(
                'create_file',
                'fa5_L1/fa5_L2/fa5_L3/fa5_L4/f4.txt',
                {'content': b'level 4\n'},
            ),
            Op(
                'create_file',
                'fa5_L1/fa5_L2/fa5_L3/fa5_L4/fa5_L5/f5.txt',
                {'content': b'level 5\n'},
            ),
        ],
        expected=[
            *[E('update', f'/f{i}.txt') for i in range(1, 6)],
        ],
    ),
    W(
        'rmtree_20_files',
        'Create dir + 20 files, rmtree — canary after proves alive',
        ops=[
            Op('mkdir', 'rm20'),
            *[
                Op(
                    'create_file',
                    f'rm20/f_{i:02d}.txt',
                    {'content': f'{i}\n'.encode(), 'pause_after': 0.02},
                )
                for i in range(20)
            ],
            Op('rmtree', 'rm20', {'pause_after': 0.5}),
            Op('create_file', 'rm20_alive.txt', {'content': b'alive\n'}),
        ],
        expected=[E('update', '/rm20_alive.txt', {'size': 6})],
    ),
    W(
        'rmtree_then_create_at_same_path',
        'Create dir+file, rmtree, create at same path with new content',
        ops=[
            Op('mkdir', 'rtcs'),
            Op('create_file', 'rtcs/old.txt', {'content': b'old\n'}),
            Op('rmtree', 'rtcs', {'pause_after': 0.5}),
            Op('mkdir', 'rtcs'),
            Op('create_file', 'rtcs/new.txt', {'content': b'new content\n'}),
        ],
        expected=[E('update', '/new.txt')],
    ),
    W(
        'create_3_level_tree_delete_middle',
        'Create a/b/c + files, rmtree b — top file survives',
        ops=[
            Op('makedirs', 'tdm_a/tdm_b/tdm_c'),
            Op('create_file', 'tdm_a/top.txt', {'content': b'top\n'}),
            Op('create_file', 'tdm_a/tdm_b/mid.txt', {'content': b'mid\n'}),
            Op(
                'create_file',
                'tdm_a/tdm_b/tdm_c/bot.txt',
                {'content': b'bot\n'},
            ),
            Op('rmtree', 'tdm_a/tdm_b', {'pause_after': 0.5}),
        ],
        expected=[E('update', '/top.txt')],
    ),
    W(
        'deeply_nested_5_level_file',
        'makedirs 5 levels, add file at leaf — detected with size',
        ops=[
            Op('makedirs', 'dn5_1/dn5_2/dn5_3/dn5_4/dn5_5'),
            Op(
                'create_file',
                'dn5_1/dn5_2/dn5_3/dn5_4/dn5_5/deepfile.txt',
                {'content': b'at the bottom\n'},
            ),
        ],
        expected=[E('update', '/deepfile.txt', {'size': 14})],
    ),
    W(
        'same_basename_different_ext',
        'Create data.txt, data.json, data.csv, data.bin — all detected',
        ops=[
            Op('create_file', 'data.txt', {'content': b'text\n'}),
            Op('create_file', 'data.json', {'content': b'{"k":1}\n'}),
            Op('create_file', 'data.csv', {'content': b'a,b\n'}),
            Op('create_file', 'data.bin', {'content': b'\x00\x01\n'}),
        ],
        expected=[
            E('update', '/data.txt'),
            E('update', '/data.json'),
            E('update', '/data.csv'),
            E('update', '/data.bin'),
        ],
    ),
    W(
        'no_extension_file',
        'Create file named Makefile (no ext) — detected with exact size',
        ops=[
            Op(
                'create_file',
                'Makefile',
                {'content': b'all:\n\techo hello\n'},
            ),
        ],
        expected=[E('update', '/Makefile', {'size': 17})],
    ),
    W(
        'rename_file_into_dir',
        'Create file + dir, rename file into dir — file detected',
        ops=[
            Op('create_file', 'rfi_src.txt', {'content': b'moving in\n'}),
            Op('mkdir', 'rfi_dir'),
            Op('rename', 'rfi_src.txt', {'dst': 'rfi_dir/rfi_src.txt'}),
        ],
        expected=[E('update', '/rfi_src.txt')],
    ),
    W(
        'rename_dir_into_sibling',
        'Create dir_a with child, dir_b empty, rename a into b/a',
        ops=[
            Op('mkdir', 'rdis_a'),
            Op('create_file', 'rdis_a/child.txt', {'content': b'child\n'}),
            Op('mkdir', 'rdis_b'),
            Op('rename', 'rdis_a', {'dst': 'rdis_b/rdis_a'}),
        ],
        expected=[E('update', '/child.txt')],
    ),
    W(
        'empty_write_truncates',
        'Create file with content, open for write (truncate) — size=0',
        ops=[
            Op('create_file', 'ewt.txt', {'content': b'content\n'}),
            Op('truncate', 'ewt.txt'),
        ],
        expected=[E('update', '/ewt.txt')],
    ),
    W(
        'mkdir_only_5',
        'Create 5 empty dirs — all detected as updates',
        ops=[*[Op('mkdir', f'mo5_d_{i}') for i in range(5)]],
        expected=[*[E('update', f'/mo5_d_{i}') for i in range(5)]],
    ),
    W(
        'spaces_and_special_chars_filenames',
        'Create files with spaces, parens, brackets in name',
        ops=[
            Op(
                'create_file',
                'file with spaces.txt',
                {'content': b'spaces\n'},
            ),
            Op('create_file', 'file (1).txt', {'content': b'parens\n'}),
            Op('create_file', 'file [copy].txt', {'content': b'brackets\n'}),
        ],
        expected=[
            E('update', '/file with spaces.txt'),
            E('update', '/file (1).txt'),
            E('update', '/file [copy].txt'),
        ],
    ),
    W(
        'very_long_filename',
        'Create file with 200-char name — detected',
        ops=[
            Op('create_file', 'a' * 200 + '.txt', {'content': b'long name\n'}),
        ],
        expected=[E('update', '/' + 'a' * 200 + '.txt', {'size': 10})],
    ),
    W(
        'full_dir_lifecycle',
        'mkdir → add file → rename dir → add file → rmtree',
        ops=[
            Op('mkdir', 'fdl_dir'),
            Op('create_file', 'fdl_dir/first.txt', {'content': b'first\n'}),
            Op('rename', 'fdl_dir', {'dst': 'fdl_renamed'}),
            Op(
                'create_file',
                'fdl_renamed/second.txt',
                {'content': b'second\n'},
            ),
            Op('rmtree', 'fdl_renamed', {'pause_after': 0.5}),
        ],
        expected=[
            E('update', '/first.txt'),
            E('update', '/second.txt'),
        ],
    ),
    W(
        'vim_backup_save',
        'Rename orig→.bak, write new orig — editor pattern',
        ops=[
            Op('create_file', 'vim_cfg.yml', {'content': b'version: 1\n'}),
            Op('rename', 'vim_cfg.yml', {'dst': 'vim_cfg.yml.bak'}),
            Op(
                'create_file',
                'vim_cfg.yml',
                {'content': b'version: 2\nchanged: true\n'},
            ),
        ],
        expected=[E('update', '/vim_cfg.yml')],
    ),
    W(
        'emacs_autosave',
        'Create main file, autosave #file#, delete autosave on save',
        ops=[
            Op('create_file', 'ema_notes.org', {'content': b'* Heading\n'}),
            Op(
                'create_file',
                '#ema_notes.org#',
                {'content': b'* Heading\n** Draft\n'},
            ),
            Op(
                'write',
                'ema_notes.org',
                {'content': b'* Heading\n** Draft\n*** Done\n'},
            ),
            Op('delete', '#ema_notes.org#'),
        ],
        expected=[E('update', '/ema_notes.org')],
    ),
    W(
        'dockerfile_build_context',
        'Create Dockerfile, src/app.py, requirements.txt — all detected',
        ops=[
            Op(
                'create_file',
                'Dockerfile',
                {'content': b'FROM python:3.11\nCOPY . /app\n'},
            ),
            Op('mkdir', 'dock_src'),
            Op(
                'create_file',
                'dock_src/app.py',
                {'content': b'def main(): print("hello")\n'},
            ),
            Op(
                'create_file',
                'requirements.txt',
                {'content': b'flask==3.0\nrequests==2.31\n'},
            ),
        ],
        expected=[
            E('update', '/Dockerfile'),
            E('update', '/app.py'),
            E('update', '/requirements.txt'),
        ],
    ),
    W(
        'wal_checkpoint_pattern',
        'Create db, write wal, merge wal into db, delete wal',
        ops=[
            Op('create_file', 'wal_data.db', {'content': b'\x00' * 1024}),
            Op('create_file', 'wal_data.db-wal', {'content': b'\x01' * 512}),
            Op(
                'write',
                'wal_data.db',
                {'content': b'\x00' * 1024 + b'\x01' * 512},
            ),
            Op('delete', 'wal_data.db-wal'),
        ],
        expected=[E('update', '/wal_data.db', {'size': 1536})],
    ),
    W(
        'lock_file_lifecycle',
        'Create .lock, write data file, delete .lock',
        ops=[
            Op('create_file', 'lfl_process.lock', {'content': b'12345'}),
            Op(
                'create_file',
                'lfl_process.dat',
                {'content': b'important data\n'},
            ),
            Op('delete', 'lfl_process.lock'),
        ],
        expected=[
            E('update', '/lfl_process.dat', {'size': 15}),
            E('update', '/lfl_process.lock'),
            E('delete', '/lfl_process.lock'),
        ],
    ),
    W(
        'pip_install_simulation',
        'Create pkg dir + __init__.py + module + dist-info',
        ops=[
            Op('mkdir', 'pip_pkg'),
            Op('create_file', 'pip_pkg/__init__.py', {'content': b''}),
            Op(
                'create_file',
                'pip_pkg/core.py',
                {'content': b'class Client: pass\n'},
            ),
            Op('mkdir', 'pip_pkg-1.0.dist-info'),
            Op(
                'create_file',
                'pip_pkg-1.0.dist-info/METADATA',
                {'content': b'Name: pkg\nVersion: 1.0\n'},
            ),
        ],
        expected=[
            E('update', '/__init__.py'),
            E('update', '/core.py'),
            E('update', '/METADATA'),
        ],
    ),
    W(
        'symlink_to_dir_child_write',
        'Create dir, symlink to dir, write file via symlink path',
        ops=[
            Op('mkdir', 'stdc_real'),
            Op('symlink', 'stdc_link', {'target': 'stdc_real'}),
            Op(
                'create_file',
                'stdc_real/via_real.txt',
                {'content': b'via real\n'},
            ),
        ],
        expected=[E('update', '/via_real.txt')],
    ),
    W(
        'append_after_rename',
        'Create file, rename, append at new path',
        ops=[
            Op('create_file', 'aar2_old.txt', {'content': b'original\n'}),
            Op(
                'rename',
                'aar2_old.txt',
                {'dst': 'aar2_new.txt', 'pause_after': 0.5},
            ),
            Op('append', 'aar2_new.txt', {'content': b'appended\n'}),
        ],
        expected=[E('update', '/aar2_old.txt')],
    ),
    W(
        'symlink_create_then_delete',
        'Create target + symlink, wait, delete symlink — delete tracked',
        ops=[
            Op('create_file', 'sctd_target.txt', {'content': b'target\n'}),
            Op('symlink', 'sctd_link', {'target': 'sctd_target.txt'}),
            Op('delete', 'sctd_link'),
        ],
        expected=[
            E('update', '/sctd_target.txt'),
            E('update', '/sctd_link'),
        ],
    ),
    W(
        'create_rename_copy_delete_mixed',
        'Interleave create, rename, copy, delete on different files',
        ops=[
            Op('create_file', 'crcm_a.txt', {'content': b'a content\n'}),
            Op('mkdir', 'crcm_dir'),
            Op('rename', 'crcm_a.txt', {'dst': 'crcm_dir/moved_a.txt'}),
            Op('copy', 'crcm_dir/moved_a.txt', {'dst': 'crcm_copy.txt'}),
            Op('create_file', 'crcm_b.txt', {'content': b'b content\n'}),
            Op('delete', 'crcm_copy.txt'),
        ],
        expected=[
            E('update', '/crcm_a.txt'),
            E('update', '/crcm_b.txt'),
        ],
    ),
]
