"""Secondary pipeline aggregation: map, reduce, and prefix workers."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterator
from typing import Any

from conf import CMode
from conf import get_agg_file_path
from conf import IngestConfig
from constants import derive_worker_id
from prefix import get_prefix
from pyflink.datastream import FlatMapFunction
from pyflink.datastream import KeyedProcessFunction
from sketches import calc_file_stats
from sketches import calc_time_stats
from sketches import decode_sketch
from sketches import encode_sketch
from sketches import init_metadata_entry


class GetPrefixWorkerCols(FlatMapFunction):
    """Emit (prefix, worker, size, atime, ctime, mtime) tuples."""

    def __init__(
        self,
        prefix_min: int,
        prefix_max: int,
    ) -> None:
        self.prefix_min = prefix_min
        self.prefix_max = prefix_max

    def flat_map(
        self,
        value: tuple[str, str, str, int, int, int, int, int],
    ) -> Iterator[tuple[str, int, int, int, int, int]]:
        """Expand each row into per-prefix tuples."""
        _, _, path, base_worker_id, size, atime, ctime, mtime = value
        for depth in range(self.prefix_min, self.prefix_max):
            if prefix := get_prefix(path, depth):
                wid = derive_worker_id(base_worker_id, depth)
                yield prefix, wid, size, atime, ctime, mtime


class SecondaryMap(KeyedProcessFunction):
    """Accumulate per-worker sketch entries for secondary index."""

    def __init__(
        self,
        mode: CMode,
        counts: dict[tuple[str, int], int],
    ) -> None:
        self.mode = mode
        self.counts = counts
        self.items: dict[tuple[str, int], Any] = {}

    def _update_entry(
        self,
        key: tuple[str, int],
        size: int,
        atime: int,
        ctime: int,
        mtime: int,
    ) -> None:
        entry = self.items[key]
        add_fn = 'add' if self.mode == CMode.dd else 'update'
        for idx, val in enumerate((size, atime, ctime, mtime)):
            getattr(entry[idx][0], add_fn)(val)
            entry[idx][1] = min(entry[idx][1], val)
            entry[idx][2] = max(entry[idx][2], val)
            entry[idx][3] += val
        entry[4] += 1
        self.items[key] = entry

    def _serialize_entry(
        self,
        key: tuple[str, int],
    ) -> dict[str, Any]:
        entry = self.items.pop(key)
        metric_names = ('size', 'atime', 'ctime', 'mtime')
        result: dict[str, Any] = {'count': entry[4]}
        for idx, name in enumerate(metric_names):
            sk, mn, mx, total = entry[idx]
            result[name] = [
                encode_sketch(self.mode, sk),
                mn,
                mx,
                total,
            ]
        return result

    def process_element(
        self,
        value: tuple[Any, ...],
        ctx: KeyedProcessFunction.Context,
    ) -> Iterator[tuple[str, str]]:
        """Accumulate metrics; yield serialized entry when complete."""
        name, worker, size, atime, ctime, mtime = value
        key = (name, worker)

        if key not in self.items:
            self.items[key] = init_metadata_entry(self.mode)

        self._update_entry(key, size, atime, ctime, mtime)

        if self.counts[key] == self.items[key][4]:
            result = self._serialize_entry(key)
            yield (key[0], json.dumps(result))


_GMETA_CONFIG: dict[str, tuple[str, str]] = {
    'u': ('user_id', 'user_id::'),
    'g': ('group_id', 'group_id::'),
}


class SecondaryReduce(KeyedProcessFunction):
    """Merge worker-level entries and produce final gmeta JSON."""

    def __init__(
        self,
        mode: CMode,
        search_group_urns: list[str],
        counts: dict[str, int],
    ) -> None:
        self.mode = mode
        self.search_group_urns = search_group_urns
        self.counts = counts
        self.items: dict[str, Any] = {}

    def _merge_entry(
        self,
        key: str,
        data: dict[str, Any],
    ) -> None:
        entry = self.items[key]
        for idx, metric_name in enumerate(
            ('size', 'atime', 'ctime', 'mtime'),
        ):
            serialized, mn, mx, total = data[metric_name]
            sketch = decode_sketch(self.mode, serialized)
            entry[idx][0].merge(sketch)
            entry[idx][1] = min(entry[idx][1], mn)
            entry[idx][2] = max(entry[idx][2], mx)
            entry[idx][3] += total
        entry[4] += data['count']
        self.items[key] = entry

    def _enrich_gmeta(
        self,
        gmeta: dict[str, Any],
        items: list[Any],
    ) -> None:
        cnt = items[4]
        stats_parts = [
            calc_file_stats(items[0], cnt, self.mode),
            calc_time_stats(
                items[1],
                cnt,
                self.mode,
                'access_date',
            ),
            calc_time_stats(
                items[2],
                cnt,
                self.mode,
                'change_date',
            ),
            calc_time_stats(
                items[3],
                cnt,
                self.mode,
                'mod_date',
            ),
        ]
        for part in stats_parts:
            gmeta['content']['_raw'].update(part.pop('_raw'))
            gmeta['content'].update(part)

    def _prepare_gmeta(self, key: str) -> str:
        items = self.items.pop(key)
        self.counts.pop(key)

        cfg = _GMETA_CONFIG.get(key[0])
        if cfg is not None:
            field, subject_prefix = cfg
            raw_id = key[1:]
            gmeta: dict[str, Any] = {
                'subject': f'{subject_prefix}{raw_id}',
                'visible_to': self.search_group_urns,
                'content': {
                    field: str(raw_id),
                    '_raw': {field: int(raw_id)},
                },
            }
        else:
            gmeta = {
                'subject': f'dir::{key}',
                'visible_to': self.search_group_urns,
                'content': {'dir': key, '_raw': {}},
            }

        self._enrich_gmeta(gmeta, items)
        return json.dumps(gmeta)

    def process_element(
        self,
        value: tuple[str, str],
        ctx: KeyedProcessFunction.Context,
    ) -> Iterator[str]:
        """Merge data and yield gmeta when all workers reported."""
        key, data_str = value
        if key not in self.items:
            self.items[key] = init_metadata_entry(self.mode)

        data = json.loads(data_str)
        self._merge_entry(key, data)

        if self.counts[key] == self.items[key][4]:
            yield self._prepare_gmeta(key)


def get_usr_grp_dir_dicts(
    conf: IngestConfig,
) -> tuple[dict[tuple[str, int], int], dict[str, int]]:
    """Read aggregation CSV and build count dictionaries."""
    assert conf.prefix_min >= 0
    assert conf.prefix_max >= conf.prefix_min

    aggregate_by_worker: Counter[tuple[str, int]] = Counter()
    aggregate: Counter[str] = Counter()

    want_usr = 'usr' in conf.pipeline_args
    want_grp = 'grp' in conf.pipeline_args
    want_dir = 'dir' in conf.pipeline_args

    file_path = get_agg_file_path(conf.agg_file_name)

    with open(file_path) as f:
        for raw_line in f:
            line = raw_line.strip()
            typ, rest = line.split(', ', 1)
            entity_id, worker_str, cnt_str = rest.rsplit(', ', 2)
            worker = int(worker_str)
            cnt = int(cnt_str)

            if typ == 'user' and want_usr:
                name = f'u{entity_id}'
                aggregate_by_worker[(name, worker)] += cnt
                aggregate[name] += cnt
            elif typ == 'group' and want_grp:
                name = f'g{entity_id}'
                aggregate_by_worker[(name, worker)] += cnt
                aggregate[name] += cnt
            elif typ == 'prefix' and want_dir:
                lvl = 0 if entity_id == '/' else entity_id.count('/')
                if conf.prefix_min <= lvl < conf.prefix_max:
                    aggregate_by_worker[(entity_id, worker)] += cnt
                    aggregate[entity_id] += cnt

    _print_summary(
        conf,
        aggregate_by_worker,
        aggregate,
        want_usr,
        want_grp,
        want_dir,
    )
    return dict(aggregate_by_worker), dict(aggregate)


def _print_summary(
    conf: IngestConfig,
    by_worker: Counter[tuple[str, int]],
    agg: Counter[str],
    want_usr: bool,
    want_grp: bool,
    want_dir: bool,
) -> None:
    print('=== aggregate_by_worker ===')
    if want_usr:
        n = sum(1 for (name, _) in by_worker if name.startswith('u'))
        print(f'users-worker keys: {n}')
    if want_grp:
        n = sum(1 for (name, _) in by_worker if name.startswith('g'))
        print(f'groups-worker keys: {n}')
    if want_dir:
        for lvl in range(conf.prefix_min, conf.prefix_max):
            n = sum(1 for (name, _) in by_worker if _dir_level(name) == lvl)
            print(f'dirs-worker lvl {lvl}: {n}')

    print('=== aggregate ===')
    if want_usr:
        n = sum(1 for name in agg if name.startswith('u'))
        print(f'users: {n}')
    if want_grp:
        n = sum(1 for name in agg if name.startswith('g'))
        print(f'groups: {n}')
    if want_dir:
        for lvl in range(conf.prefix_min, conf.prefix_max):
            n = sum(1 for name in agg if _dir_level(name) == lvl)
            print(f'dirs lvl {lvl}: {n}')


def _dir_level(name: str) -> int | None:
    if name == '/':
        return 0
    return name.count('/') if name.startswith('/') else None
