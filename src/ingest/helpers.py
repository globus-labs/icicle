"""Unified FlatMapFunction classes for all HPC datasets."""

from __future__ import annotations

import json
import stat
import zlib
from collections.abc import Iterator
from datetime import datetime
from datetime import UTC
from typing import Any

from conf import IngestConfig
from constants import derive_worker_id
from constants import NUM_WORKERS
from csv_schema import CsvSchema
from pipeline import counting_pipeline
from pipeline import primary_pipeline
from pipeline import read_from_s3
from pipeline import secondary_pipeline
from prefix import get_prefix
from pyflink.datastream import FlatMapFunction
from pyflink.datastream import StreamExecutionEnvironment


class GetGmeta(FlatMapFunction):
    """Emit primary gmeta JSON for each CSV row."""

    def __init__(
        self,
        schema: CsvSchema,
        search_group_urns: list[str],
    ) -> None:
        self.schema = schema
        self.search_group_urns = search_group_urns

    def flat_map(self, value: str) -> Iterator[str]:
        """Parse a CSV line and yield a gmeta JSON string."""
        s = self.schema
        cols = value.strip().split(',', s.n_cols - 1)
        if len(cols) != s.n_cols:
            return

        path = cols[s.path_col].strip('"')
        typ = cols[s.type_col]
        mode = cols[s.mode_col]
        user = cols[s.user_col]
        group = cols[s.group_col]
        size = cols[s.size_col]
        atime = cols[s.atime_col]
        ctime = cols[s.ctime_col]
        mtime = cols[s.mtime_col]

        adt_utc = datetime.fromtimestamp(int(atime), UTC)
        cdt_utc = datetime.fromtimestamp(int(ctime), UTC)
        mdt_utc = datetime.fromtimestamp(int(mtime), UTC)
        mode_str = stat.filemode(int(mode))

        fmt = '%Y-%m-%d %H:%M:%S'
        content: dict[str, Any] = {
            'file_type': typ,
            'file_size': int(size),
            'user_id': user,
            'group_id': group,
            'mode_first_char': mode_str[0],
            'mode': mode_str,
            'access_date': adt_utc.strftime(fmt) + ' +00:00',
            'change_date': cdt_utc.strftime(fmt) + ' +00:00',
            'mod_date': mdt_utc.strftime(fmt) + ' +00:00',
            'filename': path,
            '_raw': {
                'user_id': int(user),
                'group_id': int(group),
                'mode': int(mode),
                'access_date': int(atime),
                'change_date': int(ctime),
                'mod_date': int(mtime),
            },
        }

        if s.extra_content is not None:
            content.update(s.extra_content(cols))

        gmeta = {
            'subject': path,
            'visible_to': self.search_group_urns,
            'content': content,
        }
        yield json.dumps(gmeta)


class CountsPrepare(FlatMapFunction):
    """Emit (type, name, worker, count) tuples for aggregation."""

    def __init__(
        self,
        schema: CsvSchema,
        prefix_min: int,
        prefix_max: int,
        pipeline_args: str,
    ) -> None:
        self.schema = schema
        self.prefix_min = prefix_min
        self.prefix_max = prefix_max
        self.option = pipeline_args

    def flat_map(
        self,
        value: str,
    ) -> Iterator[tuple[str, str, int, int]]:
        """Parse CSV and yield count tuples."""
        s = self.schema
        columns = value.strip().split(',', s.n_cols - 1)
        if len(columns) != s.n_cols:
            return

        path = columns[s.path_col].strip('"')
        user = columns[s.user_col]
        group = columns[s.group_col]
        worker_id = (
            zlib.crc32(value.encode('utf-8')) & 0xFFFFFFFF
        ) % NUM_WORKERS

        if 'dir' in self.option:
            for i in range(
                self.prefix_max - 1,
                self.prefix_min - 1,
                -1,
            ):
                if prefix := get_prefix(path, i):
                    yield 'prefix', prefix, worker_id, 1
                    break

        if 'usr' in self.option:
            yield 'user', user, worker_id, 1

        if 'grp' in self.option:
            yield 'group', group, worker_id, 1


class GetUsrGrpPathWorkerCols(FlatMapFunction):
    """Emit (name, worker, size, atime, ctime, mtime) for secondary."""

    def __init__(
        self,
        csv_n_cols: int,
        prefix_min: int,
        prefix_max: int,
        pipeline_args: str,
    ) -> None:
        self.csv_n_cols = csv_n_cols
        self.prefix_min = prefix_min
        self.prefix_max = prefix_max
        self.options = pipeline_args

    def flat_map(
        self,
        value: str,
    ) -> Iterator[tuple[str, int, int, int, int, int]]:
        """Parse CSV and yield keyed metric tuples."""
        cols = value.strip().split(',', self.csv_n_cols - 1)
        if len(cols) != self.csv_n_cols:
            return

        size = int(cols[4])
        atime = int(cols[5])
        ctime = int(cols[6])
        mtime = int(cols[7])
        worker_id = (
            zlib.crc32(value.encode('utf-8')) & 0xFFFFFFFF
        ) % NUM_WORKERS

        if 'usr' in self.options:
            yield (
                f'u{cols[2]}',
                worker_id,
                size,
                atime,
                ctime,
                mtime,
            )

        if 'grp' in self.options:
            yield (
                f'g{cols[3]}',
                worker_id,
                size,
                atime,
                ctime,
                mtime,
            )

        if 'dir' in self.options:
            # path_col varies by dataset; infer from n_cols
            path_col = self.csv_n_cols - 1
            path = cols[path_col].strip('"')
            for i in range(self.prefix_min, self.prefix_max):
                if prefix := get_prefix(path, i):
                    wid = derive_worker_id(worker_id, i)
                    yield (
                        prefix,
                        wid,
                        size,
                        atime,
                        ctime,
                        mtime,
                    )


def run_pipelines(
    conf: IngestConfig,
    env: StreamExecutionEnvironment,
    schema: CsvSchema,
) -> None:
    """Dispatch configured pipelines for the given dataset."""
    base = read_from_s3(env, conf.s3_input)

    if 'PRI' in conf.pipeline_name:
        get_gmeta_fm = GetGmeta(schema, conf.search_group_urns)
        primary_pipeline(conf, base, get_gmeta_fm)

    if 'CNT' in conf.pipeline_name:
        counts_prepare_fm = CountsPrepare(
            schema,
            conf.prefix_min,
            conf.prefix_max,
            conf.pipeline_args,
        )
        counting_pipeline(conf, base, counts_prepare_fm)

    if 'SND' in conf.pipeline_name:
        secondary_pipeline(conf, base, GetUsrGrpPathWorkerCols)
