import json
import stat
import zlib
from datetime import datetime, timezone
from typing import Iterator, Tuple

from calc import (
    IngestConfig,
    counting_pipeline,
    get_prefix,
    primary_pipeline,
    read_from_s3,
    secondary_pipeline,
)
from pyflink.datastream import (
    FlatMapFunction,
    StreamExecutionEnvironment,
)

# Note 1: nersc-fsdump-bucket/1m/*.csv schema -- type (0), mode (1), uid (2), gid (3), size (4), atime (5),
# ctime (6), mtime (7), fileset (8), path (9)
# where mode is an int like 33204 and stat.filemode(int(33204)) gives '-rw-rw-r--'


class GetGmeta(FlatMapFunction):
    def __init__(self, csv_n_cols, search_group_urns):
        self.csv_n_cols = csv_n_cols
        self.search_group_urns = search_group_urns

    def flat_map(self, value: str) -> Iterator[str]:
        cols = value.strip().split(",", self.csv_n_cols - 1)
        if len(cols) != self.csv_n_cols:
            return

        typ, mode, user, group = cols[0], cols[1], cols[2], cols[3]
        size, atime, ctime, mtime = cols[4], cols[5], cols[6], cols[7]
        fileset, path = cols[8], cols[9].strip('"')

        subject = path
        adt_utc = datetime.fromtimestamp(int(atime), timezone.utc)
        cdt_utc = datetime.fromtimestamp(int(ctime), timezone.utc)
        mdt_utc = datetime.fromtimestamp(int(mtime), timezone.utc)
        mode_str = stat.filemode(int(mode))
        content = {
            "file_type": typ,  # new: "f" or "l"
            "file_size": int(size),  # int
            "fileset_name": fileset,  # str: unique in nersc
            "user_id": user,  # str
            "group_id": group,  # str
            "mode_first_char": mode_str[0],  # str
            "mode": mode_str,  # str
            "access_date": adt_utc.strftime("%Y-%m-%d %H:%M:%S") + " +00:00",
            "change_date": cdt_utc.strftime("%Y-%m-%d %H:%M:%S") + " +00:00",
            "mod_date": mdt_utc.strftime("%Y-%m-%d %H:%M:%S") + " +00:00",
            "filename": subject,
            "_raw": {
                "user_id": int(user),
                "group_id": int(group),
                "mode": int(mode),
                "access_date": int(atime),
                "change_date": int(ctime),
                "mod_date": int(mtime),
            },
        }

        gmeta = {
            "subject": subject,
            "visible_to": self.search_group_urns,
            "content": content,
        }
        yield json.dumps(gmeta)


class CountsPrepare(FlatMapFunction):  # prefix, user, and group
    def __init__(
        self,
        csv_n_cols,
        prefix_min,
        prefix_max,
        pipeline_args,
    ):
        self.csv_n_cols = csv_n_cols
        self.prefix_min = prefix_min
        self.prefix_max = prefix_max
        self.option = pipeline_args
        self.num_of_workers = 64

    def flat_map(self, value: str) -> Iterator[Tuple[str, str, int, int]]:
        columns = value.strip().split(",", self.csv_n_cols - 1)
        if len(columns) != self.csv_n_cols:
            return

        path = columns[9].strip('"')
        user, group = columns[2], columns[3]
        worker_id = zlib.crc32(value.encode("utf-8")) & 0xFFFFFFFF
        worker_id %= self.num_of_workers

        if "dir" in self.option:
            for i in range(self.prefix_max - 1, self.prefix_min - 1, -1):
                if prefix := get_prefix(path, i):
                    yield "prefix", prefix, worker_id, 1
                    break

        if "usr" in self.option:
            yield "user", user, worker_id, 1

        if "grp" in self.option:
            yield "group", group, worker_id, 1


class GetUsrGrpPathWorkerCols(FlatMapFunction):
    def __init__(self, csv_n_cols, prefix_min, prefix_max, pipeline_args):
        self.csv_n_cols = csv_n_cols
        self.prefix_min = prefix_min
        self.prefix_max = prefix_max
        self.options = pipeline_args
        self.num_of_workers = 64

    def flat_map(self, value: str) -> Iterator[Tuple[str, str, int, int, int, int]]:
        cols = value.strip().split(",", self.csv_n_cols - 1)
        if len(cols) != self.csv_n_cols:
            return

        size, atime, ctime, mtime = cols[4], cols[5], cols[6], cols[7]
        worker_id = zlib.crc32(value.encode("utf-8")) & 0xFFFFFFFF
        worker_id %= self.num_of_workers

        if "usr" in self.options:
            yield (
                f"u{cols[2]}",
                worker_id,
                int(size),
                int(atime),
                int(ctime),
                int(mtime),
            )

        if "grp" in self.options:
            yield (
                f"g{cols[3]}",
                worker_id,
                int(size),
                int(atime),
                int(ctime),
                int(mtime),
            )

        if "dir" in self.options:
            path = cols[9].strip('"')
            for i in range(self.prefix_min, self.prefix_max):
                if prefix := get_prefix(path, i):
                    if i == 2:  # depth
                        worker_id2 = worker_id // 2  # 0–31
                    elif i == 3:
                        worker_id2 = worker_id // 4  # 0–15
                    elif i >= 4:
                        worker_id2 = worker_id // 8  # 0–7
                    else:
                        # keep full granularity for shallow prefixes
                        worker_id2 = worker_id
                    yield (
                        prefix,
                        worker_id2,
                        int(size),
                        int(atime),
                        int(ctime),
                        int(mtime),
                    )


def nersc_pipelines(conf: IngestConfig, env: StreamExecutionEnvironment) -> None:
    base = read_from_s3(env, conf.s3_input)

    if "PRI" in conf.pipeline_name:
        get_gmeta_fm = GetGmeta(conf.csv_n_cols, conf.search_group_urns)
        primary_pipeline(conf, base, get_gmeta_fm)

    if "CNT" in conf.pipeline_name:
        counts_prepare_fm = CountsPrepare(
            conf.csv_n_cols,
            conf.prefix_min,
            conf.prefix_max,
            conf.pipeline_args,
        )
        counting_pipeline(conf, base, counts_prepare_fm)

    if "SND" in conf.pipeline_name:
        secondary_pipeline(conf, base, GetUsrGrpPathWorkerCols)
