import base64
import datetime
import json
import time
import traceback
from collections import Counter
from time import monotonic
from typing import Any, Callable, Dict, Final, Iterator, List, Optional, Tuple

from conf import CMode, IngestConfig, get_agg_file_path
from datasketches import kll_floats_sketch, req_floats_sketch, tdigest_float
from ddsketch import DDSketch
from ddsketch.pb import ddsketch_pb2 as pb
from ddsketch.pb.proto import DDSketchProto
from globus_sdk import (
    ClientCredentialsAuthorizer,
    ConfidentialAppAuthClient,
    SearchClient,
)
from pyflink.common import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import (
    DataStream,
    FlatMapFunction,
    KeyedProcessFunction,
    ProcessFunction,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors import FileSource, StreamFormat
from search import SEARCH_CLIENT_ID, SEARCH_CLIENT_SECRET
from utils.kafkaconnector import get_kafka_sink


def read_from_s3(env: StreamExecutionEnvironment, s3_input: str) -> DataStream:
    source = (
        FileSource.for_record_stream_format(
            StreamFormat.text_line_format(),
            s3_input,
        )
        .process_static_file_set()
        .build()
    )

    base = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "file-source"
    ).name(s3_input)
    return base


def create_search_client():
    cc = ConfidentialAppAuthClient(
        client_id=SEARCH_CLIENT_ID,
        client_secret=SEARCH_CLIENT_SECRET,
    )

    authorizer = ClientCredentialsAuthorizer(
        confidential_client=cc,
        scopes="urn:globus:auth:scope:search.api.globus.org:all",
    )

    return SearchClient(authorizer=authorizer)


def exception_to_json(exception):
    exception_dict = {
        "acknowledged": False,
        "success": False,
        "type": exception.__class__.__name__,
        "message": str(exception),
        "traceback": traceback.format_exc().splitlines(),
    }
    return json.dumps(exception_dict)


class BatchGmeta(ProcessFunction):
    def __init__(
        self,
        index_id: str,
        max_bs: float,
        max_wt: float,
        mock_ingest: bool,
    ) -> None:
        self.index_id = index_id

        self.max_batch_size = max_bs  # bytes
        self.max_wait_time = max_wt  # seconds
        self.mock_ingest = mock_ingest

        self.ingest_batch: List[Dict[str, Any]] = []
        self.ingest_batch_size: int = 0
        self.timer_ts: Optional[float] = None

        self.search_client: Optional[Any] = None

    def open(self, ctx: ProcessFunction.Context) -> None:
        self.search_client = create_search_client()

    def close(self) -> None:
        if self.ingest_batch:
            self.perform_ingest()

    def perform_ingest(self) -> str:
        data: Dict[str, Any] = {
            "ingest_type": "GMetaList",
            "ingest_data": {"gmeta": self.ingest_batch},
        }

        batch_len = len(self.ingest_batch)
        batch_sz_bytes = self.ingest_batch_size
        batch_sz_mb = batch_sz_bytes / 1024**2

        result: Dict[str, Any] = {
            "mock": self.mock_ingest,
            "msg": f"ingested {batch_len} docs ({batch_sz_bytes} bytes, {batch_sz_mb:.2f} MB)",
        }

        if self.mock_ingest:  # mock path
            result["succeed"] = True

            if batch_len == 1:  # aggregate ingest
                result["data"] = data

        else:  # real path
            try:
                if self.search_client is None:  # mypy
                    raise RuntimeError("Search client not initialized")
                resp = self.search_client.ingest(self.index_id, data=data)
                result.update({"succeed": True, "resp": resp.data})
            except Exception as e:
                result.update({"succeed": False, "resp": exception_to_json(e)})

        # clear the timer marker; next element will re-arm it
        self.timer_ts = None
        self.ingest_batch = []
        self.ingest_batch_size = 0

        return json.dumps(result)

    def process_element(
        self, value: str, ctx: KeyedProcessFunction.Context
    ) -> Iterator[str]:
        self.ingest_batch_size += len(value)
        self.ingest_batch.append(json.loads(value))

        # arm a processing-time timer if none is active
        if self.timer_ts is None:
            self.timer_ts = monotonic()

        # size trigger
        if self.ingest_batch_size >= self.max_batch_size:
            yield self.perform_ingest()

        # time trigger
        elif self.timer_ts is not None and (
            monotonic() - self.timer_ts >= self.max_wait_time
        ):
            yield self.perform_ingest()


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(time.time() * 1000)


def get_watermark_strategy():
    return WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
        MyTimestampAssigner()
    )


def primary_pipeline(
    conf: IngestConfig, base: DataStream, get_gmeta_fm: FlatMapFunction
) -> None:
    (
        base.flat_map(get_gmeta_fm, Types.STRING())
        .name("flat_map: emit primary gmeta")
        .process(
            BatchGmeta(
                conf.primary_index,
                conf.primary_max_bs,
                conf.primary_max_wt,
                conf.mock_ingest,
            ),
            Types.STRING(),
        )
        .name("process: batch primary gmeta")
        .assign_timestamps_and_watermarks(get_watermark_strategy())
        .name("kafka: assign ts")
        .sink_to(get_kafka_sink(conf.primary_ingest_result_topic))
        .name("msk: primary ingest resp")
    )


def counts_prepare(base: DataStream, counts_prepare_fm: FlatMapFunction) -> DataStream:
    counts_prepared = (
        base.flat_map(
            counts_prepare_fm,
            Types.TUPLE(
                [
                    Types.STRING(),  # type
                    Types.STRING(),  # name
                    Types.INT(),  # worker
                    Types.INT(),  # count
                ]
            ),
        )
        .name("flat_map: separate to (type, name, worker, count)")
        # type, name, worker
        .key_by(lambda x: (x[0], x[1], x[2]))
        # type, name, worker, count1 + count2
        .reduce(lambda a, b: (a[0], a[1], a[2], a[3] + b[3]))
        .name("reduce: sum counts")
    )
    return counts_prepared


def save_counts_to_octopus(counts_prepared: DataStream, topic: str, map_name) -> None:
    """
    Push worker-level aggregated counts to Kafka (Octopus) instead of writing to S3.

    We originally emitted the counts as CSV files to S3, but that approach had
    two correctness problems even when forcing `parallelism=1` and disabling
    bucket/rolling:
      1) S3 FileSink occasionally dropped a record.
         - Observed: same run produced different worker subsets (missing worker 2 or 3).
      2) Output ordering is not guaranteed.
         - FileSink writes are based on task completion order, not on sorted
           (typ, name, worker). We still needed a post-processing stage to sort.

    Output record format:
      "typ,name,worker,count"
    """
    (
        counts_prepared.map(lambda x: f"{x[0]},{x[1]},{x[2]},{x[3]}", Types.STRING())
        .name(f"map: {map_name}")
        .assign_timestamps_and_watermarks(get_watermark_strategy())
        .name("kafka: assign ts")
        .sink_to(get_kafka_sink(topic))
        .name("msk: log counts")
    )


def counting_pipeline(
    conf: IngestConfig, base: DataStream, counts_prepare_fm: FlatMapFunction
) -> None:
    counts_prepared = counts_prepare(base, counts_prepare_fm)
    map_name = f"to [{conf.pipeline_args}] CSVs by 64 workers"
    save_counts_to_octopus(counts_prepared, conf.counting_result_topic, map_name)


class GetPrefixWorkerCols(FlatMapFunction):
    def __init__(self, prefix_min, prefix_max):
        self.prefix_min = prefix_min
        self.prefix_max = prefix_max
        self.num_of_workers = 64

    def flat_map(
        self, value: Tuple[str, str, str, int, int, int, int, int]
    ) -> Iterator[Tuple[str, int, int, int, int, int]]:
        # base_worker_id: 0–63
        _, _, path, base_worker_id, size, atime, ctime, mtime = value

        for i in range(self.prefix_min, self.prefix_max):
            if prefix := get_prefix(path, i):
                if i == 2:  # depth
                    derived_worker_id = base_worker_id // 2  # 0–31
                elif i == 3:
                    derived_worker_id = base_worker_id // 4  # 0–15
                elif i >= 4:
                    derived_worker_id = base_worker_id // 8  # 0–7
                else:
                    # keep full granularity for shallow prefixes
                    derived_worker_id = base_worker_id

                yield prefix, derived_worker_id, size, atime, ctime, mtime


_SKETCH_FACTORIES: Final[Dict[CMode, Callable[[], Any]]] = {
    CMode.dd: DDSketch,
    CMode.kll: kll_floats_sketch,
    CMode.req: req_floats_sketch,
    CMode.td: tdigest_float,
}


def _init_metadata_entry(mode: CMode):  # 1/5
    try:
        sketch_factory = _SKETCH_FACTORIES[mode]
    except KeyError as exc:
        raise ValueError(f"Unsupported mode: {mode}") from exc

    def init_metric():
        return [sketch_factory(), float("inf"), -float("inf"), 0]

    # file_size, access_time, change_time, mod_time, and file count
    return [init_metric() for _ in range(4)] + [0]


def _decode_sketch(mode: CMode, payload: str):
    raw = base64.b64decode(payload.encode("ascii"))
    if mode == CMode.dd:
        proto = pb.DDSketch.FromString(raw)
        return DDSketchProto.from_proto(proto)

    if mode == CMode.kll:
        return kll_floats_sketch.deserialize(raw)
    if mode == CMode.req:
        return req_floats_sketch.deserialize(raw)
    if mode == CMode.td:
        return tdigest_float.deserialize(raw)

    raise ValueError(f"Unsupported mode: {mode}")


class SecondaryMap(KeyedProcessFunction):
    def __init__(self, mode: CMode, counts: Dict[Tuple[str, int], int]) -> None:
        self.mode = mode  # ddsketch

        self.counts: Dict[Tuple[str, int], int] = counts
        self.items: Dict[Tuple[str, int], Any] = {}

    def update_metadata_entry(self, key, size, atime, ctime, mtime):  # 2/5
        entry = self.items[key]

        if self.mode == CMode.dd:
            entry[0][0].add(size)
            entry[1][0].add(atime)
            entry[2][0].add(ctime)
            entry[3][0].add(mtime)

        else:  # CMode.kll, CMode.req, CMode.td
            entry[0][0].update(size)
            entry[1][0].update(atime)
            entry[2][0].update(ctime)
            entry[3][0].update(mtime)

        entry[0][1] = min(entry[0][1], size)
        entry[0][2] = max(entry[0][2], size)
        entry[0][3] += size

        entry[1][1] = min(entry[1][1], atime)
        entry[1][2] = max(entry[1][2], atime)
        entry[1][3] += atime

        entry[2][1] = min(entry[2][1], ctime)
        entry[2][2] = max(entry[2][2], ctime)
        entry[2][3] += ctime

        entry[3][1] = min(entry[3][1], mtime)
        entry[3][2] = max(entry[3][2], mtime)
        entry[3][3] += mtime

        entry[4] += 1  # file count
        self.items[key] = entry

    def serialize_metadata_entry(self, key):  # 3/5
        def _encode_ddsketch(sketch) -> str:
            """Serialize a DDSketch to base64-encoded string."""
            msg = DDSketchProto.to_proto(sketch).SerializeToString()
            return base64.b64encode(msg).decode("ascii")

        def _encode_apache_sketch(sketch) -> str:
            msg = sketch.serialize()
            return base64.b64encode(msg).decode("ascii")

        entry = self.items.pop(key)  # entry[i] = (sketch, min, max, sum)
        fs_sk, fs_min, fs_max, fs_sum = entry[0]
        at_sk, at_min, at_max, at_sum = entry[1]
        ct_sk, ct_min, ct_max, ct_sum = entry[2]
        mt_sk, mt_min, mt_max, mt_sum = entry[3]

        result = {"count": entry[4]}

        if self.mode == CMode.dd:
            result["size"] = [_encode_ddsketch(fs_sk), fs_min, fs_max, fs_sum]
            result["atime"] = [_encode_ddsketch(at_sk), at_min, at_max, at_sum]
            result["ctime"] = [_encode_ddsketch(ct_sk), ct_min, ct_max, ct_sum]
            result["mtime"] = [_encode_ddsketch(mt_sk), mt_min, mt_max, mt_sum]

        else:  # CMode.kll, CMode.req, CMode.td
            result["size"] = [_encode_apache_sketch(fs_sk), fs_min, fs_max, fs_sum]
            result["atime"] = [_encode_apache_sketch(at_sk), at_min, at_max, at_sum]
            result["ctime"] = [_encode_apache_sketch(ct_sk), ct_min, ct_max, ct_sum]
            result["mtime"] = [_encode_apache_sketch(mt_sk), mt_min, mt_max, mt_sum]

        return result

    def process_element(
        self,
        value: Tuple[Any, ...],
        ctx: KeyedProcessFunction.Context,
    ) -> Iterator[Tuple[str, str]]:
        (name, worker, size, atime, ctime, mtime) = value
        key = (name, worker)  # u1234, g5678, prefix

        if key not in self.items:
            self.items[key] = _init_metadata_entry(self.mode)

        self.update_metadata_entry(key, size, atime, ctime, mtime)

        if self.counts[key] == self.items[key][4]:
            result = self.serialize_metadata_entry(key)  # entry popped inside
            yield (key[0], json.dumps(result))  # discard the worker id


class SecondaryReduce(KeyedProcessFunction):
    def __init__(self, mode: CMode, search_group_urns, counts) -> None:
        self.mode = mode  # ddsketch

        self.search_group_urns = search_group_urns

        self.counts: Dict[str, int] = counts  # key: user_id, group_id, prefix
        self.items: Dict[str, Any] = {}

    def merge_metadata_entry(self, key, data):  # 4/5
        entry = self.items[key]

        metrics = [data["size"], data["atime"], data["ctime"], data["mtime"]]

        for idx, metric in enumerate(metrics):
            serialized, min_value, max_value, total = metric
            sketch = _decode_sketch(self.mode, serialized)

            entry[idx][0].merge(sketch)
            entry[idx][1] = min(entry[idx][1], min_value)
            entry[idx][2] = max(entry[idx][2], max_value)
            entry[idx][3] += total

        entry[4] += data["count"]  # file count
        self.items[key] = entry

    def enrich_gmeta(self, gmeta, items):  # 5/5
        cnt = items[4]

        if self.mode == CMode.dd:
            file_size_content = calc_file_stats_dd(items[0], cnt)
            access_time_content = calc_time_stats_dd(items[1], cnt, "access_date")
            change_time_content = calc_time_stats_dd(items[2], cnt, "change_date")
            mod_time_content = calc_time_stats_dd(items[3], cnt, "mod_date")

        else:  # CMode.kll, CMode.req, CMode.td
            file_size_content = calc_file_stats_apache(items[0], cnt)
            access_time_content = calc_time_stats_apache(items[1], cnt, "access_date")
            change_time_content = calc_time_stats_apache(items[2], cnt, "change_date")
            mod_time_content = calc_time_stats_apache(items[3], cnt, "mod_date")

        gmeta["content"]["_raw"].update(file_size_content.pop("_raw"))
        gmeta["content"]["_raw"].update(access_time_content.pop("_raw"))
        gmeta["content"]["_raw"].update(change_time_content.pop("_raw"))
        gmeta["content"]["_raw"].update(mod_time_content.pop("_raw"))

        gmeta["content"].update(file_size_content)
        gmeta["content"].update(access_time_content)
        gmeta["content"].update(change_time_content)
        gmeta["content"].update(mod_time_content)

    def prepare_user_gmeta(self, user: str) -> str:
        u_items = self.items.pop(user)
        self.counts.pop(user)

        gmeta = {
            "subject": f"user_id::{user[1:]}",  # u123 -> 123
            "visible_to": self.search_group_urns,
            "content": {
                "user_id": str(user[1:]),
                "_raw": {
                    "user_id": int(user[1:]),
                },
            },
        }

        self.enrich_gmeta(gmeta, u_items)
        return json.dumps(gmeta)

    def prepare_group_gmeta(self, group: str) -> str:
        g_items = self.items.pop(group)
        self.counts.pop(group)

        gmeta = {
            "subject": f"group_id::{group[1:]}",  # g456 -> 456
            "visible_to": self.search_group_urns,
            "content": {
                "group_id": str(group[1:]),
                "_raw": {
                    "group_id": int(group[1:]),
                },
            },
        }

        self.enrich_gmeta(gmeta, g_items)
        return json.dumps(gmeta)

    def prepare_prefix_gmeta(self, prefix: str) -> str:
        d_items = self.items.pop(prefix)
        self.counts.pop(prefix)

        gmeta = {
            "subject": f"dir::{prefix}",
            "visible_to": self.search_group_urns,
            "content": {
                "dir": prefix,
                "_raw": {},  # changed
            },
        }

        self.enrich_gmeta(gmeta, d_items)
        return json.dumps(gmeta)

    def process_element(
        self,
        value: Tuple[str, str],
        ctx: KeyedProcessFunction.Context,
    ) -> Iterator[str]:
        key, data = value
        if key not in self.items:
            self.items[key] = _init_metadata_entry(self.mode)

        data = json.loads(data)
        self.merge_metadata_entry(key, data)

        if self.counts[key] == self.items[key][4]:
            if key[0] == "u":
                yield self.prepare_user_gmeta(key)
            elif key[0] == "g":
                yield self.prepare_group_gmeta(key)
            else:
                yield self.prepare_prefix_gmeta(key)


def get_usr_grp_dir_dicts(
    conf: IngestConfig,
) -> tuple[dict[tuple[str, int], int], dict[str, int]]:
    assert conf.prefix_min >= 0
    assert conf.prefix_max >= conf.prefix_min

    # --- Use Counters for clean += logic ---
    aggregate_by_worker: Counter[tuple[str, int]] = Counter()
    aggregate: Counter[str] = Counter()

    want_usr = "usr" in conf.pipeline_args
    want_grp = "grp" in conf.pipeline_args
    want_dir = "dir" in conf.pipeline_args

    file_path = get_agg_file_path(conf.agg_file_name)

    # ---- Read CSV once ----
    with open(file_path) as f:
        for line in f:
            line = line.strip()
            typ, rest = line.split(", ", 1)
            entity_id, worker_str, cnt_str = rest.rsplit(", ", 2)
            worker = int(worker_str)
            cnt = int(cnt_str)

            # --- Users ---
            if typ == "user" and want_usr:
                name = f"u{entity_id}"
                aggregate_by_worker[(name, worker)] += cnt
                aggregate[name] += cnt

            # --- Groups ---
            elif typ == "group" and want_grp:
                name = f"g{entity_id}"
                aggregate_by_worker[(name, worker)] += cnt
                aggregate[name] += cnt

            # --- Directories ---
            elif typ == "prefix" and want_dir:
                d = entity_id
                lvl = 0 if d == "/" else d.count("/")

                if conf.prefix_min <= lvl < conf.prefix_max:
                    aggregate_by_worker[(d, worker)] += cnt
                    aggregate[d] += cnt

    # ----------------------------------------------------------
    #              PRINT SUMMARY STATISTICS
    # ----------------------------------------------------------
    # Compute how many keys belong to each category
    def is_user(name: str) -> bool:
        return name.startswith("u")

    def is_group(name: str) -> bool:
        return name.startswith("g")

    def dir_level(name: str):
        if name == "/":
            return 0
        return name.count("/") if name.startswith("/") else None

    # --- Stats for aggregate_by_worker ---
    print("=== aggregate_by_worker ===")
    if want_usr:
        n = sum(1 for (name, _) in aggregate_by_worker if is_user(name))
        print(f"users-worker keys: {n}")

    if want_grp:
        n = sum(1 for (name, _) in aggregate_by_worker if is_group(name))
        print(f"groups-worker keys: {n}")

    if want_dir:
        for lvl in range(conf.prefix_min, conf.prefix_max):
            n = sum(1 for (name, _) in aggregate_by_worker if dir_level(name) == lvl)
            print(f"dirs-worker lvl {lvl}: {n}")

    # --- Stats for aggregate ---
    print("=== aggregate ===")
    if want_usr:
        n = sum(1 for name in aggregate if is_user(name))
        print(f"users: {n}")

    if want_grp:
        n = sum(1 for name in aggregate if is_group(name))
        print(f"groups: {n}")

    if want_dir:
        for lvl in range(conf.prefix_min, conf.prefix_max):
            n = sum(1 for name in aggregate if dir_level(name) == lvl)
            print(f"dirs lvl {lvl}: {n}")

    return dict(aggregate_by_worker), dict(aggregate)


def secondary_pipeline(
    conf: IngestConfig,
    base: DataStream,
    secondary_fm: FlatMapFunction,
):
    agg_by_worker_dict, agg_dict = get_usr_grp_dir_dicts(conf)

    (
        base.flat_map(
            secondary_fm(
                conf.csv_n_cols,
                conf.prefix_min,
                conf.prefix_max,
                conf.pipeline_args,
            ),
            Types.TUPLE(
                [
                    Types.STRING(),  # name
                    Types.INT(),  # worker_id
                    Types.LONG(),  # size
                    Types.LONG(),  # atime
                    Types.LONG(),  # ctime
                    Types.LONG(),  # mtime
                ]
            ),
        )
        .key_by(lambda x: (x[0], x[1]))  # name, worker_id
        .process(
            SecondaryMap(conf.cmode, agg_by_worker_dict),
            Types.TUPLE([Types.STRING(), Types.STRING()]),  # key, json data
        )
        .key_by(lambda x: x[0])  # u1234, g5678, prefix
        .process(
            SecondaryReduce(conf.cmode, conf.search_group_urns, agg_dict),
            Types.STRING(),
        )
        .process(
            BatchGmeta(
                conf.secondary_index,
                conf.secondary_max_bs,
                conf.secondary_max_wt,
                conf.mock_ingest,
            ),
            Types.STRING(),
        )
        .assign_timestamps_and_watermarks(get_watermark_strategy())
        .sink_to(get_kafka_sink(conf.secondary_ingest_result_topic))
    )


def _return_stats(stats, metric_map, value_formatter, raw_formatter=lambda v: v):
    response = {"_raw": {}}
    for stat_key, result_key in metric_map:
        raw_value = raw_formatter(stats[stat_key])
        response[result_key] = value_formatter(raw_value)
        response["_raw"][result_key] = raw_value
    return response


def _return_file_stats(stats, prefix="file_size", suffix="_in_bytes"):
    metric_map = [("count", "file_count")]
    metric_map += [
        (metric, f"{prefix}_{metric}{suffix}")
        for metric in (
            "total",
            "average",
            "10p",
            "25p",
            "median",
            "75p",
            "90p",
            "99p",
            "min",
            "max",
        )
    ]
    return _return_stats(
        stats,
        metric_map,
        value_formatter=lambda v: str(int(v)),
        raw_formatter=int,
    )


def _return_time_stats(stats, prefix):
    def _format_time(ts):
        dt = datetime.datetime.fromtimestamp(int(ts), datetime.timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S") + " +00:00"

    metric_map = [
        (metric, f"{prefix}_{metric}")
        for metric in (
            "average",
            "10p",
            "25p",
            "median",
            "75p",
            "90p",
            "99p",
            "min",
            "max",
        )
    ]
    return _return_stats(
        stats,
        metric_map,
        value_formatter=_format_time,
        raw_formatter=int,
    )


def calc_file_stats_dd(entry, size_cnt, prefix="file_size", suffix="_in_bytes"):
    sketch, size_min, size_max, size_total = entry
    get_quantile = sketch.get_quantile_value
    quantiles = {
        label: int(get_quantile(q))
        for label, q in (
            ("10p", 0.1),
            ("25p", 0.25),
            ("median", 0.5),
            ("75p", 0.75),
            ("90p", 0.9),
            ("99p", 0.99),
        )
    }
    size_cnt = int(size_cnt)
    size_total = int(size_total)
    size_avg = size_total // size_cnt
    stats = {
        "count": size_cnt,
        "total": size_total,
        "average": size_avg,
        "min": int(size_min),
        "max": int(size_max),
        **quantiles,
    }
    return _return_file_stats(stats, prefix, suffix)


def calc_file_stats_apache(entry, size_cnt, prefix="file_size", suffix="_in_bytes"):
    sketch, size_min, size_max, size_total = entry
    get_quantile = sketch.get_quantile
    quantiles = {
        label: int(get_quantile(q))
        for label, q in (
            ("10p", 0.1),
            ("25p", 0.25),
            ("median", 0.5),
            ("75p", 0.75),
            ("90p", 0.9),
            ("99p", 0.99),
        )
    }
    size_cnt = int(size_cnt)
    size_total = int(size_total)
    size_avg = size_total // size_cnt
    stats = {
        "count": size_cnt,
        "total": size_total,
        "average": size_avg,
        "min": int(size_min),
        "max": int(size_max),
        **quantiles,
    }
    return _return_file_stats(stats, prefix, suffix)


def calc_time_stats_dd(entry, time_cnt, prefix):
    sketch, time_min, time_max, time_total = entry
    get_quantile = sketch.get_quantile_value
    quantiles = {
        label: int(get_quantile(q))
        for label, q in (
            ("10p", 0.1),
            ("25p", 0.25),
            ("median", 0.5),
            ("75p", 0.75),
            ("90p", 0.9),
            ("99p", 0.99),
        )
    }
    time_cnt = int(time_cnt)
    time_total = int(time_total)
    time_avg = time_total // time_cnt
    stats = {
        "average": time_avg,
        "min": int(time_min),
        "max": int(time_max),
        **quantiles,
    }
    return _return_time_stats(stats, prefix)


def calc_time_stats_apache(entry, time_cnt, prefix):
    sketch, time_min, time_max, time_total = entry
    get_quantile = sketch.get_quantile
    quantiles = {
        label: int(get_quantile(q))
        for label, q in (
            ("10p", 0.1),
            ("25p", 0.25),
            ("median", 0.5),
            ("75p", 0.75),
            ("90p", 0.9),
            ("99p", 0.99),
        )
    }
    time_cnt = int(time_cnt)
    time_total = int(time_total)
    time_avg = time_total // time_cnt
    stats = {
        "average": time_avg,
        "min": int(time_min),
        "max": int(time_max),
        **quantiles,
    }
    return _return_time_stats(stats, prefix)


def get_prefix(filename: str, level: int) -> str:
    """
    Return the directory prefix consisting of the first `level` path components.
    - If level <= 0: return "/".
    - If the file's directory depth < level: return "" (no prefix).
    Assumes `filename` is an absolute file path (e.g., "/a/b/c.txt") and does not end with "/".
    """
    ROOT: Final[str] = "/"
    if level <= 0:
        return ROOT

    # Split into components; last part is the file name
    parts = filename.strip("/").split("/")
    dir_parts = parts[:-1]  # exclude the file name
    depth = len(dir_parts)

    if depth < level:
        return ""  # not enough depth to produce the requested prefix

    # Join the first `level` directories
    return ROOT + "/".join(dir_parts[:level]) if level > 0 else ROOT
