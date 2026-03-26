"""Flink pipeline orchestration for primary, counting, and secondary."""

from __future__ import annotations

import time
from typing import Any

from aggregation import get_usr_grp_dir_dicts
from aggregation import SecondaryMap
from aggregation import SecondaryReduce
from batch import BatchGmeta
from conf import IngestConfig
from constants import NUM_WORKERS
from pyflink.common import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import DataStream
from pyflink.datastream import FlatMapFunction
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSource
from pyflink.datastream.connectors import StreamFormat
from utils.kafkaconnector import get_kafka_sink


def read_from_s3(
    env: StreamExecutionEnvironment,
    s3_input: str,
) -> DataStream:
    """Create a DataStream from S3 text files."""
    source = (
        FileSource.for_record_stream_format(
            StreamFormat.text_line_format(),
            s3_input,
        )
        .process_static_file_set()
        .build()
    )
    return env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        'file-source',
    ).name(s3_input)


class MyTimestampAssigner(TimestampAssigner):
    """Assign wall-clock timestamps to records."""

    def extract_timestamp(
        self,
        value: Any,
        record_timestamp: int,
    ) -> int:
        """Return current time in milliseconds."""
        return int(time.time() * 1000)


def get_watermark_strategy() -> WatermarkStrategy:
    """Build a monotonous-timestamps watermark strategy."""
    return (
        WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
            MyTimestampAssigner(),
        )
    )


def primary_pipeline(
    conf: IngestConfig,
    base: DataStream,
    get_gmeta_fm: FlatMapFunction,
) -> None:
    """Ingest individual file metadata via primary pipeline."""
    (
        base.flat_map(get_gmeta_fm, Types.STRING())
        .name('flat_map: emit primary gmeta')
        .process(
            BatchGmeta(
                conf.primary_index,
                conf.primary_max_bs,
                conf.primary_max_wt,
                conf.mock_ingest,
            ),
            Types.STRING(),
        )
        .name('process: batch primary gmeta')
        .assign_timestamps_and_watermarks(
            get_watermark_strategy(),
        )
        .name('kafka: assign ts')
        .sink_to(get_kafka_sink(conf.primary_ingest_result_topic))
        .name('msk: primary ingest resp')
    )


def counts_prepare(
    base: DataStream,
    counts_prepare_fm: FlatMapFunction,
) -> DataStream:
    """Apply counts preparation flat map and reduce by key."""
    return (
        base.flat_map(
            counts_prepare_fm,
            Types.TUPLE(
                [
                    Types.STRING(),
                    Types.STRING(),
                    Types.INT(),
                    Types.INT(),
                ],
            ),
        )
        .name('flat_map: separate to (type, name, worker, count)')
        .key_by(lambda x: (x[0], x[1], x[2]))
        .reduce(lambda a, b: (a[0], a[1], a[2], a[3] + b[3]))
        .name('reduce: sum counts')
    )


def save_counts_to_octopus(
    counts_prepared: DataStream,
    topic: str,
    map_name: str,
) -> None:
    """Push worker-level aggregated counts to Kafka."""
    (
        counts_prepared.map(
            lambda x: f'{x[0]},{x[1]},{x[2]},{x[3]}',
            Types.STRING(),
        )
        .name(f'map: {map_name}')
        .assign_timestamps_and_watermarks(
            get_watermark_strategy(),
        )
        .name('kafka: assign ts')
        .sink_to(get_kafka_sink(topic))
        .name('msk: log counts')
    )


def counting_pipeline(
    conf: IngestConfig,
    base: DataStream,
    counts_prepare_fm: FlatMapFunction,
) -> None:
    """Aggregate and publish per-entity counts."""
    prepared = counts_prepare(base, counts_prepare_fm)
    map_name = f'to [{conf.pipeline_args}] CSVs by {NUM_WORKERS} workers'
    save_counts_to_octopus(
        prepared,
        conf.counting_result_topic,
        map_name,
    )


def secondary_pipeline(
    conf: IngestConfig,
    base: DataStream,
    secondary_fm: type[FlatMapFunction],
) -> None:
    """Build and ingest aggregated secondary metadata."""
    agg_by_worker, agg_dict = get_usr_grp_dir_dicts(conf)

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
                    Types.STRING(),
                    Types.INT(),
                    Types.LONG(),
                    Types.LONG(),
                    Types.LONG(),
                    Types.LONG(),
                ],
            ),
        )
        .key_by(lambda x: (x[0], x[1]))
        .process(
            SecondaryMap(conf.cmode, agg_by_worker),
            Types.TUPLE([Types.STRING(), Types.STRING()]),
        )
        .key_by(lambda x: x[0])
        .process(
            SecondaryReduce(
                conf.cmode,
                conf.search_group_urns,
                agg_dict,
            ),
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
        .assign_timestamps_and_watermarks(
            get_watermark_strategy(),
        )
        .sink_to(
            get_kafka_sink(conf.secondary_ingest_result_topic),
        )
    )
