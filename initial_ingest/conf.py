import json
import os
import sys
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Dict, List, Optional

import pyflink
from pyflink.common import Configuration
from pyflink.common.restart_strategy import (
    RestartStrategies,
)
from pyflink.datastream import (
    RuntimeExecutionMode,
    StreamExecutionEnvironment,
)
from pyflink.util.java_utils import get_j_env_configuration


class CMode(StrEnum):
    dd = "dd"  # https://github.com/DataDog/sketches-py
    kll = "kll"  # https://apache.github.io/datasketches-python/main/quantiles/kll.html#datasketches.kll_floats_sketch
    req = "req"  # https://apache.github.io/datasketches-python/main/quantiles/req.html#datasketches.req_floats_sketch
    td = "td"  # https://apache.github.io/datasketches-python/main/quantiles/tdigest.html#datasketches.tdigest_float


@dataclass
class IngestConfig:
    s3_input: str = ""
    agg_file_name: str = ""
    execution_mode: str = ""  # Flink execution mode: streaming | batch
    cmode: CMode = CMode.dd  # Approximate algorithm computation mode
    run: int = 5

    pipeline_name: str = ""  # PRI | CNT | SND
    pipeline_args: str = ""  # usr|grp|dir apply to CNT and SND
    mock_ingest: bool = True  # if True skips search_client.ingest calls
    prefix_min: int = 1  # [prefix_min, prefix_max) for CNT and SND's dir, e.g.,
    prefix_max: int = 5  # [1, 5) == /itap, /itap/usr, /itap/usr/a, /itap/usr/a/b

    search_group_uuid: str = ""
    search_group_urns: List[str] = field(default_factory=list)
    primary_ingest_result_topic: str = "search-alpha-primary-ingest-results"
    secondary_ingest_result_topic: str = "search-alpha-secondary-ingest-results"
    counting_result_topic: str = "search-alpha-counts"

    csv_n_cols: int = 1  # placeholder

    primary_index: str = ""
    secondary_index: str = ""

    primary_max_bs: float = 9.9 * 1024**2  # bytes
    secondary_max_bs: float = 1  # bytes
    primary_max_wt: int = 5  # seconds
    secondary_max_wt: int = 1  # seconds

    def __post_init__(self) -> None:
        self.search_group_urns = [f"urn:globus:groups:id:{self.search_group_uuid}"]


def _get_ingest_props() -> Optional[Dict[str, str]]:
    if os.environ.get("IS_LOCAL"):
        prop_file = "application_properties.json"  # local
    else:
        prop_file = "/etc/flink/application_properties.json"  # on KDA
    with open(prop_file) as f:
        json_data = json.load(f)

    for entry in json_data:
        if entry.get("PropertyGroupId") == "IngestConfig":
            return entry.get("PropertyMap", {})
    return None


def get_s3_input_from_json() -> str:
    ingest_props = _get_ingest_props() or {}
    return ingest_props.get("s3_input", "")


def _get_default_config() -> IngestConfig:
    s3_input = get_s3_input_from_json()

    if "hpss" in s3_input:
        config = IngestConfig(
            s3_input="s3a://purdue-fsdump-bucket/hpss/1m",
            agg_file_name="hpss.agg2.csv",  # TODO
            csv_n_cols=9,
            prefix_min=1,
            prefix_max=5,
            search_group_uuid="b714f229-b692-11f0-89d3-0ee9d7d7fffb",
            primary_index="9f916965-0af1-49bd-892b-fde67f3d0512",  # Haochen's test1
            secondary_index="06160b2c-3055-47c2-a85f-ff92d5da2804",  # Haochen's test2
        )
    elif "nersc" in s3_input:
        config = IngestConfig(
            s3_input="s3a://nersc-fsdump-bucket/1mv2/",  # 1m: 129 files, 1mv2: 256 files, 1mv3: 13 files
            agg_file_name="nersc.agg2.csv",
            csv_n_cols=10,
            prefix_min=0,
            prefix_max=5,
            search_group_uuid="aa3e01be-b326-11ef-8558-29dad179ee6b",
            primary_index="4ca0f1cf-e71e-4ea4-8f33-3010b4bd9039",  # nersc1
            secondary_index="235c880d-3921-419c-a060-651914c3ad04",  # nersc2
        )
    elif "itap" in s3_input:
        config = IngestConfig(
            s3_input="s3a://purdue-fsdump-bucket/itap/1m",
            agg_file_name="itap.agg2.csv",
            csv_n_cols=9,
            prefix_min=1,
            prefix_max=5,
            search_group_uuid="b714f229-b692-11f0-89d3-0ee9d7d7fffb",
            primary_index="0502cca3-2a4a-4b92-8d73-054c8eb0a881",  # itap1
            secondary_index="3141323a-c5b6-4594-992b-c5135c8ede67",  # itap2
        )
    else:
        raise

    return config


def _update_config_from_json(conf: IngestConfig) -> None:
    ingest_props = _get_ingest_props()
    if ingest_props is None:
        return

    for key, value in ingest_props.items():
        if not hasattr(conf, key):
            # ignore unknown keys silently
            continue

        # Convert JSON values to correct Python types
        current_type = type(getattr(conf, key))

        converted_value: Any = value
        if current_type is bool:
            converted_value = value.lower() == "true"
        elif current_type is int:
            converted_value = int(value)
        elif current_type is CMode:
            converted_value = CMode(value)

        setattr(conf, key, converted_value)


def get_config() -> IngestConfig:
    """
    Configuration precedence (lowest → highest):
        1. IngestConfig() defaults
        2. _get_default_config() overrides the constructor defaults
        3. application_properties.json overrides both of the above
    """
    conf = _get_default_config()
    _update_config_from_json(conf)

    if conf.run > 1:
        conf.secondary_ingest_result_topic += f"-{int(conf.run)}"

    return conf


def get_pipeline_name(conf: IngestConfig):
    if "hpss" in conf.s3_input:
        s3_in = "HPSS"
    elif "nersc" in conf.s3_input:
        s3_in = "NERSC"
    elif "itap" in conf.s3_input:
        s3_in = "ITAP"
    else:
        raise

    return f"{s3_in} Ingest, pipeline: {conf.pipeline_name}, args: {conf.pipeline_args}, cmode: {conf.cmode}, prange: [{conf.prefix_min}, {conf.prefix_max})"


def get_env(conf: IngestConfig):  # 0: streaming, 1: batch
    env = StreamExecutionEnvironment.get_execution_environment()

    config = Configuration(
        j_configuration=get_j_env_configuration(env._j_stream_execution_environment)
    )
    config.set_integer("python.fn-execution.bundle.size", 100000 * 10)
    config.set_integer("python.fn-execution.bundle.time", 1000 * 10)

    # https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/execution_mode/#order-of-processing
    if conf.execution_mode == "streaming":
        env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    else:
        env.set_runtime_mode(RuntimeExecutionMode.BATCH)

    if os.environ.get("IS_LOCAL"):
        env.set_restart_strategy(RestartStrategies.no_restart())
        CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
        FLINK_PATH = os.path.dirname(os.path.abspath(pyflink.__file__))
        env.add_jars(f"file:///{CURRENT_DIR}/target/pyflink-dependencies.jar")
        print("Pipeline " + get_pipeline_name(conf))
        print("PyFlink: " + FLINK_PATH)
        print("Log dir: " + FLINK_PATH + "/log")

    return env


def get_agg_file_path(agg_file: str):
    assert agg_file
    if os.environ.get("IS_LOCAL"):
        return os.path.join("resources", agg_file)
    else:
        return os.path.join(
            [e for e in sys.path if e.endswith("resources")][0],
            agg_file,
        )


if __name__ == "__main__":
    conf = get_config()
    print(conf)

    print(get_pipeline_name(conf))
    print(get_agg_file_path(conf.agg_file_name))
