"""Configuration management for the ingest pipeline."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from dataclasses import field
from enum import StrEnum
from pathlib import Path
from typing import Any

import pyflink
from pyflink.common import Configuration
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream import RuntimeExecutionMode
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.util.java_utils import get_j_env_configuration


class CMode(StrEnum):
    """Approximate sketch algorithm selection."""

    dd = 'dd'
    kll = 'kll'
    req = 'req'
    td = 'td'


@dataclass
class IngestConfig:
    """Runtime configuration for the ingest pipeline."""

    s3_input: str = ''
    agg_file_name: str = ''
    execution_mode: str = ''
    cmode: CMode = CMode.dd
    run: int = 5

    pipeline_name: str = ''
    pipeline_args: str = ''
    mock_ingest: bool = True
    prefix_min: int = 1
    prefix_max: int = 5

    search_group_uuid: str = ''
    search_group_urns: list[str] = field(default_factory=list)
    primary_ingest_result_topic: str = 'search-alpha-primary-ingest-results'
    secondary_ingest_result_topic: str = (
        'search-alpha-secondary-ingest-results'
    )
    counting_result_topic: str = 'search-alpha-counts'

    csv_n_cols: int = 1
    primary_index: str = ''
    secondary_index: str = ''

    primary_max_bs: float = 9.9 * 1024**2
    secondary_max_bs: float = 1
    primary_max_wt: int = 5
    secondary_max_wt: int = 1

    def __post_init__(self) -> None:
        """Derive search group URNs from the UUID."""
        self.search_group_urns = [
            f'urn:globus:groups:id:{self.search_group_uuid}',
        ]


# -- Dataset registry ------------------------------------------------


@dataclass(frozen=True)
class DatasetDefaults:
    """Per-dataset default configuration values."""

    s3_input: str
    agg_file_name: str
    csv_n_cols: int
    prefix_min: int
    prefix_max: int
    search_group_uuid: str
    primary_index: str
    secondary_index: str


_DATASET_REGISTRY: dict[str, DatasetDefaults] = {
    'hpss': DatasetDefaults(
        s3_input='s3a://purdue-fsdump-bucket/hpss/1m',
        agg_file_name='hpss.agg2.csv',
        csv_n_cols=9,
        prefix_min=1,
        prefix_max=5,
        search_group_uuid=('b714f229-b692-11f0-89d3-0ee9d7d7fffb'),
        primary_index='9f916965-0af1-49bd-892b-fde67f3d0512',
        secondary_index='06160b2c-3055-47c2-a85f-ff92d5da2804',
    ),
    'nersc': DatasetDefaults(
        s3_input='s3a://nersc-fsdump-bucket/1mv2/',
        agg_file_name='nersc.agg2.csv',
        csv_n_cols=10,
        prefix_min=0,
        prefix_max=5,
        search_group_uuid=('aa3e01be-b326-11ef-8558-29dad179ee6b'),
        primary_index='4ca0f1cf-e71e-4ea4-8f33-3010b4bd9039',
        secondary_index='235c880d-3921-419c-a060-651914c3ad04',
    ),
    'itap': DatasetDefaults(
        s3_input='s3a://purdue-fsdump-bucket/itap/1m',
        agg_file_name='itap.agg2.csv',
        csv_n_cols=9,
        prefix_min=1,
        prefix_max=5,
        search_group_uuid=('b714f229-b692-11f0-89d3-0ee9d7d7fffb'),
        primary_index='0502cca3-2a4a-4b92-8d73-054c8eb0a881',
        secondary_index='3141323a-c5b6-4594-992b-c5135c8ede67',
    ),
}


_DATASET_ALIASES: dict[str, str] = {
    'fs-small': 'itap',
    'fs-medium': 'nersc',
    'fs-large': 'hpss',
}


def _detect_dataset(s3_input: str) -> str:
    """Detect the dataset name from an S3 path."""
    for key in _DATASET_REGISTRY:
        if key in s3_input:
            return key
    for alias, key in _DATASET_ALIASES.items():
        if alias in s3_input:
            return key
    raise ValueError(f'Unknown dataset in s3_input: {s3_input}')


# -- Config loading --------------------------------------------------


def _get_ingest_props() -> dict[str, str] | None:
    if os.environ.get('IS_LOCAL'):
        prop_file = 'application_properties.json'
    else:
        prop_file = '/etc/flink/application_properties.json'
    with open(prop_file) as f:
        json_data = json.load(f)

    for entry in json_data:
        if entry.get('PropertyGroupId') == 'IngestConfig':
            return entry.get('PropertyMap', {})
    return None


def get_s3_input_from_json() -> str:
    """Read s3_input from the properties file."""
    ingest_props = _get_ingest_props() or {}
    return ingest_props.get('s3_input', '')


def _get_default_config() -> IngestConfig:
    s3_input = get_s3_input_from_json()
    dataset_key = _detect_dataset(s3_input)
    defaults = _DATASET_REGISTRY[dataset_key]
    return IngestConfig(
        s3_input=defaults.s3_input,
        agg_file_name=defaults.agg_file_name,
        csv_n_cols=defaults.csv_n_cols,
        prefix_min=defaults.prefix_min,
        prefix_max=defaults.prefix_max,
        search_group_uuid=defaults.search_group_uuid,
        primary_index=defaults.primary_index,
        secondary_index=defaults.secondary_index,
    )


def _update_config_from_json(conf: IngestConfig) -> None:
    ingest_props = _get_ingest_props()
    if ingest_props is None:
        return

    for key, value in ingest_props.items():
        if not hasattr(conf, key):
            continue

        current_type = type(getattr(conf, key))
        converted_value: Any = value
        if current_type is bool:
            converted_value = value.lower() == 'true'
        elif current_type is int:
            converted_value = int(value)
        elif current_type is CMode:
            converted_value = CMode(value)

        setattr(conf, key, converted_value)


def get_config() -> IngestConfig:
    """Load configuration with precedence.

    1. IngestConfig() defaults
    2. _get_default_config() overrides the constructor defaults
    3. application_properties.json overrides both of the above
    """
    conf = _get_default_config()
    _update_config_from_json(conf)

    if conf.run > 1:
        conf.secondary_ingest_result_topic += f'-{int(conf.run)}'

    return conf


def get_pipeline_name(conf: IngestConfig) -> str:
    """Build a human-readable pipeline name string."""
    dataset_key = _detect_dataset(conf.s3_input)
    label = dataset_key.upper()
    return (
        f'{label} Ingest, pipeline: {conf.pipeline_name},'
        f' args: {conf.pipeline_args}, cmode: {conf.cmode},'
        f' prange: [{conf.prefix_min}, {conf.prefix_max})'
    )


def get_env(
    conf: IngestConfig,
) -> StreamExecutionEnvironment:
    """Create and configure a Flink execution environment."""
    env = StreamExecutionEnvironment.get_execution_environment()

    config = Configuration(
        j_configuration=get_j_env_configuration(
            env._j_stream_execution_environment,
        ),
    )
    config.set_integer(
        'python.fn-execution.bundle.size',
        1_000_000,
    )
    config.set_integer(
        'python.fn-execution.bundle.time',
        10_000,
    )

    if conf.execution_mode == 'streaming':
        env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    else:
        env.set_runtime_mode(RuntimeExecutionMode.BATCH)

    if os.environ.get('IS_LOCAL'):
        env.set_restart_strategy(RestartStrategies.no_restart())
        current_dir = os.path.dirname(
            os.path.realpath(__file__),
        )
        flink_path = os.path.dirname(
            os.path.abspath(pyflink.__file__),
        )
        env.add_jars(
            f'file:///{current_dir}/target/pyflink-dependencies.jar',
        )
        print(f'Pipeline {get_pipeline_name(conf)}')
        print(f'PyFlink: {flink_path}')
        print(f'Log dir: {flink_path}/log')

    return env


def get_agg_file_path(agg_file: str) -> str:
    """Resolve the path to an aggregation CSV file."""
    if not agg_file:
        raise ValueError('agg_file must not be empty')
    if os.environ.get('IS_LOCAL'):
        return str(Path(__file__).parent / 'resources' / agg_file)
    for p in sys.path:
        candidate = Path(p) / agg_file
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError(
        f'{agg_file} not found in sys.path resources',
    )


if __name__ == '__main__':
    conf = get_config()
    print(conf)
    print(get_pipeline_name(conf))
    print(get_agg_file_path(conf.agg_file_name))
