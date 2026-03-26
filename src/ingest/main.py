"""Entry point for the ingest pipeline."""

from __future__ import annotations

from conf import _detect_dataset
from conf import get_config
from conf import get_env
from conf import get_pipeline_name
from csv_schema import HPSS_SCHEMA
from csv_schema import ITAP_SCHEMA
from csv_schema import NERSC_SCHEMA
from helpers import run_pipelines

_SCHEMAS = {
    'hpss': HPSS_SCHEMA,
    'nersc': NERSC_SCHEMA,
    'itap': ITAP_SCHEMA,
}


def main() -> None:
    """Run the ingest pipeline for the configured dataset."""
    conf = get_config()
    dataset_key = _detect_dataset(conf.s3_input)
    schema = _SCHEMAS[dataset_key]

    env = get_env(conf)
    run_pipelines(conf, env, schema)
    env.execute(get_pipeline_name(conf))


if __name__ == '__main__':
    main()
