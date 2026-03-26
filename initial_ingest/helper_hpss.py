from calc import (
    IngestConfig,
    counting_pipeline,
    primary_pipeline,
    read_from_s3,
    secondary_pipeline,
)
from helper_itap import (
    CountsPrepare,
    GetGmeta,
    GetUsrGrpPathWorkerCols,
)
from pyflink.datastream import (
    StreamExecutionEnvironment,
)


def hpss_pipelines(conf: IngestConfig, env: StreamExecutionEnvironment) -> None:
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
