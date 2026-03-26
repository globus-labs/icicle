from conf import (
    get_config,
    get_env,
    get_pipeline_name,
    get_s3_input_from_json,
)
from helper_hpss import hpss_pipelines
from helper_itap import itap_pipelines
from helper_nersc import nersc_pipelines


def main():
    s3_input = get_s3_input_from_json()
    conf = get_config()

    if "hpss" in s3_input:
        env = get_env(conf)
        hpss_pipelines(conf, env)
        env.execute(get_pipeline_name(conf))

    elif "nersc" in s3_input:
        env = get_env(conf)
        nersc_pipelines(conf, env)
        env.execute(get_pipeline_name(conf))

    elif "itap" in s3_input:
        env = get_env(conf)
        itap_pipelines(conf, env)
        env.execute(get_pipeline_name(conf))

    else:
        raise


if __name__ == "__main__":
    main()
