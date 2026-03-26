#!/bin/bash

wait_for_kda_ready() {
    # Poll kda status until it reports READY.
    while true; do
        status=$(./kda-describe.sh)
        echo "$(date '+%Y-%m-%d %H:%M:%S') status: $status"
        if [[ "$status" == "READY" ]]; then
            break
        fi
        sleep 20
    done
}

itap_args=" --s3_input=s3a://purdue-fsdump-bucket/itap/1m --agg_file=itap.agg2.csv --cmode="dd" --run=1 "
nersc_args=" --s3_input=s3a://nersc-fsdump-bucket/1mgen2 --agg_file=nersc.agg2.csv --prefix_min=0 --prefix_max=5 --cmode="dd" --run=1 "
hpss_args=" --s3_input=s3a://purdue-fsdump-bucket/hpss/1m --agg_file=hpss.agg2.csv --cmode="dd" --run=1 "

./bmk-update-json.sh ${itap_args} --pipeline_name="PRI"
./bmk-update-kda.sh post 256
./kda-update.sh
wait_for_kda_ready

./kda-start.sh
sleep 3
wait_for_kda_ready

./bmk-update-json.sh ${itap_args} --pipeline_name="CNT" --execution_mode=batch
./bmk-update-kda.sh post 256
./kda-update.sh
wait_for_kda_ready

./kda-start.sh
sleep 3
wait_for_kda_ready

./bmk-update-json.sh ${itap_args} --pipeline_name="SND" --pipeline_args="usr|grp|dir"
./bmk-update-kda.sh post 256
./kda-update.sh
wait_for_kda_ready

./kda-start.sh
sleep 3
wait_for_kda_ready


# ./bmk-update-json.sh ${itap_args} --pipeline_name="SND" --pipeline_args="usr|grp" --cmode="dd" --run=2
# ./bmk-update-kda.sh post 256
# ./kda-update.sh
# wait_for_kda_ready

# ./kda-start.sh
# sleep 3
# wait_for_kda_ready
