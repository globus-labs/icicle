
itap_args=" --s3_input=s3a://icicle-fs-small/fs-small-1m --agg_file=itap.agg2.csv "
./bmk-update-json.sh ${itap_args} --pipeline_name="PRI"
# ./bmk-update-json.sh ${itap_args} --pipeline_name="CNT" --execution_mode=batch
# ./bmk-update-json.sh ${itap_args} --pipeline_name="SND" --pipeline_args="usr|grp|dir"

nersc_args=" --s3_input=s3a://icicle-fs-medium/fs-medium-1m --agg_file=nersc.agg2.csv --prefix_min=0 --prefix_max=5 "
# ./bmk-update-json.sh ${nersc_args} --pipeline_name="PRI"
# ./bmk-update-json.sh ${nersc_args} --pipeline_name="CNT" --execution_mode=batch
# ./bmk-update-json.sh ${nersc_args} --pipeline_name="SND" --pipeline_args="usr|grp"
# ./bmk-update-json.sh ${nersc_args} --pipeline_name="SND" --pipeline_args="dir"


hpss_args=" --s3_input=s3a://icicle-fs-large/fs-large-1m --agg_file=hpss.agg2.csv "
# ./bmk-update-json.sh ${hpss_args} --pipeline_name="PRI"
# ./bmk-update-json.sh ${hpss_args} --pipeline_name="CNT" --pipeline_args="usr|grp" --execution_mode=batch
# ./bmk-update-json.sh ${hpss_args} --pipeline_name="CNT" --pipeline_args="dir" --execution_mode=batch
# ./bmk-update-json.sh ${hpss_args} --pipeline_name="SND" --pipeline_args="usr|grp"
# ./bmk-update-json.sh ${hpss_args} --pipeline_name="SND" --pipeline_args="dir" --prefix_min=1 --prefix_max=3
# ./bmk-update-json.sh ${hpss_args} --pipeline_name="SND" --pipeline_args="dir" --prefix_min=3 --prefix_max=5

./bmk-update-kda.sh post 128
# ./bmk-update-kda.sh cicd 64
# ./bmk-update-kda.sh test 64
# ./bmk-update-kda.sh prod 16
