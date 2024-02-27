#!/bin/bash
set -e
pegasus_lite_version_major="5"
pegasus_lite_version_minor="0"
pegasus_lite_version_patch="6"
pegasus_lite_enforce_strict_wp_check="true"
pegasus_lite_version_allow_wp_auto_download="true"


. pegasus-lite-common.sh

pegasus_lite_init

# cleanup in case of failures
trap pegasus_lite_signal_int INT
trap pegasus_lite_signal_term TERM
trap pegasus_lite_unexpected_exit EXIT

printf "\n########################[Pegasus Lite] Setting up workdir ########################\n"  1>&2
# work dir
export pegasus_lite_work_dir=$PWD
pegasus_lite_setup_work_dir

printf "\n##############[Pegasus Lite] Figuring out the worker package to use ##############\n"  1>&2
# figure out the worker package to use
pegasus_lite_worker_package

set -e

printf "\n########[Pegasus Lite] Writing out script to launch user task in container ########\n"  1>&2

cat <<EOF > init_env_prepare_input_files-cont.sh
#!/bin/sh
printf "\n#################[Container] Now in pegasus lite container script #################\n"  1>&2
set -e

# tmp dirs are handled by Singularity - don't use the ones from the host
unset TEMP
unset TMP
unset TMPDIR

# setting environment variables for job
HOME=/srv
export HOME
EOF
container_env /srv >> init_env_prepare_input_files-cont.sh
cat <<EOF2 >> init_env_prepare_input_files-cont.sh
pegasus_lite_version_major=$pegasus_lite_version_major
pegasus_lite_version_minor=$pegasus_lite_version_minor
pegasus_lite_version_patch=$pegasus_lite_version_patch
pegasus_lite_enforce_strict_wp_check=$pegasus_lite_enforce_strict_wp_check
pegasus_lite_version_allow_wp_auto_download=$pegasus_lite_version_allow_wp_auto_download
pegasus_lite_inside_container=true
export pegasus_lite_work_dir=/srv

cd /srv
. ./pegasus-lite-common.sh
pegasus_lite_init

printf "\n##############[Container] Figuring out Pegasus worker package to use ##############\n"  1>&2
# figure out the worker package to use
pegasus_lite_worker_package
printf "PATH in container is set to is set to \$PATH\n"  1>&2

printf "\n##################### Setting the xbit for executables staged #####################\n"  1>&2
# set the xbit for any executables staged
/bin/chmod +x init_env

set +e
job_ec=0
printf "\n#########################[Container] Launching user task #########################\n"  1>&2

pegasus-kickstart  -n init_env -N prepare_input_files -R condorpool  -L federated-learning-example-tracker -T 2024-02-20T19:12:32+00:00 ./init_env -f source_data -size 10000 -n global_model_round_init.h5 -s 784 -c 10
set -e
printf "\n################[Container] Exiting pegasus lite container script ################\n"  1>&2
EOF2


chmod +x init_env_prepare_input_files-cont.sh
if ! [ $pegasus_lite_start_dir -ef . ]; then
	cp $pegasus_lite_start_dir/pegasus-lite-common.sh . 
fi

set +e
singularity_init federated_learning_container.sif
job_ec=$(($job_ec + $?))

singularity exec --no-home --bind $PWD:/srv --env ENABLE_MLFLOW=True --env MLFLOW_EXPERIMENT_NAME=federated-learning-example-tracker --env MLFLOW_RUN=$PEGASUS_DAG_JOB_ID --env MLFLOW_TRACKING_URI=https://dagshub.com/swarmourr/FL-WF.mlflow --env MLFLOW_TRACKING_USERNAME=swarmourr --env MLFLOW_TRACKING_PASSWORD=8e68cdbad7d3e90f137cf248e4d3d29c2c790622 --env MLFLOW_CONFIG=auto --env FILE_TYPE=None  federated_learning_container.sif /srv/init_env_prepare_input_files-cont.sh 
job_ec=$(($job_ec + $?))


set -e


# clear the trap, and exit cleanly
trap - EXIT
pegasus_lite_final_exit

