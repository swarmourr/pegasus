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

cat <<EOF > data_tracker_tracker_data_job_local_clustering_worker-1_novalocal_output-cont.sh
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
container_env /srv >> data_tracker_tracker_data_job_local_clustering_worker-1_novalocal_output-cont.sh
cat <<EOF2 >> data_tracker_tracker_data_job_local_clustering_worker-1_novalocal_output-cont.sh
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
/bin/chmod +x data_tracker

set +e
job_ec=0
printf "\n#########################[Container] Launching user task #########################\n"  1>&2

pegasus-kickstart  -n data_tracker -N tracker_data_job_local_clustering_worker-1.novalocal_output -R condorpool  -s pegasus-data/metadata_local_clustering_worker-1.novalocal_output.yaml=pegasus-data/metadata_local_clustering_worker-1.novalocal_output.yaml -L federated-learning-example-tracker -T 2024-02-20T14:02:07+00:00 ./data_tracker -files worker-1.novalocal_clusters.csv -data_dir pegasus-data -metadata_format yaml -o pegasus-data/metadata_local_clustering_worker-1.novalocal_output -file_type outputs -pfn /home/poseidon/pegasus-integration/pegasus/packages/pegasus-api/test/workflow-example/worker-1.novalocal_clusters.csv -gdrive -remote_id 1KLUNrJNfCDI8Y5iXBmASUbtwqCpaHg3L -gcredentials cred
set -e
printf "\n################[Container] Exiting pegasus lite container script ################\n"  1>&2
EOF2


chmod +x data_tracker_tracker_data_job_local_clustering_worker-1_novalocal_output-cont.sh
if ! [ $pegasus_lite_start_dir -ef . ]; then
	cp $pegasus_lite_start_dir/pegasus-lite-common.sh . 
fi

set +e
singularity_init federated_learning_container.sif
job_ec=$(($job_ec + $?))

singularity exec --no-home --bind $PWD:/srv --env ENABLE_MLFLOW=True --env MLFLOW_EXPERIMENT_NAME=federated-learning-example-tracker --env MLFLOW_RUN=$PEGASUS_DAG_JOB_ID --env MLFLOW_TRACKING_URI=https://dagshub.com/swarmourr/FL-WF.mlflow --env MLFLOW_TRACKING_USERNAME=swarmourr --env MLFLOW_TRACKING_PASSWORD=8e68cdbad7d3e90f137cf248e4d3d29c2c790622 --env MLFLOW_CONFIG=auto --env FILE_TYPE=None  federated_learning_container.sif /srv/data_tracker_tracker_data_job_local_clustering_worker-1_novalocal_output-cont.sh 
job_ec=$(($job_ec + $?))


set -e


# clear the trap, and exit cleanly
trap - EXIT
pegasus_lite_final_exit

