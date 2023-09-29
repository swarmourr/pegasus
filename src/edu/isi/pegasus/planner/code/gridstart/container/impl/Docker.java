/**
 * Copyright 2007-2017 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.code.gridstart.container.impl;

import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * An interface to determine how a job gets wrapped to be launched on various containers, as a
 * shell-script snippet that can be embedded in PegasusLite
 *
 * @author vahi
 */
public class Docker extends Abstract {

    /**
     * The suffix for the shell script created on the remote worker node, that actually launches the
     * job in the container.
     */
    public static final String CONTAINER_JOB_LAUNCH_SCRIPT_SUFFIX = "-cont.sh";

    /** The directory in the container to be used as working directory */
    public static final String CONTAINER_WORKING_DIRECTORY = "/scratch";

    private static String WORKER_PACKAGE_SETUP_SNIPPET = null;

    /** time in seconds that we wait for before launching docker run command. */
    private static final String SLEEP_TIME_FOR_DOCKER_BOOTUP = "30";

    /** the variable that stores the path for the root user * */
    private static final String ROOT_PATH_VARIABLE_KEY = "root_path";

    /**
     * Initiailizes the Container shell wrapper
     *
     * @param bag @param dag
     */
    public void initialize(PegasusBag bag, ADag dag) {
        super.initialize(bag, dag);
    }

    /**
     * Returns the snippet to wrap a single job execution In this implementation we don't wrap with
     * any container, just plain shell invocation is returned.
     *
     * @param job
     * @return
     */
    public String wrap(Job job) {
        StringBuilder sb = new StringBuilder();

        sb.append("set -e").append("\n");

        // PM-1818 for the debug mode set -x
        if (this.mPegasusMode == PegasusProperties.PEGASUS_MODE.debug) {
            sb.append("set -x").append('\n');
        }

        // within the pegasus lite script create a wrapper
        // to launch job in the container. wrapper is required to
        // deploy pegasus worker package in the container and launch the user job
        String scriptName = job.getID() + Docker.CONTAINER_JOB_LAUNCH_SCRIPT_SUFFIX;
        sb.append(constructJobLaunchScriptInContainer(job, scriptName));

        sb.append("chmod +x ").append(scriptName).append("\n");

        // copy pegasus lite common from the directory where condor transferred it via it's file
        // transfer.
        sb.append("if ! [ $pegasus_lite_start_dir -ef . ]; then").append("\n");
        sb.append("\tcp $pegasus_lite_start_dir/pegasus-lite-common.sh . ").append("\n");
        sb.append("fi").append("\n");
        sb.append("\n");

        sb.append("set +e").append('\n'); // PM-701
        sb.append("job_ec=0").append("\n");

        // sets up the variables used for docker run command
        // FIXME docker_init has to be passed the name of the tar file?
        Container c = job.getContainer();
        sb.append("docker_init").append(" ").append(c.getLFN()).append("\n");

        sb.append("job_ec=$(($job_ec + $?))").append("\n").append("\n");

        // we want to sleep for few seconds to allow the container to boot up fully
        // sb.append( "sleep" ).append( " " ).append( Docker.SLEEP_TIME_FOR_DOCKER_BOOTUP ).append(
        // "\n" );

        // assume docker is available in path
        sb.append("docker run ");
        sb.append("--user root ");

        // directory where job is run is mounted as scratch
        sb.append("-v $PWD:").append(CONTAINER_WORKING_DIRECTORY).append(" ");

        // PM-1621 add --gpus all option if user has gpus requested with the job
        if (job.vdsNS.containsKey(Pegasus.GPUS_KEY)
                || job.condorVariables.containsKey(Condor.REQUEST_GPUS_KEY)) {
            sb.append("--gpus all").append(" ");
        }

        // PM-1298 mount any host directories if specified
        for (Container.MountPoint mp : c.getMountPoints()) {
            sb.append("-v ").append(mp).append(" ");
        }

        sb.append("-w=").append(CONTAINER_WORKING_DIRECTORY).append(" ");

        // PM-1524 set entry point for the container to /bin/sh
        sb.append("--entrypoint /bin/sh").append(" ");

        // PM-1626 incorporate any user specified extra arguments
        String extraArgs = job.vdsNS.getStringValue(Pegasus.CONTAINER_ARGUMENTS_KEY);
        if (extraArgs != null) {
            sb.append(extraArgs);
            sb.append(" ");
        }

        sb.append("--name $cont_name ");
        sb.append(" $cont_image ");

        // track

        // invoke the command to run as user who launched the job
        sb.append("-c ")
                .append("\"")
                .append("set -e ;")
                .append("export ")
                .append(ROOT_PATH_VARIABLE_KEY)
                .append("=\\$PATH ;") // PM-1630 preserve the path for root user
                .append("if ! grep -q -E  \"^$cont_group:\" /etc/group ; then ")
                .append("groupadd -f --gid $cont_groupid $cont_group ;")
                .append("fi; ")
                .append("if ! id $cont_user 2>/dev/null >/dev/null; then ")
                .append("   if id $cont_userid 2>/dev/null >/dev/null; then ")
                // PM-1809 the userid already exists. let the container os decide userid
                .append("       useradd -o --uid $cont_userid --gid $cont_groupid $cont_user; ")
                .append("   else ")
                .append("       useradd --uid $cont_userid --gid $cont_groupid $cont_user; ")
                .append("   fi; ")
                .append("fi; ")
                .append("su $cont_user -c ");
        sb.append("\\\"");
        sb.append("./").append(scriptName).append(" ");
        sb.append("\\\"");

        sb.append("\"");

        sb.append("\n");

        sb.append("job_ec=$(($job_ec + $?))").append("\n").append("\n");

        // remove the docker container
        sb.append("docker rm --force $cont_name ").append(" 1>&2").append("\n");
        sb.append("job_ec=$(($job_ec + $?))").append("\n").append("\n");

        return sb.toString();
    }

    /**
     * Returns the snippet to wrap a single job execution
     *
     * @param job
     * @return
     */
    public String wrap(AggregatedJob job) {
        String snippet = this.wrap((Job) job);

        // rest the jobs stdin
        job.setStdIn("");
        job.condorVariables.removeKey("input");

        return snippet;
    }

    /**
     * Return the description
     *
     * @return
     */
    public String describe() {
        return "Docker";
    }

    /**
     * Return the container package snippet. Construct the snippet that generates the shell script
     * responsible for setting up the worker package in the container and launch the job in the
     * container.
     *
     * @param job the job
     * @param scriptName basename of the script
     * @return
     */
    protected String constructJobLaunchScriptInContainer(Job job, String scriptName) {
        if (WORKER_PACKAGE_SETUP_SNIPPET == null) {
            WORKER_PACKAGE_SETUP_SNIPPET = Docker.constructContainerWorkerPackagePreamble();
        }
        StringBuilder sb = new StringBuilder();
        Container c = job.getContainer();

        sb.append("\n");
        appendStderrFragment(
                sb,
                Abstract.PEGASUS_LITE_MESSAGE_PREFIX,
                "Writing out script to launch user task in container");
        sb.append("\n");
        sb.append("cat <<EOF > ").append(scriptName).append("\n");

        sb.append("#!/bin/bash").append("\n");
        sb.append("set -e").append("\n");

        // set the job environment variables explicitly in the -cont.sh file
        sb.append("# setting environment variables for job").append('\n');
        ENV containerENVProfiles = (ENV) c.getProfilesObject().get(Profiles.NAMESPACES.env);
        boolean pathVariableSet = false;
        for (Iterator it = containerENVProfiles.getProfileKeyIterator(); it.hasNext(); ) {
            String key = (String) it.next();
            String value = (String) containerENVProfiles.get(key);
            pathVariableSet = key.equals("PATH");

            sb.append("export").append(" ").append(key).append("=");

            // check for env variables that are constructed based on condor job classds
            // such as CONDOR_JOBID=$(cluster).$(process). these are set by condor
            // and can only picked up from the shell when a job runs on a node
            // so we only set the key
            boolean fromShell = value.contains("$(");
            if (fromShell) {
                // append the $variable
                sb.append("=").append("$").append(key);
            } else {
                sb.append("\"").append(value).append("\"");
            }
            sb.append('\n');
        }

        if (!pathVariableSet) {
            // PM-1630 special handling for path varialble. if a user has not
            // associated a PATH variable with the job, we set the PATH to
            // root_path that stores the PATH set for the root user
            sb.append("export")
                    .append(" ")
                    .append("PATH")
                    .append("=")
                    .append("\\$")
                    .append(ROOT_PATH_VARIABLE_KEY);
            sb.append('\n');
        }

        // update and include runtime environment variables such as credentials
        sb.append("EOF\n");
        sb.append("container_env ")
                .append(Docker.CONTAINER_WORKING_DIRECTORY)
                .append(" >> ")
                .append(scriptName)
                .append("\n");
        sb.append("cat <<EOF2 >> ").append(scriptName).append("\n");

        if (WORKER_PACKAGE_SETUP_SNIPPET == null) {
            WORKER_PACKAGE_SETUP_SNIPPET = Docker.constructContainerWorkerPackagePreamble();
        }
        sb.append(WORKER_PACKAGE_SETUP_SNIPPET);

        sb.append(super.inputFilesToPegasusLite(job));

        // PM-1305 the integrity check should happen in the container
        sb.append(super.enableForIntegrity(job, Abstract.CONTAINER_MESSAGE_PREFIX));

        appendStderrFragment(sb, Abstract.CONTAINER_MESSAGE_PREFIX, "Launching user task");
        sb.append("\n");
        // sb.append( "\\$kickstart \"\\${original_args[@]}\" ").append( "\n" );

        if (job instanceof AggregatedJob) {
            try {
                // for clustered jobs we embed the contents of the input
                // file in the shell wrapper itself
                sb.append(job.getRemoteExecutable()).append(" ").append(job.getArguments());
                sb.append(" << CLUSTER").append('\n');

                // PM-833 figure out the job submit directory
                String jobSubmitDirectory =
                        new File(job.getFileFullPath(mSubmitDir, ".in")).getParent();

                sb.append(slurpInFile(jobSubmitDirectory, job.getStdIn()));
                sb.append("CLUSTER").append('\n');
            } catch (IOException ioe) {
                throw new RuntimeException(
                        "[Pegasus-Lite] Error while Docker wrapping job " + job.getID(), ioe);
            }
        } else {
            sb.append(job.getRemoteExecutable())
                    .append(" ")
                    .append(job.getArguments())
                    .append("\n");
        }

        sb.append("set -e").append('\n'); // PM-701
        sb.append(super.outputFilesToPegasusLite(job));

        sb.append("EOF2").append("\n");
        // appendStderrFragment( sb, "Writing out script to launch user TASK in docker container
        // (END)" );
        sb.append("\n");
        sb.append("\n");

        return sb.toString();
    }

    /**
     * Construct the snippet that generates the shell script responsible for setting up the worker
     * package in the container.
     *
     * @return
     */
    protected static String constructContainerWorkerPackagePreamble() {
        StringBuilder sb = new StringBuilder();

        sb.append("pegasus_lite_version_major=$pegasus_lite_version_major").append("\n");
        sb.append("pegasus_lite_version_minor=$pegasus_lite_version_minor").append("\n");
        sb.append("pegasus_lite_version_patch=$pegasus_lite_version_patch").append("\n");

        sb.append("pegasus_lite_enforce_strict_wp_check=$pegasus_lite_enforce_strict_wp_check")
                .append("\n");

        sb.append(
                        "pegasus_lite_version_allow_wp_auto_download=$pegasus_lite_version_allow_wp_auto_download")
                .append("\n");

        // PM-1875 we need to export the pegasus_lite_work_dir variable to
        // ensure pegasus-transfer picks from the environment
        sb.append("export pegasus_lite_work_dir=")
                .append(Docker.CONTAINER_WORKING_DIRECTORY)
                .append("\n");

        sb.append("\n");
        sb.append(". ./pegasus-lite-common.sh").append("\n");
        sb.append("pegasus_lite_init").append("\n").append("\n");

        sb.append("\n");
        appendStderrFragment(
                sb,
                Abstract.CONTAINER_MESSAGE_PREFIX,
                "Figuring out Pegasus worker package to use");

        sb.append("# figure out the worker package to use").append("\n");

        sb.append("pegasus_lite_worker_package").append("\n");

        sb.append("echo \"PATH in container is set to is set to \\$PATH\"")
                .append("  1>&2")
                .append("\n");
        sb.append("\n");

        return sb.toString();
    }
}
