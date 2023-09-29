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
public class Singularity extends Abstract {

    /**
     * The suffix for the shell script created on the remote worker node, that actually launches the
     * job in the container.
     */
    public static final String CONTAINER_JOB_LAUNCH_SCRIPT_SUFFIX = "-cont.sh";

    /** The directory in the container to be used as working directory */
    public static final String CONTAINER_WORKING_DIRECTORY = "/srv";

    private static String WORKER_PACKAGE_SETUP_SNIPPET = null;

    /**
     * Initiailizes the Container shell wrapper
     *
     * @param bag
     * @param dag
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
        String scriptName = job.getID() + Singularity.CONTAINER_JOB_LAUNCH_SCRIPT_SUFFIX;
        sb.append(constructJobLaunchScriptInContainer(job, scriptName));

        sb.append("chmod +x ").append(scriptName).append("\n");

        // copy pegasus lite common from the directory where condor transferred it via it's file
        // transfer.
        sb.append("if ! [ $pegasus_lite_start_dir -ef . ]; then").append("\n");
        sb.append("\tcp $pegasus_lite_start_dir/pegasus-lite-common.sh . ").append("\n");
        sb.append("fi").append("\n");
        sb.append("\n");

        sb.append("set +e").append("\n");

        Container c = job.getContainer();
        sb.append("singularity_init").append(" ").append(c.getLFN()).append("\n");

        sb.append("job_ec=$(($job_ec + $?))").append("\n").append("\n");
        ;

        // assume singularity is available in path
        sb.append("singularity exec").append(" ");

        // do not mount home - this might not exists when running under for example the nobody user
        sb.append("--no-home").append(" ");

        // PM-1621 add --nv option if user has gpus requested with the job
        if (job.vdsNS.containsKey(Pegasus.GPUS_KEY)
                || job.condorVariables.containsKey(Condor.REQUEST_GPUS_KEY)) {
            sb.append("--nv").append(" ");
        }

        // exec --bind $PWD:/srv
        sb.append("--bind $PWD:").append(CONTAINER_WORKING_DIRECTORY).append(" ");

        // PM-1298 mount any host directories if specified
        for (Container.MountPoint mp : c.getMountPoints()) {
            sb.append("--bind ").append(mp).append(" ");
        }

        // PM-1626 incorporate any user specified extra arguments
        String extraArgs = job.vdsNS.getStringValue(Pegasus.CONTAINER_ARGUMENTS_KEY);
        if (extraArgs != null) {
            sb.append(extraArgs);
            sb.append(" ");
        }

        // we are running directly against image file. no loading
        sb.append(c.getLFN()).append(" ");

        // the script that sets up pegasus worker package and execute
        // user application
        sb.append("/srv/").append(scriptName).append(" ");

        sb.append("\n");

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
        return "Singularity";
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
            WORKER_PACKAGE_SETUP_SNIPPET = Singularity.constructContainerWorkerPackagePreamble();
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
        // basic shell as some containers only has dash and not bash
        sb.append("#!/bin/sh").append("\n");
        appendStderrFragment(
                sb, Abstract.CONTAINER_MESSAGE_PREFIX, "Now in pegasus lite container script");
        sb.append("set -e").append("\n");

        sb.append("\n");
        sb.append("# tmp dirs are handled by Singularity - don't use the ones from the host\n");
        sb.append("unset TEMP\n");
        sb.append("unset TMP\n");
        sb.append("unset TMPDIR\n");
        sb.append("\n");

        // set the job environment variables explicitly in the -cont.sh file
        sb.append("# setting environment variables for job").append('\n');
        sb.append("HOME=/srv").append('\n');
        sb.append("export HOME").append('\n');
        ENV containerENVProfiles = (ENV) c.getProfilesObject().get(Profiles.NAMESPACES.env);
        for (Iterator it = containerENVProfiles.getProfileKeyIterator(); it.hasNext(); ) {
            String key = (String) it.next();
            String value = (String) containerENVProfiles.get(key);
            sb.append(key).append("=");

            // check for env variables that are constructed based on condor job classds
            // such asCONDOR_JOBID=$(cluster).$(process). these are set by condor
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
            sb.append("export").append(" ").append(key).append('\n');
        }

        // update and include runtime environment variables such as credentials
        sb.append("EOF\n");
        sb.append("container_env ")
                .append(Singularity.CONTAINER_WORKING_DIRECTORY)
                .append(" >> ")
                .append(scriptName)
                .append("\n");
        sb.append("cat <<EOF2 >> ").append(scriptName).append("\n");

        // PM-1214 worker package setup in container should happen after
        // the environment variables have been set.
        if (WORKER_PACKAGE_SETUP_SNIPPET == null) {
            WORKER_PACKAGE_SETUP_SNIPPET = Singularity.constructContainerWorkerPackagePreamble();
        }
        sb.append(WORKER_PACKAGE_SETUP_SNIPPET);

        sb.append(super.inputFilesToPegasusLite(job));

        // PM-1305 the integrity check should happen in the container
        sb.append(super.enableForIntegrity(job, Abstract.CONTAINER_MESSAGE_PREFIX));

        sb.append("set +e").append('\n'); // PM-701
        sb.append("job_ec=0").append("\n");

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
                        "[Pegasus-Lite] Error while Singularity wrapping job " + job.getID(), ioe);
            }
        } else {
            sb.append(job.getRemoteExecutable())
                    .append(" ")
                    .append(job.getArguments())
                    .append("\n");
        }

        sb.append("set -e").append('\n'); // PM-701
        sb.append(super.outputFilesToPegasusLite(job));

        appendStderrFragment(
                sb, Abstract.CONTAINER_MESSAGE_PREFIX, "Exiting pegasus lite container script");
        sb.append("EOF2").append("\n");
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
        sb.append("pegasus_lite_inside_container=true").append("\n");

        // PM-1875 we need to export the pegasus_lite_work_dir variable to
        // ensure pegasus-transfer picks from the environment
        sb.append("export pegasus_lite_work_dir=")
                .append(Singularity.CONTAINER_WORKING_DIRECTORY)
                .append("\n")
                .append("\n");

        sb.append("cd /srv").append("\n");

        sb.append(". ./pegasus-lite-common.sh").append("\n");
        sb.append("pegasus_lite_init").append("\n");

        sb.append("\n");
        appendStderrFragment(
                sb,
                Abstract.CONTAINER_MESSAGE_PREFIX,
                "Figuring out Pegasus worker package to use");
        sb.append("# figure out the worker package to use").append("\n");

        sb.append("pegasus_lite_worker_package").append("\n");

        sb.append("printf \"PATH in container is set to is set to \\$PATH\\n\"")
                .append("  1>&2")
                .append("\n");
        sb.append("\n");

        return sb.toString();
    }
}
