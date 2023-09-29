/**
 * Copyright 2007-2015 University Of Southern California
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
package edu.isi.pegasus.planner.common;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.DefaultStreamGobblerCallback;
import edu.isi.pegasus.common.util.FindExecutable;
import edu.isi.pegasus.common.util.StreamGobbler;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.code.generator.Braindump;
import edu.isi.pegasus.planner.refiner.ReplicaCatalogBridge;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Helper class to call out to pegasus-db-admin to check out the status of various Pegasus
 * databases.
 *
 * @author Karan Vahi
 */
public class PegasusDBAdmin {

    public static final String MASTER_DATABASE_PROPERTY_KEY = "pegasus.catalog.master.url";
    public static final String MASTER_DATABASE_DEPRECATED_PROPERTY_KEY = "pegasus.dashboard.output";

    public static final String WORKFLOW_DATABASE_PROPERTY_KEY = "pegasus.catalog.workflow.url";
    public static final String WORKFLOW_DATABASE_DEPRECATED_PROPERTY_KEY =
            "pegasus.monitord.output";

    private static enum DB_ADMIN_COMMAND {
        create,
        downgrade,
        update,
        check,
        version
    };

    private static enum DB_ADMIN_BACKEND {
        master,
        jdbcrc
    };
    /** */
    public static void updateProperties(PegasusBag bag, ADag workflow) {
        PegasusProperties properties = bag.getPegasusProperties();
        PlannerOptions options = bag.getPlannerOptions();
        String url = properties.getProperty(MASTER_DATABASE_PROPERTY_KEY);
        if (url == null) {
            // check for deprecated
            url = properties.getProperty(MASTER_DATABASE_DEPRECATED_PROPERTY_KEY);
        }

        if (url == null) {
            // construct default path
            // construct default sb for master workflow database
            StringBuilder sb = new StringBuilder();
            sb.append("sqlite:///")
                    .append(System.getProperty("user.home"))
                    .append(File.separator)
                    .append(".pegasus")
                    .append(File.separator)
                    .append("workflow.db");
            url = sb.toString();
        }

        // set the property back
        properties.setProperty(MASTER_DATABASE_PROPERTY_KEY, url);

        // update the workflow database url property
        url = properties.getProperty(WORKFLOW_DATABASE_PROPERTY_KEY);
        if (url == null) {
            // check for deprecated
            url = properties.getProperty(WORKFLOW_DATABASE_DEPRECATED_PROPERTY_KEY);
        }

        if (url == null) {
            Braindump bd = new Braindump();
            Map<String, String> entries = new HashMap();
            try {
                bd.initialize(bag);
                entries = bd.defaultBrainDumpEntries(workflow);
            } catch (CodeGeneratorException ex) {
                throw new RuntimeException("Error while generating default braindump entries ", ex);
            }
            String workflowDBBasename =
                    entries.get(Braindump.DAX_LABEL_KEY)
                            + "-"
                            + entries.get(Braindump.DAX_INDEX_KEY)
                            + ".stampede.db";
            // construct default path
            // construct default sb for master workflow database
            StringBuilder sb = new StringBuilder();
            sb.append("sqlite:///")
                    .append(options.getSubmitDirectory())
                    .append(File.separator)
                    .append(workflowDBBasename);
            url = sb.toString();
        }
        // set the property back
        properties.setProperty(WORKFLOW_DATABASE_PROPERTY_KEY, url);
    }

    private LogManager mLogger;
    private PegasusProperties mProps;

    public PegasusDBAdmin() {
        this(LogManagerFactory.loadSingletonInstance());
    }

    public PegasusDBAdmin(LogManager logger) {
        mLogger = logger;
    }

    /**
     * Calls out to the pegasus-db-admin tool to check and update master database if required.
     *
     * @param propertiesFile
     * @return
     */
    public boolean checkMasterDatabaseForVersionCompatibility(String propertiesFile) {
        StringBuilder arguments = new StringBuilder();
        arguments.append("-t master ").append("-c ").append(propertiesFile);

        return this.checkDatabase(
                DB_ADMIN_COMMAND.update.name(), arguments.toString(), DB_ADMIN_BACKEND.master);
    }

    /**
     * Calls out to the pegasus-db-admin tool to create the jdbcrc backend
     *
     * @param propertiesFile
     * @return
     */
    public boolean createJDBCRC(String propertiesFile) {
        StringBuilder arguments = new StringBuilder();
        if (propertiesFile == null) {
            return false;
        }
        File file = new File(propertiesFile);
        if (!file.exists() && !file.canRead()) {
            mLogger.log("Unable to access file " + file, LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
        // PM-1549 remap any output replica catalog properties to replica catalog properties
        // as command line options
        PegasusProperties props = PegasusProperties.nonSingletonInstance(propertiesFile);
        arguments.append(
                remapOutputRCProperties(props, ReplicaCatalogBridge.OUTPUT_REPLICA_CATALOG_PREFIX));
        arguments.append("-t jdbcrc ").append("-c ").append(propertiesFile);

        return this.checkDatabase(
                DB_ADMIN_COMMAND.create.name(), arguments.toString(), DB_ADMIN_BACKEND.jdbcrc);
    }

    /**
     * Calls out to the pegasus-db-admin tool to check for jdbrc compatibility.
     *
     * @param propertiesFile
     * @param prefix the property prefix to remap to replica catalog props
     * @return
     */
    public boolean checkJDBCRCForCompatibility(String propertiesFile, String prefix) {
        StringBuilder arguments = new StringBuilder();
        // PM-1549 remap any output replica catalog properties to replica catalog properties
        // as command line options
        PegasusProperties props = PegasusProperties.nonSingletonInstance(propertiesFile);
        if (prefix != null) {
            arguments.append(remapOutputRCProperties(props, prefix));
        }
        arguments.append("-t jdbcrc ").append("-c ").append(propertiesFile);
        // debug turned on
        // arguments.append(" --debug ");
        return this.checkDatabase(
                DB_ADMIN_COMMAND.check.name(), arguments.toString(), DB_ADMIN_BACKEND.jdbcrc);
    }

    /**
     * checks the database backend
     *
     * @param dbCommand
     * @param checkDBArguments
     * @param backendType the backend type that is being setup.
     * @return
     */
    public boolean checkDatabase(
            String dbCommand, String checkDBArguments, DB_ADMIN_BACKEND backendType) {
        String basename = "pegasus-db-admin";
        File pegasusDBAdmin = FindExecutable.findExec(basename);
        if (pegasusDBAdmin == null) {
            throw new RuntimeException("Unable to find path to " + basename);
        }

        // construct arguments for pegasus-db-admin
        StringBuffer args = new StringBuffer();
        args.append(dbCommand);
        args.append(" ").append(checkDBArguments);
        String command = pegasusDBAdmin.getAbsolutePath() + " " + args;
        mLogger.log("Executing  " + command, LogManager.DEBUG_MESSAGE_LEVEL);

        try {
            // set the callback and run the pegasus-run command
            Runtime r = Runtime.getRuntime();
            Process p = r.exec(command);

            // spawn off the gobblers with the already initialized default callback
            StreamGobbler ips =
                    new StreamGobbler(
                            p.getInputStream(),
                            new DefaultStreamGobblerCallback(LogManager.CONSOLE_MESSAGE_LEVEL));
            StreamGobbler eps =
                    new StreamGobbler(
                            p.getErrorStream(),
                            new DefaultStreamGobblerCallback(LogManager.ERROR_MESSAGE_LEVEL));

            ips.start();
            eps.start();

            // wait for the threads to finish off
            ips.join();
            eps.join();

            // get the status
            int status = p.waitFor();

            mLogger.log(basename + " exited with status " + status, LogManager.DEBUG_MESSAGE_LEVEL);

            if (status != 0) {
                StringBuilder message = new StringBuilder();
                message.append(command).append(" failed with status ").append(status).append(" .");
                if (backendType == DB_ADMIN_BACKEND.master) {
                    message.append(
                                    " Pegasus was unable to update the the worflow database file found at")
                            .append(
                                    " ~/.pegasus/workflow.db . If this file is corrupted, a solution for")
                            .append(
                                    " problem is to remove the file with the command: rm -f ~/.pegasus/workflow.db ")
                            .append(
                                    " - but note that doing so will remove old workflows from the Pegasus Dashboard.");
                } else if (backendType == DB_ADMIN_BACKEND.jdbcrc) {
                    message.append(
                                    "Pegasus was unable to update the the JDBCRC Backend using the command ")
                            .append(command);
                }
                throw new RuntimeException(message.toString());
            }
        } catch (IOException ioe) {
            mLogger.log(
                    "IOException while executing " + basename, ioe, LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException("IOException while executing " + command, ioe);
        } catch (InterruptedException ie) {
            // ignore
        }

        return true;
    }

    /**
     * Remaps properties with pegasus.catalog.replica.output prefix to pegasus.catalog.replica and
     * creates an argument string where the remapped properties are passed on the command line
     *
     * @param props
     * @param prefix the property prefix to remap to replica catalog props
     * @return
     */
    protected String remapOutputRCProperties(PegasusProperties props, String prefix) {
        StringBuilder arguments = new StringBuilder();
        // PM-1549 check if a separate output replica catalog is specified
        Properties output = props.remap(prefix, ReplicaCatalog.c_prefix);
        if (!output.isEmpty()) {
            // we translate the properties to pegasus.catalog.replica prefix and add
            // them to the command line invocation before the conf properties
            // are passed
            for (String property : output.stringPropertyNames()) {
                arguments
                        .append("-D")
                        .append(property)
                        .append("=")
                        .append(output.getProperty(property))
                        .append(" ");
            }
        }
        return arguments.toString();
    }
}
