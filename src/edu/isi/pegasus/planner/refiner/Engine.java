/**
 * Copyright 2007-2008 University Of Southern California
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
package edu.isi.pegasus.planner.refiner;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.SubmitMapper;

/**
 * The class which is a superclass of all the various Engine classes. It defines common methods and
 * member variables.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public abstract class Engine {

    // constants
    public static final String REGISTRATION_UNIVERSE = "registration";
    public static final String TRANSFER_UNIVERSE = "transfer";

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /**
     * The handle to the Transformation Catalog. It must be instantiated in the implementing class.
     */
    protected TransformationCatalog mTCHandle;

    /** The handle to the Pool Info Provider. It is instantiated in this class */
    protected SiteStore mSiteStore;

    /** Contains the message which is to be logged by Pegasus. */
    protected String mLogMsg = "";

    /**
     * Defines the read mode for transformation catalog. Whether we want to read all at once or as
     * desired.
     *
     * @see org.griphyn.common.catalog.transformation.TCMode
     */
    protected String mTCMode;

    /** The logging object which is used to log all the messages. */
    protected LogManager mLogger;

    /** Contains the various options to the Planner as passed by the user at runtime. */
    protected PlannerOptions mPOptions;

    /** The bag of initialization objects */
    protected PegasusBag mBag;

    /**
     * Handle to the Submit directory factory, that returns the relative submit directory for a job
     */
    protected SubmitMapper mSubmitDirMapper;

    /** @param bag bag of initialization objects */
    public Engine(PegasusBag bag) {
        mBag = bag;
        mLogger = bag.getLogger();
        mProps = bag.getPegasusProperties();
        mPOptions = bag.getPlannerOptions();
        mTCHandle = bag.getHandleToTransformationCatalog();
        mSiteStore = bag.getHandleToSiteStore();
        mSubmitDirMapper = bag.getSubmitMapper();
        loadProperties();
    }

    /** Loads all the properties that are needed by the Engine classes. */
    public void loadProperties() {}

    /**
     * Complains for head node url prefix not specified
     *
     * @param refiner the name of the refiner
     * @param site the site handle
     * @param operation the file server operation
     * @throws RuntimeException when URL Prefix cannot be determined for various reason.
     */
    protected void complainForHeadNodeURLPrefix(
            String refiner, String site, FileServer.OPERATION operation) {
        this.complainForHeadNodeURLPrefix(refiner, site, operation, null);
    }

    /**
     * Complains for head node url prefix not specified
     *
     * @param refiner the name of the refiner
     * @param operation the operation for which error is throw
     * @param job the related job if any
     * @param site the site handle
     * @throws RuntimeException when URL Prefix cannot be determined for various reason.
     */
    protected void complainForHeadNodeURLPrefix(
            String refiner, String site, FileServer.OPERATION operation, Job job) {
        StringBuffer error = new StringBuffer();
        error.append("[").append(refiner).append("] ");
        if (job != null) {
            error.append("For job (").append(job.getID()).append(").");
        }
        error.append("Unable to determine URL Prefix for the FileServer ")
                .append(" for operation ")
                .append(operation)
                .append(" for shared scratch file system on site: ")
                .append(site);
        throw new RuntimeException(error.toString());
    }
}
