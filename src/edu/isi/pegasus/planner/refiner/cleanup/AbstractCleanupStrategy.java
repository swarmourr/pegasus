/**
 * Copyright 2007-2016 University Of Southern California
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
package edu.isi.pegasus.planner.refiner.cleanup;

import static edu.isi.pegasus.planner.refiner.ReplicaCatalogBridge.CACHE_REPLICA_CATALOG_IMPLEMENTER;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.refiner.ReplicaCatalogBridge;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author Arun Ramakrishnan
 * @author Karan Vahi
 * @author Rafael Ferreira da Silva
 */
public abstract class AbstractCleanupStrategy implements CleanupStrategy {

    /**
     * The prefix for CLEANUP_JOB ID i.e prefix+the parent compute_job ID becomes ID of the cleanup
     * job.
     */
    public static final String CLEANUP_JOB_PREFIX = "clean_up_";

    /** The default value for the maxjobs variable for the category of cleanup jobs. */
    public static final String DEFAULT_MAX_JOBS_FOR_CLEANUP_CATEGORY = "4";

    /** If user has not specified any value themselves */
    protected static final int NO_PROFILE_VALUE = -1;

    public static final String SCALING_MESSAGE =
            "Pegasus now has a strategy for scaling cleanup jobs based on size of workflow. "
                    + "Consider removing the property pegasus.file.cleanup.clusters.num";

    /**
     * A metadata key indicating the source site from which an input file is being retrieved. Only
     * associated for a DAXJob/SubWorkflow job
     */
    protected static final String CLEANUP_SOURCE_SITE_KEY = "cleanup_source_site";

    /**
     * The mapping to siteHandle to all the jobs that are mapped to it mapping to siteHandle(String)
     * to Set<GraphNodes>
     */
    protected HashMap mResMap;

    /**
     * The mapping of siteHandle to all subset of the jobs mapped to it that are leaves in the
     * workflow mapping to siteHandle(String) to Set<GraphNodes>.
     */
    protected HashMap mResMapLeaves;

    /**
     * The mapping of siteHandle to all subset of the jobs mapped to it that are roots in the
     * workflow mapping to siteHandle(String) to Set<GraphNodes>.
     */
    protected HashMap mResMapRoots;

    /**
     * The max depth of any job in the workflow useful for a priorityQueue implementation in an
     * array
     */
    protected int mMaxDepth;

    /** HashSet of Files that should not be cleaned up */
    protected HashSet mDoNotClean;

    /** The handle to the CleanupImplementation instance that creates the jobs for us. */
    protected CleanupImplementation mImpl;

    /** The handle to the properties passed to Pegasus. */
    protected PegasusProperties mProps;

    /** The handle to the logging object used for logging. */
    protected LogManager mLogger;

    /** The number of cleanup jobs per level to be created */
    protected int mCleanupJobsPerLevel;

    /** the number of cleanup jobs clustered into a clustered cleanup job */
    protected int mCleanupJobsSize;

    /** A boolean indicating whether we prefer use the size factor or the num factor */
    protected boolean mUseSizeFactor;

    protected PegasusBag mBag;

    /**
     * Intializes the class.
     *
     * @param bag bag of initialization objects
     * @param impl the implementation instance that creates cleanup job
     */
    @Override
    public void initialize(PegasusBag bag, CleanupImplementation impl) {
        mBag = bag;
        mProps = bag.getPegasusProperties();
        mLogger = bag.getLogger();

        mImpl = impl;

        // intialize the internal structures
        mResMap = new HashMap();
        mResMapLeaves = new HashMap();
        mResMapRoots = new HashMap();
        mDoNotClean = new HashSet();
        mMaxDepth = 0;

        mUseSizeFactor = false;

        // set the default value for maxjobs only if not specified
        // in the properties
        String key = this.getDefaultCleanupMaxJobsPropertyKey();
        if (this.mProps.getProperty(key) == null) {
            mLogger.log(
                    "Setting property "
                            + key
                            + " to  "
                            + DEFAULT_MAX_JOBS_FOR_CLEANUP_CATEGORY
                            + " to set max jobs for cleanup jobs category",
                    LogManager.CONFIG_MESSAGE_LEVEL);
            mProps.setProperty(key, DEFAULT_MAX_JOBS_FOR_CLEANUP_CATEGORY);
        }

        mCleanupJobsPerLevel = NO_PROFILE_VALUE;
        String propValue = mProps.getMaximumCleanupJobsPerLevel();
        int value = -1;
        try {
            value = Integer.parseInt(propValue);
        } catch (Exception e) {
            // ignore
        }

        if (value > 0) {
            // user has specified a value for the clustered cleanup
            mCleanupJobsPerLevel = value;
        } else {
            // check if a user has
            propValue = mProps.getClusterSizeCleanupJobsPerLevel();
            int clusterSize = -1;
            try {
                clusterSize = Integer.parseInt(propValue);
            } catch (Exception e) {
                // ignore
            }
            if (clusterSize > 0) {
                // set the algorithm to use it
                mUseSizeFactor = true;
                mCleanupJobsSize = clusterSize;
                mLogger.log(
                        "Cluster Size of cleanup jobs  " + mCleanupJobsSize,
                        LogManager.CONFIG_MESSAGE_LEVEL);
            } else {
                // PM-1212 no hardcoded default value for number of clustered cleanup jobs
                // instead we compute based on levels
                mCleanupJobsPerLevel = NO_PROFILE_VALUE;
            }
        }
        if (!mUseSizeFactor && mCleanupJobsPerLevel != NO_PROFILE_VALUE) {
            // log a config message for the number of cleanup jobs
            mLogger.log(
                    "Maximum number of cleanup jobs to be created per level "
                            + mCleanupJobsPerLevel,
                    LogManager.CONFIG_MESSAGE_LEVEL);
            // PM-1212 log a message telling users to consider disabling the knob
            mLogger.log(SCALING_MESSAGE, LogManager.INFO_MESSAGE_LEVEL);
        }
    }

    /**
     * Adds cleanup jobs to the workflow.
     *
     * @param workflow the workflow to add cleanup jobs to.
     * @return the workflow with cleanup jobs added to it.
     */
    @Override
    public Graph addCleanupJobs(Graph workflow) {
        // reset the internal data structures
        reset();

        // add the priorities to all the jobs
        // applyJobPriorities( workflow );
        // determine the files that should not be removed from the resource where it is produced
        // i.e file A produced by job J should not be removed if J does not have a stage out job
        // and A has getTransientTransferFlag() set to false
        for (Iterator it = workflow.nodeIterator(); it.hasNext(); ) {
            GraphNode _GN = (GraphNode) it.next();
            Job _SI = (Job) _GN.getContent();

            // only for compute jobs and sub workflow jobs we proceed ahead
            if (!(_SI.getJobType() == _SI.COMPUTE_JOB /*|| _SI.getJobType() == _SI.DAX_JOB*/)) {
                // System.err.println(" Skipping job" + _SI.getID() + " of type " +
                // _SI.getJobType());
                continue;
            }

            // if the compute job has a stage out job then all the files produced by it can be
            // removed
            // so , skip such cases
            boolean job_has_stageout = false;
            for (Iterator itjc = _GN.getChildren().iterator(); itjc.hasNext(); ) {
                Job _SIchild = (Job) ((GraphNode) itjc.next()).getContent();
                if (_SIchild.getJobType() == _SIchild.STAGE_OUT_JOB) {
                    job_has_stageout = true;
                    break;
                }
            }
            if (job_has_stageout) {
                continue;
            }

            // else add files with getTransientTransferFlag() set to false to the do_not_clean List
            Set _ofiles = _SI.getOutputFiles();
            for (Iterator itof = _ofiles.iterator(); itof.hasNext(); ) {
                PegasusFile of = (PegasusFile) itof.next();
                if (of.getTransientTransferFlag() == false) {
                    this.mDoNotClean.add(of);
                }
            }
        }

        //        mLogger.log( "The input workflow " + workflow,
        //                     LogManager.DEBUG_MESSAGE_LEVEL );
        // set the depth and ResMap values iteratively
        setDepth_ResMap(workflow.getRoots());
        mLogger.log("Number of sites " + mResMap.size(), LogManager.DEBUG_MESSAGE_LEVEL);

        // output for debug
        StringBuffer message = new StringBuffer();
        for (Iterator it = mResMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            message.append("Site ")
                    .append((String) entry.getKey())
                    .append(" count jobs = ")
                    .append(((Set) entry.getValue()).size());
            mLogger.log(message.toString(), LogManager.DEBUG_MESSAGE_LEVEL);

            Set whatever = (Set) entry.getValue();
            for (Iterator weit = whatever.iterator(); weit.hasNext(); ) {
                mLogger.log(
                        "* " + ((GraphNode) weit.next()).getID(), LogManager.DEBUG_MESSAGE_LEVEL);
            }
            message = new StringBuffer();
        }

        return workflow;
    }

    /** Resets the internal data structures. */
    protected void reset() {
        mResMap.clear();
        mResMapLeaves.clear();
        mResMapRoots.clear();
        mMaxDepth = 0;
    }

    /**
     * A BFS implementation to set depth value (roots have depth 1) and also to populate mResMap
     * ,mResMapLeaves,mResMapRoots which contains all the jobs that are assigned to a particular
     * resource
     *
     * @param roots List of GraphNode objects that are roots
     */
    private void setDepth_ResMap(List roots) {
        LinkedList que = new LinkedList();
        que.addAll(roots);

        for (int i = 0; i < que.size(); i++) {
            ((GraphNode) que.get(i)).setDepth(1);
        }

        while (que.size() >= 1) {
            GraphNode curGN = (GraphNode) que.removeFirst();

            // debug
            /*
            System.out.print(curGN.getDepth() +" "+((Job)curGN.getContent()).getSiteHandle()+" ");
            if( curGN.getChildren() == null )
                System.out.print("0");
            else
                System.out.print( curGN.getChildren().size() );
             */
            // populate mResMap ,mResMapLeaves,mResMapRoots
            Job si = (Job) curGN.getContent();

            Set<String> sites = getSitesForCleanup(si);
            for (String site : sites) {
                if (!mResMap.containsKey(site)) {
                    mResMap.put(site, new HashSet());
                }
                ((Set) mResMap.get(site)).add(curGN);
            }

            // System.out.println( "  site count="+((Set)mResMap.get( si.getSiteHandle() )).size()
            // );
            // now set the depth
            for (Iterator it = curGN.getChildren().iterator(); it.hasNext(); ) {
                GraphNode child = (GraphNode) it.next();
                if (!(child.getDepth() == -1 || child.getDepth() < curGN.getDepth() + 1)) {
                    continue;
                }

                child.setDepth(curGN.getDepth() + 1);
                if (child.getDepth() > mMaxDepth) {
                    mMaxDepth = child.getDepth();
                }
                que.addLast(child);
            }
        }
    }

    /**
     * Checks to see if job type is a stageout job type.
     *
     * @param type the type of the job.
     * @return boolean
     */
    protected boolean typeStageOut(int type) {
        return (type == Job.STAGE_OUT_JOB || type == Job.INTER_POOL_JOB);
    }

    /**
     * Returns sites to be used for the cleanup algorithm. For compute jobs the staging site is
     * used, while for stageout jobs non third party sites is used. For sub workflow jobs, more than
     * one site can be associated.
     *
     * <p>For all other jobs the execution site is used.
     *
     * @param job the job
     * @return the site to be used
     */
    protected Set<String> getSitesForCleanup(Job job) {
        /*
        String site =  typeStageOut( job.getJobType() )?
                             ((TransferJob)job).getNonThirdPartySite():
                             job.getStagingSiteHandle();
         */
        Set<String> sites = new HashSet();

        if (typeStageOut(job.getJobType())) {
            // for stage out jobs we prefer the non third party site
            sites.add(((TransferJob) job).getNonThirdPartySite());
        } else if (job.getJobType() == Job.COMPUTE_JOB) {
            // for compute jobs we refer to the staging site
            sites.add(job.getStagingSiteHandle());
        } else if (job.getJobType() == Job.DAX_JOB) {
            return this.getSitesForCleanup((DAXJob) job);
        } else {
            // for all other jobs we use the execution site
            sites.add(job.getSiteHandle());
        }
        return sites;
    }

    /**
     * Returns sites to be used in the cleanup algorithm for a sub workflow job
     *
     * @param job the sub workflow job
     * @return the site to be used
     */
    protected Set<String> getSitesForCleanup(DAXJob job) {
        /*
        String site =  typeStageOut( job.getJobType() )?
                             ((TransferJob)job).getNonThirdPartySite():
                             job.getStagingSiteHandle();
         */
        Set<String> sites = new HashSet();
        String cacheFile = job.getInputWorkflowCacheFile();
        // for DAX jobs we look at the site based on if any
        // inputs of DAX job are being retreived from a parent
        // job. For this look into the sub workflow input cache file
        if (cacheFile == null) {
            // PM-1918 just return the execution set for the job. that
            // is what it was for pegasus releases <= 5.0.5
            sites.add(job.getSiteHandle());
            return sites;
        }

        Properties cacheProps =
                mProps.getVDSProperties().matchingSubset(ReplicaCatalog.c_prefix, false);
        // all cache files are loaded in readonly mode
        cacheProps.setProperty(ReplicaCatalogBridge.CACHE_READ_ONLY_KEY, "true");
        // set the appropriate property to designate path to file
        cacheProps.setProperty(ReplicaCatalogBridge.CACHE_REPLICA_CATALOG_KEY, cacheFile);
        mLogger.log(
                "Loading sub workflow input cache file: " + cacheFile,
                LogManager.DEBUG_MESSAGE_LEVEL);
        ReplicaCatalog simpleFile = null;
        try {
            simpleFile =
                    ReplicaFactory.loadInstance(
                            CACHE_REPLICA_CATALOG_IMPLEMENTER, this.mBag, cacheProps);
            Map<String, Collection<ReplicaCatalogEntry>> m = simpleFile.lookup(new HashMap());

            for (PegasusFile pf : job.getInputFiles()) {
                String lfn = pf.getLFN();
                Collection<ReplicaCatalogEntry> rces = simpleFile.lookup(lfn);
                // PM-1918 source site is the site from where an input file is being retrieved.
                // DAXJob/Sub workflow jobs can be associated with multiple sites
                // that it needs to be mapped. one is the execution site. other is
                // where the parents job have placed their outputs that sub workflow
                // requries as input on their data staging site. we retrieve these
                // locations from parsing the sub workflow input cache file
                String sourceSite = job.getSiteHandle();
                for (ReplicaCatalogEntry rce : rces) {
                    sourceSite = rce.getResourceHandle();
                    mLogger.log(
                            "For sub workflow "
                                    + job.getID()
                                    + " "
                                    + lfn
                                    + " retrieved from site "
                                    + sourceSite,
                            LogManager.TRACE_MESSAGE_LEVEL);
                    sites.add(sourceSite);
                }
                pf.addMetadata(CLEANUP_SOURCE_SITE_KEY, sourceSite);
            }
        } catch (Exception e) {
            mLogger.log(
                    "Unable to load cache file " + cacheFile, e, LogManager.ERROR_MESSAGE_LEVEL);
        } finally {
            if (simpleFile != null) {
                simpleFile.close();
            }
        }

        // if sites is empty then we default to previous behavior where
        // we just take the execution site (for dax jobs it is local)
        if (sites.isEmpty()) {
            sites.add(job.getSiteHandle());
        }

        return sites;
    }

    /**
     * Returns the property key that can be used to set the max jobs for the default category
     * associated with the registration jobs.
     *
     * @return the property key
     */
    public String getDefaultCleanupMaxJobsPropertyKey() {
        StringBuilder key = new StringBuilder();

        key.append(Dagman.NAMESPACE_NAME)
                .append(".")
                .append(CleanupImplementation.DEFAULT_CLEANUP_CATEGORY_KEY)
                .append(".")
                .append(Dagman.MAXJOBS_KEY.toLowerCase());

        return key.toString();
    }
}
