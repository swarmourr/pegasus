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
package edu.isi.pegasus.planner.selector.site;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.DataFlowJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.namespace.Selector;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.Iterator;
import java.util.List;

/**
 * The base class for the site selectors that want to map one job at a time.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class AbstractPerJob extends Abstract {

    /**
     * Maps the jobs in the workflow to the various grid sites.
     *
     * @param workflow the workflow in a Graph form.
     * @param sites the list of <code>String</code> objects representing the execution sites that
     *     can be used.
     */
    public void mapWorkflow(ADag workflow, List sites) {
        // iterate through the jobs in BFS
        for (Iterator it = workflow.iterator(); it.hasNext(); ) {
            GraphNode node = (GraphNode) it.next();

            Job job = (Job) node.getContent();

            // System.out.println( "Setting job level for " + job.getID() + " to " +
            // node.getDepth());
            job.setLevel(node.getDepth());

            // only map a job for which execute site hint
            // is not specified in the DAX
            String executionKey = Selector.EXECUTION_SITE_KEY;
            if (job.getSelectorProfiles().containsKey(executionKey)
                    || job.hints.containsKey(executionKey)) {
                mLogger.log(
                        "Job "
                                + job.getID()
                                + " will be mapped based on selector|hints profile key "
                                + executionKey,
                        LogManager.DEBUG_MESSAGE_LEVEL);
            } else {
                if (job instanceof DataFlowJob) {
                    // PM-1205 datalfows are clustered jobs
                    // we map the constitutent jobs not the datalfow job itself.
                    for (Iterator consIT = ((DataFlowJob) job).nodeIterator(); consIT.hasNext(); ) {
                        GraphNode n = (GraphNode) consIT.next();
                        Job j = (Job) n.getContent();
                        mapJob(j, sites);
                    }
                }

                mapJob(job, sites);
            }
        }
    }

    /**
     * Maps a job in the workflow to the various grid sites.
     *
     * @param job the job to be mapped.
     * @param sites the list of <code>String</code> objects representing the execution sites that
     *     can be used.
     */
    public abstract void mapJob(Job job, List sites);
}
