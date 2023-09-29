/**
 * Copyright 2007-2020 University Of Southern California
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
package edu.isi.pegasus.planner.classes;

import java.util.HashMap;
import java.util.Map;

/**
 * Keywords used in the Workflow YAML Schema starting Pegasus 5.0 version
 *
 * @author Karan Vahi
 */
public enum WorkflowKeywords {
    PEGASUS("pegasus"),
    X_PEGASUS("x-pegasus"),
    NAME("name"),
    WORKFLOW("workflow"),
    REPLICA_CATALOG("replicaCatalog"),
    SITE_CATALOG("siteCatalog"),
    TRANSFORMATION_CATALOG("transformationCatalog"),
    HOOKS("hooks"),
    HOOKS_SHELL_TYPE("shell"),
    JOBS("jobs"),
    JOB_NAMESPACE("namespace"),
    JOB_VERSION("version"),
    JOB_ID("id"),
    JOB_ARGUMENTS("arguments"),
    JOB_STDIN("stdin"),
    JOB_STDOUT("stdout"),
    JOB_STDERR("stderr"),
    JOB_FILE("file"), // to designate dax or dag file only
    PROFILES("profiles"),
    USES("uses"),
    LFN("lfn"),
    METADATA("metadata"),
    NODE_LABEL("nodeLabel"),
    TYPE("type"),
    STAGE_OUT("stageOut"),
    REGISTER_REPLICA("registerReplica"),
    OPTIONAL("optional"),
    BYPASS("bypass"),
    FOR_PLANNING("forPlanning"),
    SIZE("size"),
    JOB_DEPENDENCIES("jobDependencies"),
    CHILDREN("children");

    private String mName;

    private static Map<String, WorkflowKeywords> mKeywordsVsType = new HashMap<>();

    static {
        for (WorkflowKeywords key : WorkflowKeywords.values()) {
            mKeywordsVsType.put(key.getReservedName(), key);
        }
    }

    WorkflowKeywords(String name) {
        this.mName = name;
    }

    public String getReservedName() {
        return mName;
    }

    public static WorkflowKeywords getReservedKey(String key) {
        return mKeywordsVsType.get(key);
    }
}
