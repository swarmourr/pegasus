#!/usr/bin/env python3
import os

import logging
from pathlib import Path
from argparse import ArgumentParser

logging.basicConfig(level=logging.DEBUG)

# --- Import Pegasus API ------------------------------------------------------
from Pegasus.api import *


class MergeWorkflow:
    wf = None
    sc = None
    tc = None
    rc=None
    props = None

    dagfile = None
    wf_name = None
    wf_dir = None

    # --- Init ----------------------------------------------------------------
    def __init__(self, dagfile="workflow.yml"):
        self.dagfile = dagfile
        self.wf_name = "merge"
        self.wf_dir = str(Path(__file__).parent.resolve())

    # --- Write files in directory --------------------------------------------
    def write(self):
        if not self.sc is None:
            self.wf.add_site_catalog(self.sc)
            #self.sc.write()
        #self.wf.add_site_catalog(self.sc)
        self.props.write()
        #self.rc.write()
        #self.tc.write()
        self.wf.add_transformation_catalog(self.tc)
        self.rc=ReplicaCatalog()
        self.wf.add_replica_catalog(self.rc)
        self.wf.write()
        return
        
    # --- Configuration (Pegasus Properties) ----------------------------------
    def create_pegasus_properties(self):
        self.props = Properties()

        # props["pegasus.monitord.encoding"] = "json"
        # self.properties["pegasus.integrity.checking"] = "none"
        return

    # --- Site Catalog --------------------------------------------------------
    def create_sites_catalog(self, exec_site_name="condorpool"):
        self.sc = SiteCatalog()

        shared_scratch_dir = os.path.join(self.wf_dir, "scratch")
        local_storage_dir = os.path.join(self.wf_dir, "output")

        local = Site("local").add_directories(
            Directory(Directory.SHARED_SCRATCH, shared_scratch_dir).add_file_servers(
                FileServer("file://" + shared_scratch_dir, Operation.ALL)
            ),
            Directory(Directory.LOCAL_STORAGE, local_storage_dir).add_file_servers(
                FileServer("file://" + local_storage_dir, Operation.ALL)
            ),
        )

        exec_site = (
            Site(exec_site_name)
            .add_pegasus_profile(style="condor")
            .add_condor_profile(universe="vanilla")
            .add_profiles(Namespace.PEGASUS, key="data.configuration", value="condorio")
        )

        self.sc.add_sites(local, exec_site)

    # --- Transformation Catalog (Executables and Containers) -----------------
    def create_transformation_catalog(self, exec_site_name="condorpool"):
        self.tc = TransformationCatalog()

        ls = Transformation(
            "ls", site=exec_site_name, pfn="/bin/ls", is_stageable=False,
        )
        cat = Transformation(
            "cat", site=exec_site_name, pfn="/bin/cat", is_stageable=False,
        )

        self.tc.add_transformations(ls, cat)

    # --- Create Workflow -----------------------------------------------------
    def create_workflow(self):
        self.wf = Workflow(self.wf_name, infer_dependencies=True,tracker_type="auto")
       

        dirs = ("/bin", "/usr/bin", "/usr/local/bin")
        cat = Job("cat")

        for i, d in enumerate(dirs):
            f = File("bin_%d.txt" % i)

            cat.add_args(f).add_inputs(f)
            ls = Job("ls").add_args("-l", d).set_stdout(f, stage_out=True, register_replica=False)

            self.wf.add_jobs(ls)

        output = File("binaries.txt")
        cat.set_stdout(output, stage_out=True, register_replica=True)

        self.wf.add_jobs(cat)


if __name__ == "__main__":
    parser = ArgumentParser(description="Pegasus Merge Workflow")

    parser.add_argument(
        "-s",
        "--skip_sites_catalog",
        action="store_true",
        help="Skip site catalog creation",
    )
    parser.add_argument(
        "-e",
        "--execution_site_name",
        metavar="STR",
        type=str,
        default="condorpool",
        help="Execution site name (default: condorpool)",
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="STR",
        type=str,
        default="workflow.yml",
        help="Output file (default: workflow.yml)",
    )

    args = parser.parse_args()

    workflow = MergeWorkflow(args.output)

    if not args.skip_sites_catalog:
        print("Creating execution sites...")
        workflow.create_sites_catalog(args.execution_site_name)

    print("Creating workflow properties...")
    workflow.create_pegasus_properties()
   
    print("Creating transformation catalog...")
    workflow.create_transformation_catalog(args.execution_site_name)

    print("Creating merge workflow dag...")
    workflow.create_workflow()

    workflow.write()
