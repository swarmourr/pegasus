#!/usr/bin/env python3

from ast import literal_eval
from collections import OrderedDict
import json
import os
import pprint
import site
import sys
import logging
import random
from pathlib import Path
from argparse import ArgumentParser
import numpy as np
import pandas as pd

import getpass
import json
import os
import re
import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import yaml
#from jsonschema import validate


from Pegasus.api import *

from Pegasus.api import Site


logging.basicConfig(level=logging.DEBUG)


class FederatedLearningWorkflow():
    wf = None
    sc = None
    tc = None
    rc = None
    props = None

    dagfile = None
    wf_dir = None
    shared_scratch_dir = None
    local_storage_dir = None
    wf_name = "federated-learning-example-tracker"
    
    # --- Init ---------------------------------------------------------------------
    def __init__(self, dagfile="workflow.yml"):
        self.dagfile = dagfile
        self.wf_dir = "/home/poseidon/workflows/FL-workflow/federated-learning-fedstack-PM"
        self.shared_scratch_dir = "/home/poseidon/workflows/FL-workflow/federated-learning-fedstack-PM/scratch"
        self.local_storage_dir = "/home/poseidon/workflows/FL-workflow/federated-learning-fedstack-PM/output"
        self.local_data_dir = "/home/poseidon/workflows/FL-workflow/federated-learning-fedstack-PM/pegasus-data"
        self.worker=["worker-1.novalocal","worker-2.novalocal","worker-3.novalocal"]
        return

    
    # --- Write files in directory -------------------------------------------------
    def write(self):
        if not self.sc is None:
            self.wf.add_site_catalog(self.sc)
            #
            #self.sc.write()
        #self.wf.add_site_catalog(self.sc)
        self.props.write()
        #self.rc.write()
        #self.tc.write()
        self.wf.add_transformation_catalog(self.tc)
        self.wf.add_replica_catalog(self.rc)
        self.wf.write()
        print("haihiya write")
        print(self.wf.__dict__["site_catalog"].__dict__)
        return


    # --- Configuration (Pegasus Properties) ---------------------------------------
    def create_pegasus_properties(self):
        self.props = Properties()
        self.props["pegasus.integrity.checking"] = "none"
        return


    # --- Site Catalog -------------------------------------------------------------
    def create_sites_catalog(self, exec_site_name="condorpool"):
        self.sc = SiteCatalog()

        local = (Site("local")
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, self.shared_scratch_dir)
                            .add_file_servers(FileServer("file://" + self.shared_scratch_dir, Operation.ALL)),
                        Directory(Directory.LOCAL_STORAGE, self.local_storage_dir)
                            .add_file_servers(FileServer("file://" + self.local_storage_dir, Operation.ALL))     
                    )
                )

        exec_site = (Site(exec_site_name)
                        .add_condor_profile(universe="vanilla")
                        .add_pegasus_profile(
                            style="condor"
                        )
                    )

        
        self.sc.add_sites(local, exec_site)
        """if "/srv" not in str(Path.cwd()):
            local_storage_found = False
            for key, value in self.sc.__dict__["sites"].items():
                if key == "local":
                    directories = [dir.__dict__ for dir in value.__dict__["directories"]]
                    print(directories)
                    for directory in directories:
                        if directory['directory_type'] == 'localStorage':
                            print("hanana ")
                            localStorage_path = directory['path']
                            print(localStorage_path)
                            local_storage_found = True
                            break  # Exit the inner loop
                        elif directory['directory_type'] == 'sharedScratch':
                            sharedScratch_path = directory['path']
                            print(sharedScratch_path)

                    if local_storage_found:
                        break  # Exit the outer """
        return


    # --- Transformation Catalog (Executables and Containers) ----------------------
    def create_transformation_catalog(self, exec_site_name="condorpool"):
        self.tc = TransformationCatalog()
        
        federated_learning_container = Container("federated_learning_container",
            container_type = Container.SINGULARITY,
            image="/home/poseidon/workflows/FL-workflow/federated-learning-fedstack-PM/containers/fl.sif",
            image_site="local",
        )

        mkdir = Transformation("mkdir", site="local", pfn="/bin/mkdir", is_stageable=False)
        
        
        init_env = Transformation("init_env", site=exec_site_name, pfn=os.path.join(self.wf_dir, "bin/init_env.py"), is_stageable=True, container=federated_learning_container).add_metadata(track_Trans=True)
        local_clustering = Transformation("local_clustering", site=exec_site_name, pfn=os.path.join(self.wf_dir, "bin/localClustering.py"), is_stageable=True, container=federated_learning_container).add_metadata(track_Trans=True)
        global_clustering = Transformation("global_clustering", site=exec_site_name, pfn=os.path.join(self.wf_dir, "bin/globalClustering.py"), is_stageable=True, container=federated_learning_container).add_metadata(track_Trans=True)

        self.tc.add_containers(federated_learning_container)
        self.tc.add_transformations(init_env,local_clustering,global_clustering)
        return

    # --- Replica Catalog ----------------------------------------------------------
    def create_replica_catalog(self, model_path=""):
        self.rc = ReplicaCatalog()
        self.rc.add_replica("local", "source_data", os.path.join(self.wf_dir, "data", "oneyeardata.csv"))


    # --- Create Workflow ----------------------------------------------------------
    def create_workflow(self, clients, selection, rounds, model_path,initiation, main_round):
        self.wf = Workflow(self.wf_name, infer_dependencies=True)
        self.wf.add_metadata(wf_track=True)
        
        
        source_data=File("source_data").add_metadata(input_track=True,mlflow="auto")
        worker_data= [File(f"{self.worker[0]}_data.csv").add_metadata(output_track=True),File(f"{self.worker[1]}_data.csv").add_metadata(output_track=True),File(f"{self.worker[2]}_data.csv").add_metadata(output_track=True)]
        config_file= File(f"clusters.json").add_metadata(output_track=True)
        ref_dist=File("ref_dist.npy")
        init_env_job=(Job(f"init_env", _id=f"prepare_input_files", node_label=f"prepare_input_files")
                .add_args(f"-f source_data -size 10000 -n global_model_round_init.h5 -s 784 -c 10")
                .add_inputs(source_data)
                .add_outputs(worker_data[0],worker_data[1],worker_data[2],config_file,ref_dist, stage_out=True, register_replica=False)
            )
        self.wf.add_jobs(init_env_job)
        local_clustering_outputs=[]
        for worker in range(len(self.worker)):
            local_clustering_output = File(f"{self.worker[worker]}_clusters.csv").add_metadata(output_track=True,mlflow="auto")
            local_clustering_outputs.append(local_clustering_output)
            locals()[f"local_clustering_job_{self.worker[worker]}"]=(Job("local_clustering",_id=f"local_clustering_{self.worker[worker]}", node_label=f"local_clustering_{self.worker[worker]}")
                                    .add_args(f"-f {self.worker[worker]}_data.csv -dist ref_dist.npy -node {self.worker[worker]}")
                                    .add_inputs(worker_data[worker].add_metadata(output_track=True,mlflow="auto"),ref_dist.add_metadata(output_track=True,mlflow="auto"))
                                    .add_outputs(local_clustering_output,stage_out=True, register_replica=False)
                                    )
            self.wf.add_jobs(locals()[f"local_clustering_job_{self.worker[worker]}"])

        global_clustering_output=File(f"global_clustering_results.csv").add_metadata(output_track=True,mlflow="auto")
        global_clustering_job=(Job("global_clustering", _id="global_clustering", node_label="global_clustering")
                               .add_args(f"-f {' '.join([x.lfn for x in local_clustering_outputs])}")
                               .add_inputs(*local_clustering_outputs)
                               .add_outputs(global_clustering_output,stage_out=True, register_replica=False)
        )
        self.wf.add_jobs(global_clustering_job)
        return self.wf 
    
         
    # --- Run Workflow ----------------------------------------------------------
    
    def run_workflow(self,execution_site_name, skip_sites_catalog,clients, number_of_selected_clients, number_of_rounds,initiation, model_path, round):
        if clients < 1:
            print("Clients number needs to be > 0")
            exit()
        
        if not skip_sites_catalog:
            print("Creating execution sites...")
            self.create_sites_catalog(execution_site_name)

        print("Creating execution sites...")
        self.create_sites_catalog(execution_site_name)
        
        print("Creating workflow properties...")
        self.create_pegasus_properties()
        
        print("Creating transformation catalog...")
        self.create_transformation_catalog(execution_site_name)
    
        print("Creating replica catalog...")
        self.create_replica_catalog(model_path)
    
        print("Creating the federated learning workflow dag...")
        self.create_workflow(clients, number_of_selected_clients, number_of_rounds,model_path,initiation,round)
        
        print("Creating tracking informations")
        
        
        
        self.write()
        
    

if __name__ == '__main__':
    parser = ArgumentParser(description="Pegasus Federated Learning Workflow Example")

    parser.add_argument("-s", "--skip-sites-catalog", action="store_true", help="Skip site catalog creation")
    parser.add_argument("-e", "--execution-site-name", metavar="STR", type=str, default="condorpool", help="Execution site name (default: condorpool)")
    parser.add_argument("-o", "--output", metavar="STR", type=str, default="workflow.yml", help="Output file (default: workflow.yml)")
    parser.add_argument("-c", "--clients", metavar="INT", type=int, default=1, help="Number of available clients (default: 1)")
    parser.add_argument("-n", "--number-of-selected-clients", metavar="INT", type=int, default=1, help="Number of selected clients (default: 1)")
    parser.add_argument("-r", "--number-of-rounds", metavar="INT", type=int, default=1, help="Number of rounds (default: 1)")
    parser.add_argument("-score",metavar="INT", type=int, default=100, help="Score to stop the taining")
    args = parser.parse_args()
        
      
    if args.clients < 1:
          print("Clients number needs to be > 0")
          exit()
    
    

    
    workflow = FederatedLearningWorkflow(dagfile=args.output)
    workflow.run_workflow(args.execution_site_name, args.skip_sites_catalog,args.clients, args.number_of_selected_clients, args.number_of_rounds, True, "",0)
   
            


    #workflow.wf.plan(submit=True).wait()
    #workflow.wf.remove()
      
