#!/usr/bin/env python3

import os
import sys
import logging
import random
from pathlib import Path
from argparse import ArgumentParser
import pandas as pd

#logging.basicConfig(level=logging.DEBUG)

# --- Import Pegasus API -----------------------------------------------------------
from Pegasus.api import *






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
    wf_name = "federated-learning-example-tracked-dated-fl1"
    
    # --- Init ---------------------------------------------------------------------
    def __init__(self, dagfile="workflow.yml"):
        self.dagfile = dagfile
        self.wf_dir = str(Path(__file__).parent.resolve())
        self.shared_scratch_dir = os.path.join(self.wf_dir, "scratch")
        self.local_storage_dir = os.path.join(self.wf_dir, "output")
        return

    
    # --- Write files in directory -------------------------------------------------
    def write(self):
        if not self.sc is None:
            self.wf.add_site_catalog(self.sc)
            #self.sc.write()
        #self.wf.add_site_catalog(self.sc)
        self.props.write()
        #self.rc.write()
        #self.tc.write()
        self.wf.add_transformation_catalog(self.tc)
        self.wf.add_replica_catalog(self.rc)
        self.wf.write()
        return


    # --- Configuration (Pegasus Properties) ---------------------------------------
    def create_pegasus_properties(self):
        self.props = Properties()
        self.props["pegasus.transfer.bypass.input.staging"]=True
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
                        .add_directories(
                            Directory(Directory.SHARED_SCRATCH, self.shared_scratch_dir)
                            .add_file_servers(FileServer("scp://poseidon@federated-learning-submit" + self.shared_scratch_dir, Operation.ALL))
                        )
                        .add_condor_profile(universe="vanilla", request_memory="2048")
                        .add_pegasus_profile(
                            style="condor",
                            memory="2048",
                            data_configuration="nonsharedfs",
                            SSH_PRIVATE_KEY="/home/poseidon/.ssh/cluster_key",
                            auxillary_local=True
                        )
                    )

        
        self.sc.add_sites(local, exec_site)
        return


    # --- Transformation Catalog (Executables and Containers) ----------------------
    def create_transformation_catalog(self, exec_site_name="condorpool"):
        self.tc = TransformationCatalog()
        
        scp_wf_dir = f"scp://poseidon@federated-learning-submit/{self.wf_dir}"
        
        federated_learning_container = Container("federated_learning_container",
            container_type = Container.SINGULARITY,
            image=f"{scp_wf_dir}/containers/fl.sif",
            image_site="local"
        )
        
        # Add the orcasound processing
        mkdir = Transformation("mkdir", site="local", pfn="/bin/mkdir", is_stageable=False)
        
        init_model = Transformation("init_model", site=exec_site_name, pfn=f"{scp_wf_dir}/bin/init_model.py", is_stageable=True, container=federated_learning_container).add_metadata(track_Trans=True)
        local_model = Transformation("local_model", site=exec_site_name, pfn=f"{scp_wf_dir}/bin/local_model.py", is_stageable=True, container=federated_learning_container).add_metadata(track_Trans=True)
        global_model = Transformation("global_model", site=exec_site_name, pfn=f"{scp_wf_dir}/bin/global_model.py", is_stageable=True, container=federated_learning_container).add_metadata(track_Trans=True)
        evaluate_model = Transformation("evaluate_model", site=exec_site_name, pfn=f"{scp_wf_dir}/bin/evaluate_model.py", is_stageable=True, container=federated_learning_container).add_metadata(track_Trans=True)
        perf_model = Transformation("perf_model", site=exec_site_name, pfn=f"{scp_wf_dir}/bin/perf_model.py", is_stageable=True, container=federated_learning_container).add_metadata(track_Trans=True)
    
        
        self.tc.add_containers(federated_learning_container)
        self.tc.add_transformations(init_model, local_model, global_model, evaluate_model, perf_model)
        return


    # --- Replica Catalog ----------------------------------------------------------
    def create_replica_catalog(self):
        self.rc = ReplicaCatalog()
        scp_wf_dir = f"scp://poseidon@federated-learning-submit/{self.wf_dir}"
        self.rc.add_replica("local", "train-images-idx3-ubyte", f"{scp_wf_dir}/mnist/train-images-idx3-ubyte")
        self.rc.add_replica("local", "train-labels-idx1-ubyte", f"{scp_wf_dir}/mnist/train-labels-idx1-ubyte")
        self.rc.add_replica("local", "t10k-images-idx3-ubyte", f"{scp_wf_dir}/mnist/t10k-images-idx3-ubyte")
        self.rc.add_replica("local", "t10k-labels-idx1-ubyte", f"{scp_wf_dir}/mnist/t10k-labels-idx1-ubyte")
        #self.rc.add_replica("local", "global_model_round_init.h5", f"{scp_wf_dir}/models/global_model_round_0.h5")
        return


    # --- Create Workflow ----------------------------------------------------------
    def create_workflow(self, clients, selection, rounds):
        self.wf = Workflow(self.wf_name, infer_dependencies=True)
        self.wf.add_metadata(wf_track=True)
        
        mnist_train_size = 60000 #these are fixed limits based on the dataset
        mnist_test_size = 10000 #these are fixed limits based on the dataset
        
        #for now let's give each client sequential image range --> [client*images_per_client:(client+1)*images_per_client-1]
        #for the last client let's give [client*images_per_client:mnist_training_size]
        images_per_client = mnist_train_size // clients
        images_test_per_client=mnist_test_size // clients
        client_list = [i for i in range(clients)]
        eval_test=80
        train_images = File("train-images-idx3-ubyte").add_metadata(input_track=True,mlflow="auto")
        train_labels = File("train-labels-idx1-ubyte").add_metadata(input_track=True,mlflow="auto")
        test_images = File("t10k-images-idx3-ubyte").add_metadata(input_track=True,mlflow="auto")
        test_labels = File("t10k-labels-idx1-ubyte").add_metadata(input_track=True,mlflow="auto")
        global_model_name=""
        
        #step 1 : Build intial model 
        global_model = File(f"global_model_round_init.h5").add_metadata(output_track=True)
        initial_model= (Job("init_model", _id=f"init_model", node_label=f"init_model")
                .add_args(f"-n global_model_round_init.h5")
                .add_outputs(global_model, stage_out=True, register_replica=False)        
        )
               
        self.wf.add_jobs(initial_model)
         
        # step by rounds 
        for round in range(rounds):  
                  #step 1 DONE: read the client list
                  #step 2 DONE: split data into buckets
                  if round==0:
                    global_model_name="global_model_round_init.h5"
              
                  else:
                    global_model_name=f"global_model_round_{round-1}.h5"
                    
                  global_model = File(global_model_name).add_metadata(output_track=True)
      
                  #step 3 DONE: select clients for FL
                  selected_clients = random.sample(client_list, k=selection)
                  
                  #step 4 TODO: BUILD the initial global model and save in ./models
                  #step 4 TODO: ADJUST the local_model.py to accept the ranges and save in ./bin
                  #step 4: foreach client build local model(s)
                  local_model_base_name="local_model_job"
                  global_model_base_name="global_model_job"
                  #step 4a: preprocess data
                  #step 4b: build local model(s)
                  local_model_outputs = []
                  for client in selected_clients:
                      start_position = client*images_per_client
                      end_position = (client+1)*images_per_client - 1
                      local_model_output = File(f"local_model_{start_position}_{end_position}_round_{round}.h5").add_metadata(output_track=True)
                      local_model_outputs.append(local_model_output)
                      locals()[local_model_base_name+str(round)] = (Job(f"local_model", _id=f"client_{client}_round_{round}", node_label=f"local_model_client_{client}_round_{round}")
                          .add_args(f"-s {start_position} -e {end_position} -r {round} -m {global_model_name}")
                          .add_inputs(train_images, train_labels, global_model)
                          .add_outputs(local_model_output, stage_out=True, register_replica=False)
                      )
      
                      self.wf.add_jobs(locals()[local_model_base_name+str(round)] )
      
                  #step 5 DONE: build global model
                  global_model = File(f"global_model_round_{round}.h5").add_metadata(output_track=True)
                  locals()[global_model_base_name+str(round)]  = (Job("global_model", _id=f"global_model_round_{round}", node_label=f"global_model_round_{round}")
                      .add_args(f"-r {round} -f {' '.join([x.lfn for x in local_model_outputs])}")
                      .add_inputs(*local_model_outputs)
                      .add_outputs(global_model, stage_out=True, register_replica=False)
                  )
      
                  self.wf.add_jobs(locals()[global_model_base_name+str(round)] )
                  
                  last_model_name=f"global_model_round_{round}.h5"
                  
                  
              
                  #step 6 DONE: select clients for evaluation
                  selected_eval_clients = random.sample(client_list, k=selection)
          
                  #step 7 TODO: edit evalute script to accept client number and name the output based on that
                  #step 7 TODO: read the mnist test dataset from the single files
                  #step 7 TODO: add a preprocess part in the evaluation script
                  #step 7 DONE: foreach client run evaluation
          
                  evaluate_model_outputs = []
                  for client in selected_eval_clients:
                      start_position = client*images_test_per_client
                      end_position = (client+1)*images_test_per_client - 1
                      evaluate_model_output = File(f"global_model_evaluation_{client}_{round}.csv").add_metadata(output_track=True)
                      evaluate_model_outputs.append(evaluate_model_output)
                      evaluate_model_job = (Job("evaluate_model", _id=f"eval_client_{client}_{round}", node_label=f"evaluate_model_client_{client}_{round}")
                          .add_args(f"-s {start_position} -e {end_position} -c {client} -m {last_model_name} -r {round}")
                          .add_inputs(global_model, test_images, test_labels)
                          .add_outputs(evaluate_model_output, stage_out=True, register_replica=False)
                      )
                    
                      self.wf.add_jobs(evaluate_model_job)
                      
                 
              
      
                  #step 8: get global evaluation score
                  result_file_name=f"Model_performences_{round}.csv"
                  Final_model_Results = File(result_file_name).add_metadata(output_track=True)
                  model_perf_job = (Job("perf_model", _id=f"perf_model_{round}", node_label=f"perf_model_format_{round}")
                        .add_args(f"-f {' '.join([x.lfn for x in evaluate_model_outputs])}  -n {result_file_name}")
                        .add_inputs(*evaluate_model_outputs)   
                        .add_outputs(Final_model_Results, stage_out=True, register_replica=False)
                    ) 
        
                  self.wf.add_jobs(model_perf_job)
                                    
                  

        return self.wf 


if __name__ == '__main__':
    parser = ArgumentParser(description="Pegasus Federated Learning Workflow Example")

    parser.add_argument("-s", "--skip-sites-catalog", action="store_true", help="Skip site catalog creation")
    parser.add_argument("-e", "--execution-site-name", metavar="STR", type=str, default="condorpool", help="Execution site name (default: condorpool)")
    parser.add_argument("-o", "--output", metavar="STR", type=str, default="workflow.yml", help="Output file (default: workflow.yml)")
    parser.add_argument("-c", "--clients", metavar="INT", type=int, default=1, help="Number of available clients (default: 1)")
    parser.add_argument("-n", "--number-of-selected-clients", metavar="INT", type=int, default=1, help="Number of selected clients (default: 1)")
    parser.add_argument("-r", "--number-of-rounds", metavar="INT", type=int, default=1, help="Number of rounds (default: 1)")

    
    args = parser.parse_args()
    if args.clients < 1:
        print("Clients number needs to be > 0")
        exit()
    
    workflow = FederatedLearningWorkflow(dagfile=args.output)
    
    if not args.skip_sites_catalog:
        print("Creating execution sites...")
        workflow.create_sites_catalog(args.execution_site_name)

    print("Creating workflow properties...")
    workflow.create_pegasus_properties()
    
    print("Creating transformation catalog...")
    workflow.create_transformation_catalog(args.execution_site_name)

    print("Creating replica catalog...")
    workflow.create_replica_catalog()

    print("Creating the federated learning workflow dag...")
    workflow.create_workflow(args.clients, args.number_of_selected_clients, args.number_of_rounds)
   
    workflow.write()

    #workflow.wf.plan(submit=True).wait()