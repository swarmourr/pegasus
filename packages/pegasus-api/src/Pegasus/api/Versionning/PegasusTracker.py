#!/usr/bin/env python3

from ast import literal_eval
from collections import OrderedDict
import json
import os
import pprint
import sys
import logging
import random
from pathlib import Path
from argparse import ArgumentParser
import numpy as np
import pandas as pd
from .MLflowIterfaces import *


#logging.basicConfig(level=logging.DEBUG)

# --- Import Pegasus API -----------------------------------------------------------
from Pegasus.api import *
import configparser

DEFAULT_CONFIG_DIR = os.path.expanduser("~/.pegasus")
DEFAULT_CONFIG_FILE=DEFAULT_CONFIG_DIR+"pegasus_data_config.conf"

class PegasusTracker():
    def __init__(self,local_storage_dir="",wf_dir="",wf=None,rc=None,tc=None,sc=None,execution_site_name="condorpool",dagfile="workflow.yml",config_file=DEFAULT_CONFIG_DIR+"pegasus_data_config.conf") -> None:
        #self.REPO_OWNER="swarmourr"
        #self.REPO_NAME="FL-WF"
        self.mlflow_jobs_run_id={}
        self.mlflow_runs=set()
        self.MLFLOW_CREDENTIALS={}
        self.config = configparser.ConfigParser()
        self.config.read(DEFAULT_CONFIG_FILE)
        
        if "pegasusData" in self.config:
            self.config.read(self.config.get("pegasusData", "path"))
        else:
            pass
        
        if self.config.get('MLflow', 'auth_type') == "token":
            self.mlflow_repos=MLflowManager(tracking_uri=self.config.get('MLflow', 'tracking_uri'),auth_type=self.config.get('MLflow', 'auth_type'),**{"token":self.config.get('MLflow', 'token')})
            self.MLFLOW_CREDENTIALS={"MLFLOW_TRACKING_TOKEN":self.config.get('MLflow', 'token')}
        elif self.config.get('MLflow', 'auth_type') == "username_password":
            self.mlflow_repos=MLflowManager(tracking_uri=self.config.get('MLflow', 'tracking_uri'),auth_type=self.config.get('MLflow', 'auth_type'),**{"username":self.config.get('MLflow', 'username'),"password":self.config.get('MLflow', 'password')})
            self.MLFLOW_CREDENTIALS={"MLFLOW_TRACKING_USERNAME":self.config.get('MLflow', 'username'),"MLFLOW_TRACKING_PASSWORD":self.config.get('MLflow', 'password')}
        elif self.config.get('MLflow', 'auth_type') == "non_auth":
            self.mlflow_repos=MLflowManager(tracking_uri=self.config.get('MLflow', 'tracking_uri'),auth_type=self.config.get('MLflow', 'auth_type'),**{})
            self.MLFLOW_CREDENTIALS={}
        else:
            raise ValueError("Invalid authentication type.")
        
        self.metadata_version_outputs=[]
        self.wf=wf
        self.dagfile=dagfile
        self.site=execution_site_name
        self.rc=rc
        self.tc=tc
        self.sc=sc
        self.wf_dir=wf_dir
        self.local_storage_dir=local_storage_dir
        try:
            #self.rc.add_replica("local", "cred", os.path.join(self.wf_dir, "Versionning", "cred3.json"))
            self.rc.add_replica("local", "cred", self.config.get('GoogleDrive', 'gdrive_credentials_file'))
            self.credentials=File("cred")
            self.remote_id= self.config.get('GoogleDrive', 'gdrive_parent_folder')
        except:
            self.credentials=None
            print("google drive api using services not configured")
        try:
            self.rc.add_replica("local", "bucket",self.config.get('Bucket', 'bucket_path'))
            self.bucket_credentials=File("bucket")
            #self.rc.add_replica("local", "bucket", "/home/poseidon/.pegasus/credentials.conf")
        except:
            self.bucket_credentials=None
            print("bucket not configured")
        try:
            self.rc.add_replica("local", "metadata", self.config.get('metadata', 'path'))
            self.metadata_file=File("metadata")
            #self.rc.add_replica("local", "metadata","/home/poseidon/workflows/FL-workflow/federated-learning-fedstack-PM/output/metadata.yaml")
        except:
            pass


        self.container =list(self.tc.containers.values())[0]
        self.container_updated=False
        

        data_tracker = Transformation("data_tracker", site=self.site, pfn=os.path.join(self.wf_dir, "Versionning/versiondata.py"), is_stageable=True, container=self.container)
        combiner = Transformation("combiner", site=self.site, pfn=os.path.join(self.wf_dir, "Versionning/pegasus_yaml_combiner.py"), is_stageable=True, container=self.container)
        self.tc.add_transformations(data_tracker,combiner)
        

    def move_element_to_index(self,od, source_index, target_index):
        if not isinstance(od, OrderedDict) or source_index < 0 or source_index >= len(od) or target_index < 0 or target_index >= len(od):
            raise ValueError("Invalid OrderedDict or source/target index")

        # Get the items as (key, value) pairs
        items = list(od.items())

        # Check if the source index is within the range of items
        if source_index >= len(items):
            raise ValueError("Source index is out of range")

        # Pop the item from the source index
        key_to_move, value_to_move = items.pop(source_index)

        # Calculate the actual target index based on the input
        target_index = len(items) if target_index == len(items) else target_index

        # Insert the item at the target index
        items.insert(target_index, (key_to_move, value_to_move))

        # Reconstruct the OrderedDict with the updated items
        od.clear()
        for key, value in items:
            od[key] = value

        return od

    def track_input(self):
        suffixe="tracker_data_job_input_"
        job_index=0
        files_to_track=[]
        wf_jobs=len(list(self.wf.jobs.items()))
        mlflow_job_file={}
        for job_name,job_object in list(self.wf.jobs.items()):
            mlflow_files_type=[]
            file_index=0
            for file in  job_object.get_inputs():
                print(file)
                if "input_track" in file.__dict__['metadata']:
                    files_to_track.append({file.lfn : {"job":job_name, "next_index":job_index+1,"current_index":wf_jobs}})
                    del file.__dict__['metadata']["input_track"] 
                if "mlflow" in  file.__dict__['metadata'] and job_name not in self.mlflow_runs:
                    if "auto" in file.__dict__['metadata']['mlflow']:
                        self.MLflowConfiguer(file.__dict__['metadata']['mlflow'],job_name)
                    else:
                        mlflow_files_type.append({file.lfn :literal_eval(file.__dict__['metadata']['mlflow'])["FILE_TYPE"]})
                        if file_index==len(list(job_object.get_inputs())):
                            self.MLflowConfiguer(file.__dict__['metadata']['mlflow'],job_name,mlflow_files_type)
                        else:
                            self.MLflowConfiguer(file.__dict__['metadata']['mlflow'],job_name)

                    #mlflow_jobs[job_name]=file.__dict__['metadata']['mlflow']
                    self.mlflow_runs.add(job_name) 
                file_index=file_index+1
            job_index=job_index+1
            wf_jobs=wf_jobs+1
            mlflow_job_file[job_name]=mlflow_files_type
        for rc_name, rc_object in  list(self.rc.entries.items()):
            lfn=rc_object.__dict__["lfn"]
            key_exists = any(lfn in d for d in files_to_track)
            if key_exists:
                # Find the dictionary with the key and update it with the new key-value pair
                for d in files_to_track:
                    if lfn in d:
                        d[lfn]["pfn"] = list(rc_object.__dict__['pfns'])[0].pfn

        grouped_data = {}
        for data in files_to_track:
            key = next(iter(data))
            job_value = data[key]['job']
            if job_value not in grouped_data:
                grouped_data[job_value] = []
            grouped_data[job_value].append(data)
        
        for job_name, dictionaries in grouped_data.items():
            current_files_tracker=set()
            current_files_pfn=[]
            for data in dictionaries:
                current_files_pfn.append(list(data.items())[0][1]["pfn"])
                current_files_tracker.update(data.keys())
            #job_name=file[list(file.keys())[0]]["job"]
            metadata_input_files=File(f"pegasus-data/metadata_{job_name}_input.yaml")
            self.metadata_version_outputs.append(metadata_input_files)
            str_gdrive=""
            str_bucket=""
            job_args=f"-files {' '.join(list(current_files_tracker))} -data_dir pegasus-data -metadata_format yaml -o pegasus-data/metadata_{job_name}_input -file_type inputs -pfn {' '.join(list(current_files_pfn))}"
            locals()[f"tracker_data_job_input_{job_name}"]=(Job("data_tracker", _id=f"tracker_data_job_{job_name}_input", node_label=f"tracker_data_job_{job_name}_input")
                    #.add_args(f"-files {' '.join(list(current_files_tracker))} -data_dir pegasus-data -bucket -bcredentials bucket -metadata_format yaml   -gdrive -remote_id {self.remote_id}  -o pegasus-data/metadata_{job_name}_input -gcredentials {self.credentials} -file_type inputs -pfn {' '.join(list(current_files_pfn))}")
                    .add_inputs(*[File(x) for x in list(current_files_tracker)])
                    .add_outputs(metadata_input_files)
                    .add_pegasus_profile(label=job_name)
                )
            if self.bucket_credentials is not None:
                str_bucket= f"-bucket -bcredentials {self.bucket_credentials}"
                locals()[f"tracker_data_job_input_{job_name}"].add_inputs(self.bucket_credentials)
            if self.credentials is not None:
                str_gdrive= f"-gdrive -remote_id {self.remote_id} -gcredentials {self.credentials}"
                locals()[f"tracker_data_job_input_{job_name}"].add_inputs(self.credentials)
            locals()[f"tracker_data_job_input_{job_name}"].add_args(f"{job_args} {str_bucket} {str_gdrive}".replace("  ", " ").strip())
            if job_name  in self.mlflow_jobs_run_id :
                if self.mlflow_jobs_run_id[job_name].get("config")=="custom":
                    locals()[f"tracker_data_job_input_{job_name}"].add_env(MLFLOW_JOB_NAME= job_name, MLFLOW_EXPERIMENT_NAME=self.mlflow_jobs_run_id[job_name]["MLFLOW_EXPERIMENT_NAME"],MLFLOW_TRACKING_URI=self.config.get('MLflow', 'tracking_uri'),**self.MLFLOW_CREDENTIALS,ENABLE_MLFLOW=True, MLFLOW_RUN=[key for key in self.mlflow_jobs_run_id[job_name] if key != "MLFLOW_EXPERIMENT_NAME"][0],MLFLOW_RUN_ID=[self.mlflow_jobs_run_id[job_name][key] for key in self.mlflow_jobs_run_id[job_name] if key != "MLFLOW_EXPERIMENT_NAME"][0],FILE_TYPE=str(mlflow_job_file[job_name]))
                else:
                     locals()[f"tracker_data_job_input_{job_name}"].add_env(MLFLOW_RUN_ID=[self.mlflow_jobs_run_id[job_name][key] for key in self.mlflow_jobs_run_id[job_name] if key != "MLFLOW_EXPERIMENT_NAME"][0])
            self.wf.get_job(job_name).add_pegasus_profile(label=job_name)
            self.wf.add_jobs(locals()[f"tracker_data_job_input_{job_name}"])
            self.wf.add_dependency(self.wf.get_job(job_name),children=[locals()[f"tracker_data_job_input_{job_name}"]])
           
        for file in files_to_track:
            self.move_element_to_index(self.wf.jobs,source_index=file[list(file.keys())[0]]["current_index"],target_index=file[list(file.keys())[0]]["next_index"])
    
    def track_output(self):
        job_index=0
        files_to_track_output=[]
        mlflow_job_file={}

        wf_jobs=len(list(self.wf.jobs.items()))
        for job_name,job_object in list(self.wf.jobs.items()):
            file_index=1
            mlflow_files_type=[]

            for file in  job_object.get_outputs():
                if "output_track" in file.__dict__['metadata']:
                    files_to_track_output.append({file.lfn : {"job":job_name, "next_index":job_index+1,"current_index":wf_jobs,"pfn":os.path.join(self.local_storage_dir,file.lfn)}})
                    del file.__dict__['metadata']["output_track"]

                if "mlflow" in  file.__dict__['metadata'] and job_name not in self.mlflow_runs:
                        if "auto" in file.__dict__['metadata']['mlflow']:
                            self.MLflowConfiguer(file.__dict__['metadata']['mlflow'],job_name)
                        else:
                            mlflow_files_type.append({file.lfn :literal_eval(file.__dict__['metadata']['mlflow'])["FILE_TYPE"]})
                            if file_index==len(list(job_object.get_inputs())):
                                self.MLflowConfiguer(file.__dict__['metadata']['mlflow'],job_name,mlflow_files_type)
                            else:
                                self.MLflowConfiguer(file.__dict__['metadata']['mlflow'],job_name)
                                locals()[f"tracker_data_job_output_{job_name}"].add_env(MLFLOW_JOB_NAME= job_name,MLFLOW_EXPERIMENT_NAME=self.mlflow_jobs_run_id[job_name]["MLFLOW_EXPERIMENT_NAME"],MLFLOW_TRACKING_URI=self.config.get('MLflow', 'tracking_uri'),**self.MLFLOW_CREDENTIALS,ENABLE_MLFLOW=True,MLFLOW_RUN=[key for key in self.mlflow_jobs_run_id[job_name] if key != "MLFLOW_EXPERIMENT_NAME"][0],MLFLOW_RUN_ID=[self.mlflow_jobs_run_id[job_name][key] for key in self.mlflow_jobs_run_id[job_name] if key != "MLFLOW_EXPERIMENT_NAME"][0],FILE_TYPE=str(mlflow_job_file[job_name]))

                        self.mlflow_runs.add(job_name) 
                file_index=file_index+1
                mlflow_job_file[job_name]=mlflow_files_type
            job_index=job_index+1

        grouped_data = {}
        for data in files_to_track_output:
            key = next(iter(data))
            job_value = data[key]['job']
            if job_value not in grouped_data:
                grouped_data[job_value] = []
            grouped_data[job_value].append(data)

        for job_name, dictionaries in grouped_data.items():
            current_files_tracker=set()
            current_files_pfn=[]
            for data in dictionaries:
                current_files_pfn.append(list(data.items())[0][1]["pfn"])
                current_files_tracker.update(data.keys())
            metadata_output_files=File(f"pegasus-data/metadata_{job_name}_output.yaml")
            self.metadata_version_outputs.append(metadata_output_files)
            str_bucket=""
            str_gdrive=""
            job_args=f"-files {' '.join(list(current_files_tracker))} -data_dir pegasus-data -metadata_format yaml -o pegasus-data/metadata_{job_name}_output -file_type outputs -pfn {' '.join(list(current_files_pfn))}"
            locals()[f"tracker_data_job_output_{job_name}"]=(Job("data_tracker", _id=f"tracker_data_job_{job_name}_output", node_label=f"tracker_data_job_{job_name}_output")
                    #.add_args(f"-files {' '.join(list(current_files_tracker))} -data_dir pegasus-data -metadata_format yaml  -bucket -bcredentials  {self.bucket_credentials} -gdrive -remote_id {self.remote_id}  -o pegasus-data/metadata_{job_name}_output -gcredentials {self.credentials} -file_type outputs -pfn {' '.join(list(current_files_pfn))}")
                    .add_inputs(*[File(x) for x in list(current_files_tracker)])
                    .add_outputs(metadata_output_files)
                    .add_pegasus_profile(label=job_name)
                )
            if self.bucket_credentials is not None:
                str_bucket= f"-bucket -bcredentials {self.bucket_credentials}"
                locals()[f"tracker_data_job_output_{job_name}"].add_inputs(self.bucket_credentials)
            if self.credentials is not None:
                str_gdrive= f"-gdrive -remote_id {self.remote_id} -gcredentials {self.credentials}"
                locals()[f"tracker_data_job_output_{job_name}"].add_inputs(self.credentials)
            locals()[f"tracker_data_job_output_{job_name}"].add_args(f"{job_args} {str_bucket} {str_gdrive}".replace("  ", " ").strip())
            if job_name  in self.mlflow_jobs_run_id :
                if self.mlflow_jobs_run_id[job_name].get("config")=="custom":
                    locals()[f"tracker_data_job_output_{job_name}"].add_env(MLFLOW_JOB_NAME= job_name,MLFLOW_EXPERIMENT_NAME=self.mlflow_jobs_run_id[job_name]["MLFLOW_EXPERIMENT_NAME"],MLFLOW_TRACKING_URI=self.config.get('MLflow', 'tracking_uri'),**self.MLFLOW_CREDENTIALS,ENABLE_MLFLOW=True,MLFLOW_RUN=[key for key in self.mlflow_jobs_run_id[job_name] if key != "MLFLOW_EXPERIMENT_NAME"][0],MLFLOW_RUN_ID=[self.mlflow_jobs_run_id[job_name][key] for key in self.mlflow_jobs_run_id[job_name] if key != "MLFLOW_EXPERIMENT_NAME"][0],FILE_TYPE=str(mlflow_job_file[job_name]))
                else:
                     locals()[f"tracker_data_job_output_{job_name}"].add_env(MLFLOW_RUN_ID=[self.mlflow_jobs_run_id[job_name][key] for key in self.mlflow_jobs_run_id[job_name] if key != "MLFLOW_EXPERIMENT_NAME"][0])
            self.wf.get_job(job_name).add_pegasus_profile(label=job_name)
            self.wf.add_jobs(locals()[f"tracker_data_job_output_{job_name}"])
            self.wf.add_dependency(self.wf.get_job(job_name),children=[locals()[f"tracker_data_job_output_{job_name}"]])
    
        for file in files_to_track_output:
            self.move_element_to_index(self.wf.jobs,source_index=file[list(file.keys())[0]]["current_index"],target_index=file[list(file.keys())[0]]["next_index"])

    def track_wf(self):
        
        if self.wf.__dict__['metadata']["wf_track"]:
            self.wf.add_site_catalog(self.sc)
            self.wf.add_replica_catalog(self.rc)
            self.wf.add_transformation_catalog(self.tc)
            self.rc.add_replica("local", "wf", os.path.join(self.wf_dir, self.dagfile))
            wf_name_track=File("wf")
            output_wf=File(f"pegasus-data/metadata_{wf_name_track}.yaml")
            self.metadata_version_outputs.append(output_wf)
            str_bucket=""
            str_gdrive=""
            job_args=f"-files {wf_name_track} -data_dir pegasus-data -metadata_format yaml -o pegasus-data/metadata_{wf_name_track} -file_type workflow -pfn {os.path.join(self.wf_dir, self.dagfile)}"
            locals()[f"tracker_wf_{self.wf.__dict__['name']}"]=(Job("data_tracker", _id=f"tracker_wf_{self.wf.__dict__['name']}", node_label=f"tracker_wf_{self.wf.__dict__['name']}")
                    #.add_args(f"-files {wf_name_track} -data_dir pegasus-data -metadata_format yaml -bucket -bcredentials  bucket -gdrive -remote_id {self.remote_id}  -o pegasus-data/metadata_{wf_name_track} -gcredentials {self.credentials} -file_type workflow -pfn {os.path.join(self.wf_dir, self.dagfile)}")
                    .add_inputs(wf_name_track)
                    .add_outputs(output_wf)
                    .add_env(ENABLE_MLFLOW=False)    
                )
            if self.bucket_credentials is not None:
               str_bucket= f"-bucket -bcredentials {self.bucket_credentials}"
               locals()[f"tracker_wf_{self.wf.__dict__['name']}"].add_inputs(self.bucket_credentials)
            if self.credentials is not None:
                str_gdrive= f"-gdrive -remote_id {self.remote_id} -gcredentials {self.credentials}"
                locals()[f"tracker_wf_{self.wf.__dict__['name']}"].add_inputs(self.credentials)
                
            locals()[f"tracker_wf_{self.wf.__dict__['name']}"].add_args(f"{job_args} {str_bucket} {str_gdrive}".replace("  ", " ").strip())
            self.wf.add_jobs(locals()[f"tracker_wf_{self.wf.__dict__['name']}"])
    
    def build_metadata(self):
        metadata_output_combiner=File(f"metadata.yaml")
        metadata_output_combiner_job=(Job("combiner", _id="metadata_combiner", node_label="metadata_combiner")
                               .add_args(f"-files {' '.join([x.lfn for x in self.metadata_version_outputs])} -output_file_name {self.metadata_file} -file_type yaml")
                               .add_inputs(*self.metadata_version_outputs,self.metadata_file)
                               .add_outputs(metadata_output_combiner,stage_out=True, register_replica=False)
        
        )
        self.wf.add_jobs(metadata_output_combiner_job)  
    
    def full_tracker(self):
            self.track_input()
            self.track_output()
            self.track_wf()
            self.build_metadata()


    def auto_logger(self):
        job_index=0
        files_to_track=[]

        wf_jobs=len(list(self.wf.jobs.items()))

        for job_name,job_object in list(self.wf.jobs.items()):
            for file in  job_object.get_inputs():
                files_to_track.append({file.lfn : {"job":job_name, "next_index":job_index+1,"current_index":wf_jobs}})
            job_index=job_index+1
            wf_jobs=wf_jobs+1

        for rc_name, rc_object in  list(self.rc.entries.items()):
            lfn=rc_object.__dict__["lfn"]
            key_exists = any(lfn in d for d in files_to_track)
            if key_exists:
                # Find the dictionary with the key and update it with the new key-value pair
                for d in files_to_track:
                    if lfn in d:
                        d[lfn]["pfn"] = list(rc_object.__dict__['pfns'])[0].pfn

        grouped_data = {}
        for data in files_to_track:
            key = next(iter(data))
            job_value = data[key]['job']
            if job_value not in grouped_data:
                grouped_data[job_value] = []
            grouped_data[job_value].append(data)

        for job_name, dictionaries in grouped_data.items():
            current_files_tracker=set()
            current_files_pfn=[]
            for data in dictionaries:
                try:
                    current_files_pfn.append(list(data.items())[0][1]["pfn"])
                    current_files_tracker.update(data.keys())
                except:
                    print(f"{list(data.keys())[0]} this file is considred as output")
            #job_name=file[list(file.keys())[0]]["job"]
            metadata_input_files=File(f"pegasus-data/metadata_{job_name}_input.yaml")
            self.metadata_version_outputs.append(metadata_input_files)
            locals()[f"tracker_data_job_input_{job_name}"]=(Job("data_tracker", _id=f"tracker_data_job_{job_name}_input", node_label=f"tracker_data_job_{job_name}_input")
                    .add_args(f"-files {' '.join(list(current_files_tracker))} -data_dir pegasus-data -metadata_format yaml  -bucket -bucket_credentials {self.bucket_credentials}-gdrive -remote_id {self.remote_id}  -o pegasus-data/metadata_{job_name}_input -gcredentials {self.credentials} -file_type inputs -pfn {' '.join(list(current_files_pfn))}")
                    .add_inputs(*[File(x) for x in list(current_files_tracker)],self.credentials , self.bucket_credentials)
                    .add_outputs(metadata_input_files)
                )
            self.wf.add_jobs(locals()[f"tracker_data_job_input_{job_name}"])
        for file in files_to_track:
            self.move_element_to_index(self.wf.jobs,source_index=file[list(file.keys())[0]]["current_index"],target_index=file[list(file.keys())[0]]["next_index"])


        job_index=0
        files_to_track_output=[]

        wf_jobs=len(list(self.wf.jobs.items()))
        for job_name,job_object in list(self.wf.jobs.items()):
            for file in  job_object.get_outputs():
                files_to_track_output.append({file.lfn : {"job":job_name, "next_index":job_index+1,"current_index":wf_jobs,"pfn":os.path.join(self.local_storage_dir,file.lfn)}})

        grouped_data = {}
        for data in files_to_track_output:
            key = next(iter(data))
            job_value = data[key]['job']
            if job_value not in grouped_data:
                grouped_data[job_value] = []
            grouped_data[job_value].append(data)

        
        for job_name, dictionaries in grouped_data.items():
            current_files_tracker=set()
            current_files_pfn=[]
            for data in dictionaries:
                current_files_pfn.append(list(data.items())[0][1]["pfn"])
                current_files_tracker.update(data.keys())
            #job_name=file[list(file.keys())[0]]["job"]
            metadata_output_files=File(f"pegasus-data/metadata_{job_name}_output.yaml")
            self.metadata_version_outputs.append(metadata_output_files)
            locals()[f"tracker_data_job_output_{job_name}"]=(Job("data_tracker", _id=f"tracker_data_job_{job_name}_output", node_label=f"tracker_data_job_{job_name}_output")
                    .add_args(f"-files {' '.join(list(current_files_tracker))} -data_dir pegasus-data -metadata_format yaml  -bucket -bcredentials  bucket -gdrive -remote_id {self.remote_id}  -o pegasus-data/metadata_{job_name}_output -gcredentials {self.credentials} -file_type outputs -pfn {' '.join(list(current_files_pfn))}")
                    .add_inputs(*[File(x) for x in list(current_files_tracker)],self.credentials,self.bucket_credentials)
                    .add_outputs(metadata_output_files)
                )
            
            self.wf.add_jobs(locals()[f"tracker_data_job_output_{job_name}"])

    
        for file in files_to_track_output:
            self.move_element_to_index(self.wf.jobs,source_index=file[list(file.keys())[0]]["current_index"],target_index=file[list(file.keys())[0]]["next_index"])


        #-------track wf------------
        self.rc.add_replica("local", "wf", os.path.join(self.wf_dir, self.dagfile))
        wf_name_track=File("wf")
        output_wf=File(f"pegasus-data/metadata_{wf_name_track}.yaml")
        self.metadata_version_outputs.append(output_wf)
        locals()[f"tracker_wf_{self.wf.__dict__['name']}"]=(Job("data_tracker", _id=f"tracker_wf_{self.wf.__dict__['name']}", node_label=f"tracker_wf_{self.wf.__dict__['name']}")
                    .add_args(f"-files {wf_name_track} -data_dir pegasus-data -metadata_format yaml  -bucket -bcredentials  {self.bucket_credentials} -gdrive -remote_id {self.remote_id} -o pegasus-data/metadata_{wf_name_track} -gcredentials {self.credentials} -file_type workflow -pfn {os.path.join(self.wf_dir, self.dagfile)}")
                    .add_inputs(wf_name_track,self.credentials,self.bucket_credentials)
                    .add_outputs(output_wf)
                )
        self.wf.add_jobs(locals()[f"tracker_wf_{self.wf.__dict__['name']}"])

    def MLflowConfiguer(self,config,job_name,file_type=None):
        #REPO_OWNER="swarmourr"
        #REPO_NAME="FL-WF"
        #mlflow.set_tracking_uri(f'https://dagshub.com/{REPO_OWNER}/{REPO_NAME}.mlflow')
        required_keys = ["name", "container_type", "image", "arguments", "mounts", "image_site", "checksum", "metadata", "bypass_staging"]
        
        try:
            if isinstance(literal_eval(config), dict):
                mlflow_config=dict(literal_eval(config))
                self.mlflow_repos.create_experiment(mlflow_config['MLFLOW_EXPERIMENT'])
                if job_name not in self.mlflow_jobs_run_id:
                    print(f"Creating run for the job {job_name}")
                    print(f"The run name {mlflow_config['MLFLOW_RUN']}")
                    run_id=self.mlflow_repos.create_run(experiment_name=mlflow_config['MLFLOW_EXPERIMENT'],run_name=mlflow_config['MLFLOW_RUN'])
                    self.mlflow_jobs_run_id[job_name]={mlflow_config['MLFLOW_RUN']:run_id ,"MLFLOW_EXPERIMENT_NAME":mlflow_config['MLFLOW_EXPERIMENT'],"config":"custom"}
                else:
                    print(f"The run for the job {job_name} already exist")
                    print(f"The run name {mlflow_config['MLFLOW_RUN']} already exist")
                    run_id= self.mlflow_jobs_run_id[job_name][mlflow_config['MLFLOW_RUN']]
                if file_type is None:
                    self.wf.get_job(job_name).add_env(MLFLOW_JOB_NAME= job_name,ENABLE_MLFLOW=True, MLFLOW_EXPERIMENT_NAME=mlflow_config['MLFLOW_EXPERIMENT'],MLFLOW_RUN=mlflow_config['MLFLOW_RUN'],MLFLOW_TRACKING_URI=self.config.get('MLflow', 'tracking_uri'),MLFLOW_RUN_ID=run_id,**self.MLFLOW_CREDENTIALS)
                else:
                    self.wf.get_job(job_name).add_env(MLFLOW_JOB_NAME= job_name, ENABLE_MLFLOW=True, MLFLOW_EXPERIMENT_NAME=mlflow_config['MLFLOW_EXPERIMENT'],MLFLOW_RUN=mlflow_config['MLFLOW_RUN'],MLFLOW_TRACKING_URI=self.config.get('MLflow', 'tracking_uri'),MLFLOW_RUN_ID=run_id,**self.MLFLOW_CREDENTIALS,FILE_TYPE=str(file_type))
        except:
            if isinstance(config, str) and config=="auto":
                if not self.container_updated:
                    arguments_container=self.container.__dict__
                    arguments_container={key: value for key, value in arguments_container.items() if value is not None}
                    arguments_container = {key: arguments_container[key] for key in required_keys if key in arguments_container} 
                    arguments_container["container_type"]=eval("Container."+str(arguments_container["container_type"].upper()))               
                    self.mlflow_repos.create_experiment(self.wf.__dict__["name"])
                    if job_name not in self.mlflow_jobs_run_id:
                        run_id=self.mlflow_repos.create_run(experiment_name=self.wf.__dict__["name"],run_name=f'{job_name}')
                        self.mlflow_jobs_run_id[job_name]={job_name:run_id,"config":"auto"}
                    else:
                        run_id= self.mlflow_jobs_run_id[job_name][job_name]
                    formatted_pairs = " --env ".join([f"{key.upper()}={value}" for key, value in self.MLFLOW_CREDENTIALS.items()])
                    arguments_container["arguments"]=f"--env ENABLE_MLFLOW=True --env MLFLOW_EXPERIMENT_NAME={self.wf.__dict__['name']} --env MLFLOW_RUN=$PEGASUS_DAG_JOB_ID --env MLFLOW_TRACKING_URI={self.config.get('MLflow', 'tracking_uri')} --env {formatted_pairs} --env MLFLOW_CONFIG={config} --env FILE_TYPE=None "
                    self.tc.__dict__["containers"][self.container.name]=Container(**arguments_container)
                    self.container_updated=True
                    self.wf.get_job(job_name).add_env(MLFLOW_RUN_ID=run_id)
                    #self.mlflow_jobs_run_id[job_name]=run_id
                else:
                    if job_name not in self.mlflow_jobs_run_id:
                        run_id=self.mlflow_repos.create_run(experiment_name=self.wf.__dict__["name"],run_name=f'{job_name}')
                        self.mlflow_jobs_run_id[job_name]={job_name:run_id,"config":"auto"}
                    else:
                        run_id= self.mlflow_jobs_run_id[job_name][job_name]
                    self.wf.get_job(job_name).add_env(MLFLOW_RUN_ID=run_id)
                    print("container already updated")
                print(self.mlflow_jobs_run_id)
            else:
                print(job_name)
                print(config)
                raise ValueError("Invalid configuration type.")
            