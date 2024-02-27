import json
import logging
from collections import OrderedDict, defaultdict
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Dict, List, Optional, TextIO, Union

from ._utils import _chained, _get_enum_str
from .errors import DuplicateError, NotFoundError, PegasusError
from .mixins import HookMixin, MetadataMixin, ProfileMixin , Namespace
from .replica_catalog import File, ReplicaCatalog
from .site_catalog import SiteCatalog
from .transformation_catalog import Transformation, TransformationCatalog ,Container
from .writable import Writable, _CustomEncoder, _filter_out_nones

from ast import literal_eval
from collections import OrderedDict
import os

from .MLflowIterfaces import MLflowManager
from mlflow.utils.mlflow_tags import MLFLOW_PARENT_RUN_ID
from datetime import datetime
import configparser

from Pegasus.client._client import from_env



PEGASUS_VERSION = "5.0.4"
DEFAULT_CONFIG_DIR = os.path.expanduser("~/.pegasus/")
DEFAULT_CONFIG_FILE=DEFAULT_CONFIG_DIR+"pegasus_data_config.conf"
METADATA_FILE=  os.path.join(DEFAULT_CONFIG_DIR, "Metadata.yaml")

print(DEFAULT_CONFIG_FILE)
__all__ = ["AbstractJob", "Job", "SubWorkflow", "Workflow"]

log = logging.getLogger(__name__)




class PegasusTracker():
    def __init__(self,local_storage_dir="",wf_dir="",wf=None,rc=None,tc=None,sc=None,execution_site_name="condorpool",dagfile="workflow.yml",config_file=DEFAULT_CONFIG_FILE) -> None:
        #self.REPO_OWNER="swarmourr"
        #self.REPO_NAME="FL-WF"
        self.mlflow_jobs_run_id={}
        self.mlflow_runs=set()
        self.MLFLOW_CREDENTIALS={}
        self.token=None
        self.branch=None
        self.owner=None
        self.repo=None
        self.use_git=False
        self.pull=False
        self.config = configparser.ConfigParser()
        self.PARENT_MLFLOW_ID=None

        try:
            self.config.read(DEFAULT_CONFIG_FILE)
        except:
            print("No configuration file found please use pegasus-data to add necessairy configuration if you want data tracking")
        
        self.mlflow=True
        
        if "pegasusData" in self.config:
            self.config.read(self.config.get("pegasusData", "path"))

        if self.config.has_section("MLflow"):
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
                self.mlflow=False
                print("Mlflow authentication not correctly configured --> Skipping the mlflow tracking ")
                
        else:
            self.mlflow = False
            print("Mlflow not configured --> Skipping the mlflow tracking")
        
        if self.config.has_section('git'):
            self.use_git=True
            self.token=self.config.get("git","token")
            self.owner=self.config.get("git","owner")
            self.repo=self.config.get("git","repo")
            self.branch=self.config.get("git","branch")
            self.pull=self.config.getboolean("git","pull")


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
            self.metadata_combiner=True
            #self.rc.add_replica("local", "metadata","/home/poseidon/workflows/FL-workflow/federated-learning-fedstack-PM/output/metadata.yaml")
        except:
            self.metadata_combiner=False
    
        self.metadata_output_combiner=None
        try:
            self.container =list(self.tc.containers.values())[0]
        except:
            self.container=None
        self.container_updated=False
        

        data_tracker = Transformation("data_tracker", site=self.site, pfn=os.path.join(os.path.dirname(os.path.abspath(__file__)), "Versionning/versiondata.py"), is_stageable=True, container=self.container)
        combiner = Transformation("combiner", site=self.site, pfn=os.path.join(os.path.dirname(os.path.abspath(__file__)), "Versionning/pegasus_yaml_combiner.py"), is_stageable=True, container=self.container)
        versioning_transformations=Transformation("version_transformations", site=self.site, pfn=os.path.join(os.path.dirname(os.path.abspath(__file__)), "Versionning/transformationVersioning.py"), is_stageable=True, container=self.container)
        cp_transformation = Transformation("cp", site='local', pfn="/bin/cp", is_stageable=False)
        self.tc.add_transformations(data_tracker,combiner,versioning_transformations,cp_transformation)
        

    def move_element_to_index(self,od, source_index, target_index):
        if not isinstance(od, OrderedDict) or source_index < 0 or source_index >= len(od)+1 or target_index < 0 or target_index >= len(od)+1:
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
        if self.rc==None:
            print("nothing")
        else:
            suffixe="tracker_data_job_input_"
            job_index=0
            files_to_track=[]
            wf_jobs=len(list(self.wf.jobs.items()))
            mlflow_job_file={}
            for job_name,job_object in list(self.wf.jobs.items()):
                mlflow_files_type=[]
                file_index=0
                for file in  job_object.get_inputs():
                    if "input_track" in file.__dict__['metadata']:
                        files_to_track.append({file.lfn : {"job":job_name, "next_index":job_index+1,"current_index":job_index}})
                        del file.__dict__['metadata']["input_track"] 
                    if "mlflow" in  file.__dict__['metadata'] and job_name not in self.mlflow_runs and self.mlflow==True:
                        if "auto" in file.__dict__['metadata']['mlflow']:
                            if self.PARENT_MLFLOW_ID==None:
                                self.PARENT_MLFLOW_ID=self.MLflowConfiguerParent()
                            print("-------------Hada rah Parent ID-----------------")
                            print(f"---------------{self.PARENT_MLFLOW_ID}-----------")
                            self.MLflowConfiguer(file.__dict__['metadata']['mlflow'],job_name,run_parent_id=self.PARENT_MLFLOW_ID)
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
                print("creating jobs")
                locals()[f"tracker_data_job_input_{job_name}"]=(Job(transformation="data_tracker", _id=f"tracker_data_job_{job_name}_input", node_label=f"tracker_data_job_{job_name}_input")
                        .add_inputs(*[File(x) for x in list(current_files_tracker)])
                        .add_outputs(metadata_input_files)
                        .add_pegasus_profile(label=job_name)
                    )
                #.add_args(f"-files {' '.join(list(current_files_tracker))} -data_dir pegasus-data -bucket -bcredentials bucket -metadata_format yaml   -gdrive -remote_id {self.remote_id}  -o pegasus-data/metadata_{job_name}_input -gcredentials {self.credentials} -file_type inputs -pfn {' '.join(list(current_files_pfn))}")
                print(f"tracker_data_job_input_{job_name} created ")
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

        return self.wf
    
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
                    files_to_track_output.append({file.lfn : {"job":job_name, "next_index":job_index+1,"current_index":job_index,"pfn":os.path.join(self.local_storage_dir,file.lfn)}})
                    del file.__dict__['metadata']["output_track"]

                if "mlflow" in  file.__dict__['metadata'] and job_name not in self.mlflow_runs and self.mlflow==True:
                        if "auto" in file.__dict__['metadata']['mlflow']:
                            if self.PARENT_MLFLOW_ID==None:
                                self.PARENT_MLFLOW_ID=self.MLflowConfiguerParent()
                            self.MLflowConfiguer(file.__dict__['metadata']['mlflow'],job_name,run_parent_id=self.PARENT_MLFLOW_ID)
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
        return self.wf
    
    def track_transformations(self):
        if self.rc==None:
            self.rc=ReplicaCatalog()
        self.wf_transformations_name = []
        self.wf_transformations_path = []
        self.transfomations_replicas_file=[]
        wf_trans = list(self.wf.transformation_catalog.__dict__.items())
        metadata_output_files=File(f"pegasus-data/metadata_transformations.yaml")
        self.metadata_version_outputs.append(metadata_output_files)
        for trans in wf_trans:
            if trans[0] == "transformations":
                for trans_key, trans_value in trans[1].items():
                    if ("track_Trans" in list(trans_value.__dict__["metadata"].keys())) and bool(trans_value.__dict__["metadata"]["track_Trans"]) == True:
                        trans_dict = trans_value.__dict__
                        trans_name = trans_dict["name"]
                        for site in trans_dict["sites"].items():
                            trans_description = site[1].__dict__
                            trans_pfn = site[1].__dict__["pfn"]
                            trans_type = site[1].__dict__[
                                "transformation_type"]
                        self.wf_transformations_name.append(trans_name)
                        self.wf_transformations_path.append(trans_pfn)
                    
        for id,used_transformation_path in enumerate(self.wf_transformations_path):
            self.rc.add_replica("local",self.wf_transformations_name[id],used_transformation_path)
            self.transfomations_replicas_file.append(File(self.wf_transformations_name[id]))
        job_args=f"-files {' '.join([x.lfn for x in self.transfomations_replicas_file])} -names {' '.join([x for x in self.wf_transformations_name])} -path {' '.join([x for x in self.wf_transformations_path])} --use-git -token {self.token} -owner {self.owner} -repo {self.repo} -branch {self.branch}"
        transformation_versioning_job=(Job("version_transformations",_id="transformation_versioning_job", node_label="transformation_versioning_job")
                                                .add_inputs(*self.transfomations_replicas_file)
                                                .add_outputs(metadata_output_files)
                                                .add_args(job_args)) 
        self.wf.add_jobs(transformation_versioning_job)

        return self.wf


    def track_wf(self):
        if "wf_track" in self.wf.__dict__['metadata'].keys():
            if self.sc is None:
                pass
            else:
                self.wf.add_site_catalog(self.sc)
            #print(self.rc.__dict__)
            #self.wf.add_replica_catalog(self.rc)
            #self.wf.add_transformation_catalog(self.tc)
            if self.rc==None:
                self.rc = ReplicaCatalog()
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
        return self.wf
    
    def build_metadata(self):
        if self.metadata_combiner :
            self.metadata_output_combiner=File(f"metadata.yaml")
            metadata_output_combiner_job=(Job("combiner", _id="metadata_combiner", node_label="metadata_combiner")
                                .add_inputs(*self.metadata_version_outputs,self.metadata_file)
                                .add_outputs(self.metadata_output_combiner,stage_out=True, register_replica=False)
            
            )
            if self.use_git:
                if self.pull:
                    metadata_output_combiner_job.add_args(f"-files {' '.join([x.lfn for x in self.metadata_version_outputs])} -output_file_name {self.metadata_file} -file_type yaml --use-git -token {self.token} -owner {self.owner} -repo {self.repo} -branch {self.branch} -pull")
                else:
                    metadata_output_combiner_job.add_args(f"-files {' '.join([x.lfn for x in self.metadata_version_outputs])} -output_file_name {self.metadata_file} -file_type yaml --use-git -token {self.token} -owner {self.owner} -repo {self.repo} -branch {self.branch}")

            else: 
                metadata_output_combiner_job.add_args(f"-files {' '.join([x.lfn for x in self.metadata_version_outputs])} -output_file_name {self.metadata_file} -file_type yaml")

            self.wf.add_jobs(metadata_output_combiner_job)  
        

            cp_job=Job("cp",_id="copy_file", node_label="copy_file")\
                    .add_args(self.metadata_output_combiner, DEFAULT_CONFIG_DIR)\
                    .add_inputs(self.metadata_output_combiner)\
                    .add_profiles(Namespace.SELECTOR, key="execution.site", value="local")\
                    
            self.wf.add_jobs(cp_job)

        return self.wf
    
    def full_tracker(self):
            self.wf=self.track_input()
            self.wf=self.track_output()
            self.wf=self.track_wf()
            self.wf=self.track_transformations()
            self.wf=self.build_metadata()
            
            return self.wf


    def auto_logger(self):
        job_index=0
        files_to_track=[]

        wf_jobs=len(list(self.wf.jobs.items()))

        for job_name,job_object in list(self.wf.jobs.items()):
            for file in  job_object.get_inputs():
                files_to_track.append({file.lfn : {"job":job_name, "next_index":job_index+1,"current_index":job_index}})
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
                files_to_track_output.append({file.lfn : {"job":job_name, "next_index":job_index+1,"current_index":job_index,"pfn":os.path.join(self.local_storage_dir,file.lfn)}})

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

    def MLflowConfiguer(self,config,job_name,file_type=None,run_parent_id=None):
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
                        if run_parent_id==None:
                            run_id=self.mlflow_repos.create_run(experiment_name=self.wf.__dict__["name"],run_name=f'{job_name}')
                        else:
                            print(f"----- inisde  :{run_parent_id}")
                            run_id=self.mlflow_repos.create_run(experiment_name=self.wf.__dict__["name"],run_name=f'{job_name}',tags=run_parent_id)
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
                        if run_parent_id==None:
                            print("je suis nOne")
                            run_id=self.mlflow_repos.create_run(experiment_name=self.wf.__dict__["name"],run_name=f'{job_name}')
                        else:
                            print(f"----- inisde  :{run_parent_id}")
                            run_id=self.mlflow_repos.create_run(experiment_name=self.wf.__dict__["name"],run_name=f'{job_name}',tags=run_parent_id)
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
            
    def MLflowConfiguerParent(self):
        self.mlflow_repos.create_experiment(self.wf.__dict__["name"])
        return self.mlflow_repos.create_run(experiment_name=self.wf.__dict__["name"],run_name=f'{datetime.now().strftime("%Y%m%d%H%M%S")}_{self.wf.__dict__["name"]}')
            

class AbstractJob(HookMixin, ProfileMixin, MetadataMixin):
    """An abstract representation of a workflow job"""

    def __init__(self, _id: Optional[str] = None, node_label: Optional[str] = None):
        """
        :param _id: a unique id, if None is given then one will be assigned when this job is added to a :py:class:`~Pegasus.api.workflow.Workflow`, defaults to None
        :type _id: Optional[str]
        :param node_label: a short descriptive label that can be assigned to this job, defaults to None
        :type node_label: Optional[str]

        **Note**: avoid using IDs such as :code:`'0000008'` or :code:`'00000009'` as these may end up being
        unquoted by PyYaml, and consequently misinterpreted as integer values when
        read in by other tools.
        """
        self._id = _id
        self.node_label = node_label
        self.args = list()
        self.uses = set()

        self.stdout = None
        self.stderr = None
        self.stdin = None

        self.hooks = defaultdict(list)
        self.profiles = defaultdict(OrderedDict)
        self.metadata = OrderedDict()

    @_chained
    def add_inputs(self, *input_files: Union[File, str], bypass_staging: bool = False):
        """
        add_inputs(self, *input_files: Union[File, str], bypass: bool = False)
        Add one or more :py:class:`~Pegasus.api.replica_catalog.File` objects as input to this job.
        If :code:`input_file` is given as a str, a :py:class:`~Pegasus.api.replica_catalog.File` object is created for
        you internally with the given value as its lfn.

        :param input_files: the :py:class:`~Pegasus.api.replica_catalog.File` objects to be added as inputs to this job
        :type input_files: Union[File, str]
        :param bypass_staging: whether or not to bypass the staging site when this file is fetched by the job, defaults to False
        :type bypass_staging: bool, optional
        :raises DuplicateError: all input files must be unique
        :raises TypeError: job inputs must be of type :py:class:`~Pegasus.api.replica_catalog.File` or str
        :return: self
        """
        for file in input_files:
            if not isinstance(file, (File, str)):
                raise TypeError(
                    "invalid input_file: {file}; input_file(s) must be of type File or str".format(
                        file=file
                    )
                )

            if isinstance(file, str):
                file = File(file)

            _input = _Use(
                file,
                _LinkType.INPUT,
                register_replica=None,
                stage_out=None,
                bypass_staging=bypass_staging,
            )
            if _input in self.uses:
                raise DuplicateError(
                    "file: {file} has already been added as input to this job".format(
                        file=file.lfn
                    )
                )

            self.uses.add(_input)

    def get_inputs(self):
        """Get this job's input :py:class:`~Pegasus.api.replica_catalog.File` s

        :return: all input files associated with this job
        :rtype: set
        """
        return {use.file for use in self.uses if use._type == "input"}

    @_chained
    def add_outputs(
        self,
        *output_files: Union[File, str],
        stage_out: bool = True,
        register_replica: bool = True
    ):
        """
        add_outputs(self, *output_files: Union[File, str], stage_out: bool = True, register_replica: bool = True)
        Add one or more :py:class:`~Pegasus.api.replica_catalog.File` objects as outputs to this job. :code:`stage_out` and :code:`register_replica`
        will be applied to all files given.
        If :code:`output_file` is given as a str, a :py:class:`~Pegasus.api.replica_catalog.File` object is created for
        you internally with the given value as its lfn.

        :param output_files: the :py:class:`~Pegasus.api.replica_catalog.File` objects to be added as outputs to this job
        :type output_files: Union[File, str]
        :param stage_out: whether or not to send files back to an output directory, defaults to True
        :type stage_out: bool, optional
        :param register_replica: whether or not to register replica with a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog`, defaults to True
        :type register_replica: bool, optional
        :raises DuplicateError: all output files must be unique
        :raises TypeError: job outputs must be of type :py:class:`~Pegasus.api.replica_catalog.File` or str
        :return: self
        """
        for file in output_files:
            if not isinstance(file, (File, str)):
                raise TypeError(
                    "invalid output_file: {file}; output_file(s) must be of type File or str".format(
                        file=file
                    )
                )

            if isinstance(file, str):
                file = File(file)

            output = _Use(
                file,
                _LinkType.OUTPUT,
                stage_out=stage_out,
                register_replica=register_replica,
            )
            if output in self.uses:
                raise DuplicateError(
                    "file: {file} already added as output to this job".format(
                        file=file.lfn
                    )
                )

            self.uses.add(output)

    def get_outputs(self):
        """Get this job's output :py:class:`~Pegasus.api.replica_catalog.File` objects

        :return: all output files associated with this job
        :rtype: set
        """
        return {use.file for use in self.uses if use._type == "output"}

    @_chained
    def add_checkpoint(
        self,
        checkpoint_file: Union[File, str],
        stage_out: bool = True,
        register_replica: bool = True,
    ):
        """
        add_checkpoint(self, checkpoint_file: Union[File, str], stage_out: bool = True, register_replica: bool = True)
        Add an output :py:class:`~Pegasus.api.replica_catalog.File` of this job as a checkpoint file
        If :code:`checkpoint_file` is given as a str, a :py:class:`~Pegasus.api.replica_catalog.File` object is created for
        you internally with the given value as its lfn.

        :param checkpoint_file: the :py:class:`~Pegasus.api.replica_catalog.File` to be added as a checkpoint file to this job
        :type checkpoint_file: Union[File, str]
        :param stage_out: whether or not to send files back to an output directory, defaults to True
        :type stage_out: bool, optional
        :param register_replica: whether or not to register replica with a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog`, defaults to True
        :type register_replica: bool, optional
        :raises DuplicateError: all output files must be unique
        :raises TypeError: job inputs must be of type :py:class:`~Pegasus.api.replica_catalog.File` or str
        :return: self
        """

        if not isinstance(checkpoint_file, (File, str)):
            raise TypeError(
                "invalid checkpoint_file: {file}; checkpoint_file must be of type File or str".format(
                    file=checkpoint_file
                )
            )

        if isinstance(checkpoint_file, str):
            checkpoint_file = File(checkpoint_file)

        checkpoint = _Use(
            checkpoint_file,
            _LinkType.CHECKPOINT,
            stage_out=stage_out,
            register_replica=register_replica,
        )

        if checkpoint in self.uses:
            raise DuplicateError(
                "file: {file} already added as output to this job".format(
                    file=checkpoint_file.lfn
                )
            )

        self.uses.add(checkpoint)

    @_chained
    def add_args(self, *args: Union[File, int, float, str]):
        """
        add_args(self, *args: Union[File, int, float, str])
        Add arguments to this job. Each argument will be separated by a space.
        Each argument must be either a File, scalar, or str.

        :param args: arguments to pass to this job (each arg in arg will be separated by a space)
        :type args: Union[File, int, float, str]
        :return: self
        """
        self.args.extend(args)

    @_chained
    def set_stdin(self, file: Union[str, File]):
        """
        set_stdin(self, file: Union[str, File])
        Set stdin to a :py:class:`~Pegasus.api.replica_catalog.File` . If file
        is given as a str, a :py:class:`~Pegasus.api.replica_catalog.File` object
        is created for you internally with the given value as its lfn.

        :param file: a file that will be read into stdin
        :type file: Union[str, File]
        :raises TypeError: file must be of type :py:class:`~Pegasus.api.replica_catalog.File` or str
        :raises DuplicateError: stdin is already set or the given file has already been added as an input to this job
        :return: self
        """
        if not isinstance(file, (File, str)):
            raise TypeError(
                "invalid file: {file}; file must be of type File or str".format(
                    file=file
                )
            )

        if self.stdin is not None:
            raise DuplicateError("stdin has already been set to a file")

        if isinstance(file, str):
            file = File(file)

        self.add_inputs(file)
        self.stdin = file

    def get_stdin(self):
        """Get the :py:class:`~Pegasus.api.replica_catalog.File` being used for stdin

        :return: the stdin file
        :rtype: File
        """
        return self.stdin

    @_chained
    def set_stdout(
        self,
        file: Union[str, File],
        stage_out: bool = True,
        register_replica: bool = True,
    ):
        """
        set_stdout(self, file: Union[str, File], stage_out: bool = True, register_replica: bool  = True)
        Set stdout to a :py:class:`~Pegasus.api.replica_catalog.File` . If file is given as a str,
        a :py:class:`~Pegasus.api.replica_catalog.File` object is created for you internally
        with the given value as its lfn.

        :param file: a file that stdout will be written to
        :type file: Union[str, File]
        :param stage_out: whether or not to send files back to an output directory, defaults to True
        :type stage_out: bool, optional
        :param register_replica: whether or not to register replica with a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog`, defaults to True
        :type register_replica: bool, optional
        :raises TypeError: file must be of type :py:class:`~Pegasus.api.replica_catalog.File` or str
        :raises DuplicateError: stdout is already set or the given file has already been added as an output to this job
        :return: self
        """
        if not isinstance(file, (File, str)):
            raise TypeError(
                "invalid file: {file}; file must be of type File or str".format(
                    file=file
                )
            )

        if self.stdout is not None:
            raise DuplicateError("stdout has already been set to a file")

        if isinstance(file, str):
            file = File(file)

        self.add_outputs(file, stage_out=stage_out, register_replica=register_replica)
        self.stdout = file

    def get_stdout(self):
        """Get the :py:class:`~Pegasus.api.replica_catalog.File` being used for stdout

        :return: the stdout file
        :rtype: File
        """
        return self.stdout

    @_chained
    def set_stderr(
        self,
        file: Union[str, File],
        stage_out: bool = True,
        register_replica: bool = True,
    ):
        """
        set_stderr(self, file: Union[str, File], stage_out: bool = True, register_replica: bool = True)
        Set stderr to a :py:class:`~Pegasus.api.replica_catalog.File` . If file is given as a str,
        a :py:class:`~Pegasus.api.replica_catalog.File` object is created for you internally
        with the given value as its lfn.

        :param file: a file that stderr will be written to
        :type file: Union[str, File]
        :param stage_out: whether or not to send files back to an output directory, defaults to True
        :type stage_out: bool, optional
        :param register_replica: whether or not to register replica with a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog`, defaults to True
        :type register_replica: bool, optional
        :raises TypeError: file must be of type :py:class:`~Pegasus.api.replica_catalog.File` or str
        :raises DuplicateError: stderr is already set or the given file has already been added as an output to this job
        :return: self
        """
        if not isinstance(file, (File, str)):
            raise TypeError(
                "invalid file: {file}; file must be of type File or str".format(
                    file=file
                )
            )

        if self.stderr is not None:
            raise DuplicateError("stderr has already been set to a file")

        if isinstance(file, str):
            file = File(file)

        self.add_outputs(file, stage_out=stage_out, register_replica=register_replica)
        self.stderr = file

    def get_stderr(self):
        """Get the :py:class:`~Pegasus.api.replica_catalog.File` being used for stderr

        :return: the stderr file
        :rtype: File
        """
        return self.stderr

    def __json__(self):
        return _filter_out_nones(
            OrderedDict(
                [
                    ("id", self._id),
                    ("stdin", self.stdin.lfn if self.stdin is not None else None),
                    ("stdout", self.stdout.lfn if self.stdout is not None else None),
                    ("stderr", self.stderr.lfn if self.stderr is not None else None),
                    ("nodeLabel", self.node_label),
                    (
                        "arguments",
                        [
                            arg.lfn if isinstance(arg, File) else arg
                            for arg in self.args
                        ],
                    ),
                    ("uses", [use for use in self.uses]),
                    (
                        "profiles",
                        OrderedDict(sorted(self.profiles.items(), key=lambda _: _[0]))
                        if len(self.profiles) > 0
                        else None,
                    ),
                    ("metadata", self.metadata if len(self.metadata) > 0 else None),
                    (
                        "hooks",
                        OrderedDict(
                            [
                                (hook_name, [hook for hook in values])
                                for hook_name, values in self.hooks.items()
                            ]
                        )
                        if len(self.hooks) > 0
                        else None,
                    ),
                ]
            )
        )


class Job(AbstractJob):
    """
    A typical workflow Job that executes a :py:class:`~Pegasus.api.transformation_catalog.Transformation`.
    See :py:class:`~Pegasus.api.workflow.AbstractJob` for full list of available functions.

    .. code-block:: python

        # Example
        if1 = File("if1")
        if2 = File("if2")

        of1 = File("of1")
        of2 = File("of2")

        # Assuming a transformation named "analyze.py" has been added to your
        # transformation catalog:
        job = Job("analyze.py")\\
                .add_args("-i", if1, if2, "-o", of1, of2)\\
                .add_inputs(if1, if2)\\
                .add_outputs(of1, of2, stage_out=True, register_replica=False)

    """

    def __init__(
        self,
        transformation: Union[str, Transformation],
        _id: Optional[str] = None,
        node_label: Optional[str] = None,
        namespace: Optional[str] = None,
        version: Optional[str] = None,
    ):
        """
        :param transformation: :py:class:`~Pegasus.api.transformation_catalog.Transformation` object or name of the transformation that this job uses
        :type transformation: Union[str, Transformation]
        :param _id: a unique id; if none is given then one will be assigned when the job is added by a :py:class:`~Pegasus.api.workflow.Workflow`, defaults to None
        :type _id: Optional[str]
        :param node_label: a short descriptive label that can be assigned to this job, defaults to None
        :type node_label: Optional[str]
        :param namespace: namespace to which the :py:class:`~Pegasus.api.transformation_catalog.Transformation` belongs, defaults to None
        :type namespace: Optional[str]
        :param version: version of the given :py:class:`~Pegasus.api.transformation_catalog.Transformation`, defaults to None
        :type version: Optional[str]
        :raises TypeError: transformation must be one of type :py:class:`~Pegasus.api.transformation_catalog.Transformation` or str
        """
        if isinstance(transformation, Transformation):
            self.transformation = transformation.name
            self.namespace = transformation.namespace
            self.version = transformation.version
        elif isinstance(transformation, str):
            self.transformation = transformation
            self.namespace = namespace
            self.version = version
        else:
            raise TypeError(
                "invalid transformation: {transformation}; transformation must be of type Transformation or str".format(
                    transformation=transformation
                )
            )

        AbstractJob.__init__(self, _id=_id, node_label=node_label)

    def __json__(self):
        job_json = OrderedDict(
            [
                ("type", "job"),
                ("namespace", self.namespace),
                ("version", self.version),
                ("name", self.transformation),
            ]
        )

        job_json.update(AbstractJob.__json__(self))

        return _filter_out_nones(job_json)

    def __repr__(self):
        args = ""

        if self._id:
            args += "_id={}, ".format(self._id)

        if self.namespace:
            args += "namespace={}, ".format(self.namespace)

        args += "transformation={}".format(self.transformation)

        if self.version:
            args += ", version={}".format(self.version)

        if self.node_label:
            args += ", node_label={}".format(self.node_label)

        return "Job({})".format(args)


class SubWorkflow(AbstractJob):
    """
    Job that represents a subworkflow.
    See :py:class:`~Pegasus.api.workflow.AbstractJob` for full list of available functions.
    SubWorkflow jobs can be created using several different methods. These are outlined
    below.

    .. code-block:: python

        root_wf = Workflow("root")
        # "workflow.yml" must be added to the ReplicaCatalog
        j1 = SubWorkflow(file="workflow.yml", is_planned=False)
        root_wf.add_jobs(j1)

        another_wf = Workflow("another-wf")
        j2 = SubWorkflow(another_wf, _id="j2")
        root_wf.add_jobs(j2)

        # Upon invoking root_wf.write() or root_wf.plan(), another_wf will automatically
        # be serialized to CWD / "another-wf_j2.yml" and added to an inline ReplicaCatalog;
        # This means that you may define "another_wf" in a separate python script, and
        # import it here where it would be used.
        root_wf.write()
    """

    def __init__(
        self,
        file: Union[str, File, "Workflow"],
        is_planned: bool = False,
        _id: Optional[str] = None,
        node_label: Optional[str] = None,
    ):
        """
        :param file: :py:class:`~Pegasus.api.replica_catalog.File`, the name of the workflow file as a :code:`str`, or :py:class:`~Pegasus.api.workflow.Workflow`
        :type file: Union[str, File, Workflow]
        :param is_planned: whether or not this subworkflow has already been planned by the Pegasus planner, defaults to False
        :type is_planned: bool
        :param _id: a unique id; if none is given then one will be assigned when the job is added by a :py:class:`~Pegasus.api.workflow.Workflow`, defaults to None
        :type _id: Optional[str]
        :param node_label: a short descriptive label that can be assigned to this job, defaults to None
        :type node_label: Optional[str]
        :raises TypeError: file must be of type :py:class:`~Pegasus.api.replica_catalog.File` or str
        """
        AbstractJob.__init__(self, _id=_id, node_label=node_label)

        if not isinstance(file, (File, str, Workflow)):
            raise TypeError(
                "invalid file: {file}; file must be of type File, str, or Workflow".format(
                    file=file
                )
            )

        self.type = "condorWorkflow" if is_planned else "pegasusWorkflow"

        if isinstance(file, File):
            self.file = file.lfn
        else:
            self.file = file

        # ensure that add_planner_args() is not invoked multiple times for each SubWorkflow
        # instance as this will create duplicate arguments
        self._planner_args_already_set = False

        if not isinstance(self.file, Workflow):
            self.add_inputs(File(self.file, for_planning=True))

    @_chained
    def add_planner_args(
        self,
        *,
        conf: Optional[Union[str, Path]] = None,
        basename: Optional[str] = None,
        job_prefix: Optional[str] = None,
        cluster: Optional[List[str]] = None,
        sites: Optional[List[str]] = None,
        output_sites: Optional[List[str]] = None,
        staging_sites: Optional[Dict[str, str]] = None,
        cache: Optional[List[Union[str, Path]]] = None,
        input_dirs: Optional[List[Union[str, Path]]] = None,
        output_dir: Optional[Union[str, Path]] = None,
        dir: Optional[Union[str, Path]] = None,
        relative_dir: Optional[Union[str, Path]] = None,
        random_dir: Union[bool, str, Path] = False,
        relative_submit_dir: Optional[Union[str, Path]] = None,
        inherited_rc_files: Optional[List[Union[str, Path]]] = None,
        cleanup: Optional[str] = None,
        reuse: Optional[List[Union[str, Path]]] = None,
        verbose: int = 0,
        quiet: int = 0,
        force: bool = False,
        force_replan: bool = False,
        forward: Optional[List[str]] = None,
        submit: bool = False,
        java_options: Optional[List[str]] = None,
        **properties: Dict[str, str]
    ):
        r"""
        add_planner_args(self, conf: Optional[Union[str, Path]] = None, basename: Optional[str] = None, job_prefix: Optional[str] = None, cluster: Optional[List[str]] = None, sites: Optional[List[str]] = None, output_sites: Optional[List[str]] = None, staging_sites: Optional[Dict[str, str]] = None, cache: Optional[List[Union[str, Path]]] = None, input_dirs: Optional[List[str]] = None, output_dir: Optional[str] = None, dir: Optional[str] = None, relative_dir: Optional[Union[str, Path]] = None, random_dir: Union[bool, str, Path] = False, relative_submit_dir: Optional[Union[str, Path]] = None, inherited_rc_files: Optional[List[Union[str, Path]]] = None, cleanup: Optional[str] = None, reuse: Optional[List[Union[str,Path]]] = None, verbose: int = 0, quiet: int = 0, force: bool = False, force_replan: bool = False, forward: Optional[List[str]] = None, submit: bool = False, json: bool = False, java_options: Optional[List[str]] = None, **properties: Dict[str, str])
        Add pegasus-planner arguments. This function can only be used when
        :code:`is_planned=False` is set in :py:class:`~Pegasus.api.workflow.SubWorkflow` and
        may only be invoked once.

        :param conf:  the path to the properties file to use for planning, defaults to None
        :type conf: Optional[Union[str, Path]]
        :param basename: the basename prefix while constructing the per workflow files like .dag etc., defaults to None
        :type basename: Optional[str]
        :param job_prefix: the prefix to be applied while construction job submit filenames, defaults to None
        :type job_prefix: Optional[str]
        :param cluster: comma separated list of clustering techniques to be applied to the workflow to cluster jobs in to larger jobs, to avoid scheduling overheads., defaults to None
        :type cluster: Optional[List[str]]
        :param sites: list of execution sites on which to map the workflow, defaults to None
        :type sites: Optional[List[str]]
        :param output_sites: the output sites where the data products during workflow execution are transferred to, defaults to None
        :type output_sites: Optional[List[str]]
        :param staging_sites: key, value pairs of execution site to staging site mappings such as :code:`{"condorpool": "staging-site"}`, defaults to None
        :type staging_sites: Optional[Dict[str,str]]
        :param cache: comma separated list of replica cache files, defaults to None
        :type cache: Optional[List[Union[str, Path]]]
        :param input_dirs: comma separated list of optional input directories where the input files reside on submit host, defaults to None
        :type input_dirs: Optional[List[Union[str, Path]]]
        :param output_dir: an optional output directory where the output files should be transferred to on submit host, defaults to None
        :type output_dir: Optional[Union[str, Path]]
        :param dir: the directory where to generate the executable workflow, defaults to None
        :type dir: Optional[Union[str, Path]]
        :param relative_dir: the relative directory to the base directory where to generate the concrete workflow, defaults to None
        :type relative_dir: Optional[Union[str, Path]]
        :param random_dir: if set to :code:`True`, a random timestamp based name will be used for the execution directory that is created by the create dir jobs; else if a path is given as a :code:`str` or :code:`pathlib.Path`, then that will be used as the basename of the directory that is to be created, defaults to False
        :type random_dir: Union[bool, str, Path], optional
        :param relative_submit_dir: the relative submit directory where to generate the concrete workflow. Overrides relative_dir, defaults to None
        :type relative_submit_dir: Optional[Union[str, Path]]
        :param inherited_rc_files: comma separated list of replica files, defaults to None
        :type inherited_rc_files: Optional[List[Union[str, Path]]]
        :param cleanup: the cleanup strategy to use. Can be a :code:`str` :code:`none|inplace|leaf|constraint`, defaults to None (internally, pegasus-plan will use the default :code:`inplace` if :code:`None` is given)
        :type cleanup: Optional[str]
        :param reuse: list of submit directories of previous runs from which to pick up for reuse (e.g. :code:`["/workflows/submit_dir1", "/workflows/submit_dir2"]`), defaults to None
        :type reuse: Optional[List[Union[str,Path]]]
        :param verbose: verbosity, defaults to 0
        :type verbose: int, optional
        :param quiet: decreases the verbosity of messages about what is going on, defaults to 0
        :type quiet: int
        :param force: skip reduction of the workflow, resulting in build style dag, defaults to False
        :type force: bool, optional
        :param force_replan: force replanning for sub workflows in case of failure, defaults to False
        :type force_replan: bool
        :param forward: any options that need to be passed ahead to pegasus-run in format option[=value] (e.g. :code:`["nogrid"]`), defaults to None
        :type forward: Optional[List[str]]
        :param submit: submit the executable workflow generated, defaults to False
        :type submit: bool, optional
        :param java_options: pass to jvm a non standard option (e.g. :code:`["mx1024m", "ms512m"]`), defaults to None
        :type java_options: Optional[List[str]]
        :param \*\*properties: configuration properties (e.g. :code:`**{"pegasus.mode": "development"}`, which would be passed to pegasus-plan as :code:`-Dpegasus.mode=development`). Note that configuration properties set here take precedance over the properties file property with the same key.
        :raises TypeError: an invalid type was given for one or more arguments
        :raises PegasusError: SubWorkflow.add_planner_args() can only be called by SubWorkflows that have not yet been planned (i.e. SubWorkflow('wf_file', is_planned=False))
        :raises PegasusError: SubWorkflow.add_planner_args() can only be invoked once
        :return: self
        """
        # planner arguments can only be added when SubWorkflow is yet to be planned
        if self.type == "condorWorkflow":
            raise PegasusError(
                "SubWorkflow.add_planner_args() can only be called by SubWorkflows that have not been planned. (i.e. SubWorkflow('wf_file', is_planned=False)"
            )

        # ensure that this is only called once
        if self._planner_args_already_set:
            raise PegasusError(
                "SubWorkflow.add_planner_args() can only be invoked once"
            )

        self._planner_args_already_set = True

        for k, v in properties.items():
            self.add_args("-D{}={}".format(k, v))

        if basename:
            self.add_args("--basename", basename)

        if job_prefix:
            self.add_args("--job-prefix", job_prefix)

        if conf:
            self.add_args("--conf", str(conf))

        if cluster:
            if not isinstance(cluster, list):
                raise TypeError(
                    "invalid cluster: {}; list of str must be given".format(cluster)
                )

            self.add_args("--cluster", ",".join(cluster))

        if sites:
            if not isinstance(sites, list):
                raise TypeError(
                    "invalid sites: {}; list of str must be given".format(sites)
                )
            self.add_args("--sites", ",".join(sites))

        if output_sites:
            if not isinstance(output_sites, list):
                raise TypeError(
                    "invalid output_sites: {}; list of str must be given".format(
                        output_sites
                    )
                )

            self.add_args("--output-sites", ",".join(output_sites))

        if staging_sites:
            if not isinstance(staging_sites, dict):
                raise TypeError(
                    "invalid staging_sites: {}; dict<str, str> must be given".format(
                        staging_sites
                    )
                )

            self.add_args(
                "--staging-site",
                ",".join(
                    "{site}={staging_site}".format(site=s, staging_site=ss)
                    for s, ss in staging_sites.items()
                ),
            )

        if cache:
            if not isinstance(cache, list):
                raise TypeError(
                    "invalid cache: {}; list of str must be given".format(cache)
                )

            self.add_args("--cache", ",".join(str(c) for c in cache))

        if input_dirs:
            if not isinstance(input_dirs, list):
                raise TypeError(
                    "invalid input_dirs: {}; list of str must be given".format(
                        input_dirs
                    )
                )

            self.add_args("--input-dir", ",".join(str(_id) for _id in input_dirs))

        if output_dir:
            self.add_args("--output-dir", str(output_dir))

        if dir:
            self.add_args("--dir", str(dir))

        if relative_dir:
            self.add_args("--relative-dir", str(relative_dir))

        if relative_submit_dir:
            self.add_args("--relative-submit-dir", str(relative_submit_dir))

        if random_dir:
            if random_dir == True:
                self.add_args("--randomdir")
            else:
                self.add_args("--randomdir={}".format(random_dir))

        if inherited_rc_files:
            if not isinstance(inherited_rc_files, list):
                raise TypeError(
                    "invalid inherited_rc_files: {}; list of str must be given".format(
                        inherited_rc_files
                    )
                )

            self.add_args(
                "--inherited-rc-files", ",".join(str(f) for f in inherited_rc_files)
            )

        if cleanup:
            self.add_args("--cleanup", cleanup)

        if reuse:
            self.add_args("--reuse", ",".join(str(p) for p in reuse))

        if verbose > 0:
            self.add_args("-" + ("v" * verbose))

        if quiet > 0:
            self.add_args("-" + ("q" * quiet))

        if force:
            self.add_args("--force")

        if force_replan:
            self.add_args("--force-replan")

        if forward:
            if not isinstance(forward, list):
                raise TypeError(
                    "invalid forward: {}; list of str must be given".format(forward)
                )

            for opt in forward:
                self.add_args("--forward", opt)

        if submit:
            self.add_args("--submit")

        if java_options:
            if not isinstance(java_options, list):
                raise TypeError(
                    "invalid java_options: {}; list of str must be given".format(
                        java_options
                    )
                )

            for opt in java_options:
                self.add_args("-X{}".format(opt))

    def __json__(self):
        # error should only be raised internally if SubWorkflow was given a Workflow
        # object, then __json__() is invoked before a call to Workflow.write(),
        # which is where the SubWorkflow would have been serialized to a file
        if isinstance(self.file, Workflow):
            raise PegasusError("the given SubWorkflow file must be a File object")

        dax_json = OrderedDict([("type", self.type), ("file", self.file)])
        dax_json.update(AbstractJob.__json__(self))

        return dax_json

    def __repr__(self):

        args = ""

        if self._id:
            args += "_id={}, ".format(self._id)

        args += "file={}".format(self.file)

        if self.type == "condorWorkflow":
            args += ", is_planned=True"
        else:
            args += ", is_planned=False"

        if self.node_label:
            args += ", node_label={}".format(self.node_label)

        return "SubWorkflow({})".format(args)


class _LinkType(Enum):
    """Internal class defining link types"""

    INPUT = "input"
    OUTPUT = "output"
    CHECKPOINT = "checkpoint"


class _Use:
    """Internal class used to represent input and output files of a job"""

    def __init__(
        self,
        file,
        link_type,
        stage_out=True,
        register_replica=True,
        bypass_staging=False,
    ):
        if not isinstance(file, File):
            raise TypeError(
                "invalid file: {file}; file must be of type File".format(file=file)
            )

        self.file = file

        if not isinstance(link_type, _LinkType):
            raise TypeError(
                "invalid link_type: {link_type}; link_type must one of {enum_str}".format(
                    link_type=link_type, enum_str=_get_enum_str(_LinkType)
                )
            )

        if link_type != _LinkType.INPUT and bypass_staging:
            raise ValueError("bypass can only be set to True when link type is INPUT")

        self.bypass = None
        if bypass_staging:
            self.bypass = bypass_staging

        self._type = link_type.value

        self.stage_out = stage_out
        self.register_replica = register_replica

    def __hash__(self):
        return hash(self.file)

    def __eq__(self, other):
        if isinstance(other, _Use):
            return self.file.lfn == other.file.lfn
        raise ValueError("_Use cannot be compared with {}".format(type(other)))

    def __json__(self):
        return _filter_out_nones(
            OrderedDict(
                [
                    ("lfn", self.file.lfn),
                    (
                        "metadata",
                        self.file.metadata if len(self.file.metadata) > 0 else None,
                    ),
                    ("size", self.file.size),
                    ("forPlanning", self.file.for_planning),
                    ("type", self._type),
                    ("stageOut", self.stage_out),
                    ("registerReplica", self.register_replica),
                    ("bypass", self.bypass),
                ]
            )
        )


class _JobDependency:
    """Internal class used to represent a jobs dependencies within a workflow"""

    def __init__(self, parent_id, children_ids):
        self.parent_id = parent_id
        self.children_ids = children_ids

    def __eq__(self, other):
        if isinstance(other, _JobDependency):
            return (
                self.parent_id == other.parent_id
                and self.children_ids == other.children_ids
            )
        raise ValueError(
            "_JobDependency cannot be compared with {}".format(type(other))
        )

    def __json__(self):
        return OrderedDict(
            [("id", self.parent_id), ("children", list(self.children_ids))]
        )


def _needs_client(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if not self._client:
            self._client = from_env()

        return f(self, *args, **kwargs)

    return wrapper


def _needs_submit_dir(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if not self._submit_dir:
            raise PegasusError(
                "{f} requires a submit directory to be set; Workflow.plan() must be called prior to {f}".format(
                    f=f
                )
            )

        return f(self, *args, **kwargs)

    return wrapper


class Workflow(Writable, HookMixin, ProfileMixin, MetadataMixin):
    """Represents multi-step computational steps as a directed
    acyclic graph.

    .. code-block:: python

        # Example
        import logging

        from pathlib import Path

        from Pegasus.api import *

        logging.basicConfig(level=logging.DEBUG)

        # --- Replicas -----------------------------------------------------------------
        with open("f.a", "w") as f:
            f.write("This is sample input to KEG")

        fa = File("f.a").add_metadata(creator="ryan")
        rc = ReplicaCatalog().add_replica("local", fa, Path(".") / "f.a")

        # --- Transformations ----------------------------------------------------------
        preprocess = Transformation(
                        "preprocess",
                        site="condorpool",
                        pfn="/usr/bin/pegasus-keg",
                        is_stageable=False,
                        arch=Arch.X86_64,
                        os_type=OS.LINUX
                    )

        findrange = Transformation(
                        "findrange",
                        site="condorpool",
                        pfn="/usr/bin/pegasus-keg",
                        is_stageable=False,
                        arch=Arch.X86_64,
                        os_type=OS.LINUX
                    )

        analyze = Transformation(
                        "analyze",
                        site="condorpool",
                        pfn="/usr/bin/pegasus-keg",
                        is_stageable=False,
                        arch=Arch.X86_64,
                        os_type=OS.LINUX
                    )

        tc = TransformationCatalog().add_transformations(preprocess, findrange, analyze)

        # --- Workflow -----------------------------------------------------------------
        '''
                            [f.b1] - (findrange) - [f.c1]
                            /                             \\
        [f.a] - (preprocess)                               (analyze) - [f.d]
                            \\                             /
                            [f.b2] - (findrange) - [f.c2]

        '''
        wf = Workflow("blackdiamond")

        fb1 = File("f.b1")
        fb2 = File("f.b2")
        job_preprocess = Job(preprocess)\\
                            .add_args("-a", "preprocess", "-T", "3", "-i", fa, "-o", fb1, fb2)\\
                            .add_inputs(fa)\\
                            .add_outputs(fb1, fb2)

        fc1 = File("f.c1")
        job_findrange_1 = Job(findrange)\\
                            .add_args("-a", "findrange", "-T", "3", "-i", fb1, "-o", fc1)\\
                            .add_inputs(fb1)\\
                            .add_outputs(fc1)

        fc2 = File("f.c2")
        job_findrange_2 = Job(findrange)\\
                            .add_args("-a", "findrange", "-T", "3", "-i", fb2, "-o", fc2)\\
                            .add_inputs(fb2)\\
                            .add_outputs(fc2)

        fd = File("f.d")
        job_analyze = Job(analyze)\\
                        .add_args("-a", "analyze", "-T", "3", "-i", fc1, fc2, "-o", fd)\\
                        .add_inputs(fc1, fc2)\\
                        .add_outputs(fd)

        wf.add_jobs(job_preprocess, job_findrange_1, job_findrange_2, job_analyze)
        wf.add_replica_catalog(rc)
        wf.add_transformation_catalog(tc)

        try:
            wf.plan(submit=True)\\
                .wait()\\
                .analyze()\\
                .statistics()
        except PegasusClientError as e:
            print(e.output)


    """

    _DEFAULT_FILENAME = "workflow.yml"

    def __init__(self, name: str, infer_dependencies: bool = True ,tracker_type: str ="full"):
        """
        :param name: name of the :py:class:`~Pegasus.api.workflow.Workflow`
        :type name: str
        :param infer_dependencies: whether or not to automatically compute job dependencies based on input and output files used by each job, defaults to True
        :type infer_dependencies: bool, optional
        :raises ValueError: workflow name may not contain any / or spaces
        """

        if any(c in name for c in "/ "):
            raise ValueError(
                "Invalid workflow name: {}, workflow name may not contain any / or spaces".format(
                    name
                )
            )

        Writable.__init__(self)

        self.name = name
        self.infer_dependencies = infer_dependencies

        # client specific members
        self._submit_dir = None
        self._braindump = None

        # set/overridden by call to Workflow.run
        self._run_output = None

        self._client = None

        self._has_subworkflow_jobs = False

        # sequence unique to this workflow only
        self.sequence = 1

        self.jobs = OrderedDict()
        self.dependencies = defaultdict(_JobDependency)

        self.site_catalog = None
        self.transformation_catalog = None
        self.replica_catalog = None

        self.hooks = defaultdict(list)
        self.profiles = defaultdict(OrderedDict)
        self.metadata = OrderedDict()

        self.tracker_type=tracker_type

    @property
    @_needs_submit_dir
    def braindump(self):
        """Once this workflow has been planned using :py:class:`~Pegasus.api.workflow.Workflow.plan`,
        the braindump file can be accessed for information such as :code:`user`, :code:`submit_dir`, and
        :code:`root_wf_uuid`. For a full list of available attributes, see :py:class:`~Pegasus.braindump.Braindump`.

        .. code-block:: python

            try:
                wf.plan(submit=True)

                print(wf.braindump.user)
                print(wf.braindump.submit_hostname)
                print(wf.braindump.submit_dir)
                print(wf.braindump.root_wf_uuid)
                print(wf.braindump.wf_uuid)
            except PegasusClientError as e:
                print(e.output)


        :getter: returns a :py:class:`~Pegasus.braindump.Braindump` object corresponding to the most recent call of :py:class:`~Pegasus.api.workflow.Workflow.plan`
        :rtype: Pegasus.braindump.Braindump
        :raises PegasusError: :py:class:`~Pegasus.api.workflow.Workflow.plan` must be called before accessing the braindump file
        """
        return self._braindump

    @property
    def run_output(self):
        """Get the json output from pegasus-run after it has been called.

        .. code-block:: python

            try:
                wf.plan()
                wf.run()

                print(wf.run_output)
            except PegasusClientError as e:
                print(e.output)


        :raises PegasusError: :py:class:`~Pegasus.api.workflow.Workflow.run` must be called prior to accessing run_output
        :return: output of pegasus-run
        :rtype: dict
        """
        if not self._run_output:
            raise PegasusError(
                "Workflow.run must be called before run_output can be accessed"
            )

        return self._run_output

    @_chained
    @_needs_client
    def plan(
        self,
        *,
        conf: Optional[Union[str, Path]] = None,
        basename: Optional[str] = None,
        job_prefix: Optional[str] = None,
        cluster: Optional[List[str]] = None,
        sites: Optional[List[str]] = None,
        output_sites: List[str] = ["local"],
        staging_sites: Optional[Dict[str, str]] = None,
        cache: Optional[List[Union[str, Path]]] = None,
        input_dirs: Optional[List[Union[str, Path]]] = None,
        output_dir: Optional[Union[str, Path]] = None,
        dir: Optional[Union[str, Path]] = None,
        relative_dir: Optional[Union[str, Path]] = None,
        random_dir: Union[bool, str, Path] = False,
        relative_submit_dir: Optional[Union[str, Path]] = None,
        inherited_rc_files: Optional[List[Union[str, Path]]] = None,
        cleanup: str = "inplace",
        reuse: Optional[List[Union[str, Path]]] = None,
        verbose: int = 0,
        quiet: int = 0,
        force: bool = False,
        force_replan: bool = False,
        forward: Optional[List[str]] = None,
        submit: bool = False,
        java_options: Optional[List[str]] = None,
        **properties: Dict[str, str]
    ):
        r"""
        plan(self, conf: Optional[Union[str, Path]] = None, basename: Optional[str] = None, job_prefix: Optional[str] = None, cluster: Optional[List[str]] = None, sites: Optional[List[str]] = None, output_sites: List[str] = ["local"], staging_sites: Optional[Dict[str, str]] = None, cache: Optional[List[Union[str, Path]]] = None, input_dirs: Optional[List[str]] = None, output_dir: Optional[str] = None, dir: Optional[str] = None, relative_dir: Optional[Union[str, Path]] = None, random_dir: Union[bool, str, Path] = False, relative_submit_dir: Optional[Union[str, Path]] = None, inherited_rc_files: Optional[List[Union[str, Path]]] = None, cleanup: str = "inplace", reuse: Optional[List[Union[str,Path]]] = None, verbose: int = 0, quiet: int = 0, force: bool = False, force_replan: bool = False, forward: Optional[List[str]] = None, submit: bool = False, json: bool = False, java_options: Optional[List[str]] = None, **properties: Dict[str,str])
        Plan the workflow.

        .. code-block:: python

            try:
                # configuration properties
                properties = {"pegasus.mode": "development}

                wf.plan(verbose=3, submit=True, **properties)
            except PegasusClientError as e:
                print(e.output)

        :param conf:  the path to the properties file to use for planning, defaults to None
        :type conf: Optional[Union[str, Path]]
        :param basename: the basename prefix while constructing the per workflow files like .dag etc., defaults to None
        :type basename: Optional[str]
        :param job_prefix: the prefix to be applied while construction job submit filenames, defaults to None
        :type job_prefix: Optional[str]
        :param cluster: comma separated list of clustering techniques to be applied to the workflow to cluster jobs in to larger jobs, to avoid scheduling overheads., defaults to None
        :type cluster: Optional[List[str]]
        :param sites: list of execution sites on which to map the workflow, defaults to None
        :type sites: Optional[List[str]]
        :param output_sites: the output sites where the data products during workflow execution are transferred to, defaults to :code:`["local"]`
        :type output_sites: List[str]
        :param staging_sites: key, value pairs of execution site to staging site mappings such as :code:`{"condorpool": "staging-site"}`, defaults to None
        :type staging_sites: Optional[Dict[str,str]]
        :param cache: comma separated list of replica cache files, defaults to None
        :type cache: Optional[List[Union[str, Path]]]
        :param input_dirs: comma separated list of optional input directories where the input files reside on submit host, defaults to None
        :type input_dirs: Optional[List[Union[str, Path]]]
        :param output_dir: an optional output directory where the output files should be transferred to on submit host, defaults to None
        :type output_dir: Optional[Union[str, Path]]
        :param dir: the directory where to generate the executable workflow, defaults to None
        :type dir: Optional[Union[str, Path]]
        :param relative_dir: the relative directory to the base directory where to generate the concrete workflow, defaults to None
        :type relative_dir: Optional[Union[str, Path]]
        :param random_dir: if set to :code:`True`, a random timestamp based name will be used for the execution directory that is created by the create dir jobs; else if a path is given as a :code:`str` or :code:`pathlib.Path`, then that will be used as the basename of the directory that is to be created, defaults to False
        :type random_dir: Union[bool, str, Path], optional
        :param relative_submit_dir: the relative submit directory where to generate the concrete workflow. Overrides relative_dir, defaults to None
        :type relative_submit_dir: Optional[Union[str, Path]]
        :param inherited_rc_files: comma separated list of replica files, defaults to None
        :type inherited_rc_files: Optional[List[Union[str, Path]]]
        :param cleanup: the cleanup strategy to use. Can be :code:`none|inplace|leaf|constraint`, defaults to :code:`inplace`
        :type cleanup: str, optional
        :param reuse: list of submit directories of previous runs from which to pick up for reuse (e.g. :code:`["/workflows/submit_dir1", "/workflows/submit_dir2"]`), defaults to None
        :type reuse: Optional[List[Union[str,Path]]]
        :param verbose: verbosity, defaults to 0
        :type verbose: int, optional
        :param quiet: decreases the verbosity of messages about what is going on, defaults to 0
        :type quiet: int
        :param force: skip reduction of the workflow, resulting in build style dag, defaults to False
        :type force: bool, optional
        :param force_replan: force replanning for sub workflows in case of failure, defaults to False
        :type force_replan: bool
        :param forward: any options that need to be passed ahead to pegasus-run in format option[=value] (e.g. :code:`["nogrid"]`), defaults to None
        :type forward: Optional[List[str]]
        :param submit: submit the executable workflow generated, defaults to False
        :type submit: bool, optional
        :param java_options: pass to jvm a non standard option (e.g. :code:`["mx1024m", "ms512m"]`), defaults to None
        :type java_options: Optional[List[str]]
        :param \*\*properties: configuration properties (e.g. :code:`**{"pegasus.mode": "development"}`, which would be passed to pegasus-plan as :code:`-Dpegasus.mode=development`). Note that configuration properties set here take precedance over the properties file property with the same key.
        :raises PegasusClientError: pegasus-plan encountered an error
        :return: self
        """
        # if the workflow has not yet been written to a file and plan is
        # called, write the file to default
        if not self._path:
            # self._path is set by write
            self.write()

        workflow_instance = self._client.plan(
            abstract_workflow=self._path,
            basename=basename,
            job_prefix=job_prefix,
            cluster=cluster,
            cache=[str(c) for c in cache] if cache else None,
            conf=str(conf) if conf else None,
            sites=sites,
            output_sites=output_sites,
            staging_sites=staging_sites,
            input_dirs=[str(_dir) for _dir in input_dirs] if input_dirs else None,
            output_dir=str(output_dir) if output_dir else None,
            dir=str(dir) if dir else None,
            relative_dir=str(relative_dir) if relative_dir else None,
            relative_submit_dir=str(relative_submit_dir)
            if relative_submit_dir
            else None,
            inherited_rc_files=[str(p) for p in inherited_rc_files]
            if inherited_rc_files
            else None,
            random_dir=random_dir if isinstance(random_dir, bool) else str(random_dir),
            cleanup=cleanup,
            reuse=[str(submit_dir) for submit_dir in reuse] if reuse else None,
            verbose=verbose,
            quiet=quiet,
            force=force,
            force_replan=force_replan,
            forward=forward,
            submit=submit,
            java_options=java_options,
            **properties,
        )

        self._submit_dir = workflow_instance._submit_dir
        self._braindump = workflow_instance.braindump

    @_chained
    @_needs_submit_dir
    @_needs_client
    def run(self, *, verbose: int = 0, grid: bool = False):
        """
        run(self, verbose: int = 0, json: bool = False, grid: bool = False)
        Run the planned workflow.

        :param verbose: verbosity, defaults to 0
        :type verbose: int, optional
        :param grid: enable checking for grids, defaults to False
        :type grid: bool, optional
        :raises PegasusClientError: pegasus-run encountered an error
        :return: self
        """
        self._run_output = self._client.run(
            self._submit_dir, verbose=verbose, grid=grid
        )

    @_chained
    @_needs_submit_dir
    @_needs_client
    def status(self, *, long: bool = False, verbose: int = 0):
        """
        status(self, long: bool = False, verbose: int = 0)
        Monitor the workflow by quering Condor and directories.

        :param long: Show all DAG states, including sub-DAGs, default only totals. defaults to False
        :type long: bool, optional
        :param verbose:  verbosity, defaults to False
        :type verbose: int, optional
        :raises PegasusClientError: pegasus-status encountered an error
        :return: self
        """

        self._client.status(self._submit_dir, long=long, verbose=verbose)

    @_needs_submit_dir
    @_needs_client
    def get_status(self) -> Union[dict, None]:
        """
        get_status(self)

        Returns current status information of the workflow as a dict in the
        following format:

        .. code-block:: python

            {
                "totals": {
                    "unready": <int>,
                    "ready": <int>,
                    "pre": <int>,
                    "queued": <int>,
                    "post": <int>,
                    "succeeded": <int>,
                    "failed": <int>,
                    "percent_done": <float>,
                    "total": <int>
                },
                "dags": {
                    "root": {
                        "unready": <int>,
                        "ready": <int>,
                        "pre": <int>,
                        "queued": <int>,
                        "post": <int>,
                        "succeeded": <int>,
                        "failed": <int>,
                        "percent_done": <float>,
                        "state": <"Running" | "Success" | "Failure">,
                        "dagname": <str>
                    }
                }
            }

        Keys are defined as follows
            * :code:`unready`: Jobs blocked by dependencies
            * :code:`ready`: Jobs ready for submission
            * :code:`pre`: PRE-Scripts running
            * :code:`queued`: Submitted jobs
            * :code:`post`: POST-Scripts running
            * :code:`succeeded`: Job completed with success
            * :code:`failed`: Jobs completed with failure
            * :code:`percent_done`: Success percentage
            * :code:`state`: Workflow state
            * :code:`dagname`: Name of workflow

        :return: current status information
        :rtype: Union[dict, None]
        """
        return self._client.get_status(
            root_wf_name=self.name, submit_dir=self._submit_dir
        )

    @_chained
    @_needs_submit_dir
    @_needs_client
    def wait(self, *, delay: int = 5):
        """
        wait(self, delay: int = 5)
        Displays progress bar to stdout and blocks until the workflow either
        completes or fails.

        :param delay: refresh rate in seconds of the progress bar, defaults to 5
        :type delay: int, optional
        :raises PegasusClientError: pegasus-status encountered an error
        :return: self
        """

        self._client.wait(self.name, self._submit_dir, delay=delay)

    @_chained
    @_needs_submit_dir
    @_needs_client
    def remove(self, *, verbose: int = 0):
        """
        remove(self, verbose: int = 0)
        Removes this workflow that has been planned and submitted.

        :param verbose:  verbosity, defaults to 0
        :type verbose: int, optional
        :raises PegasusClientError: pegasus-remove encountered an error
        :return: self
        """
        self._client.remove(self._submit_dir, verbose=verbose)

    @_chained
    @_needs_submit_dir
    @_needs_client
    def analyze(self, *, verbose: int = 0):
        """
        analyze(self, verbose: int = 0)
        Debug a workflow.

        :param verbose: verbosity, defaults to 0
        :type verbose: int, optional
        :raises PegasusClientError: pegasus-analyzer encountered an error
        :return: self
        """
        self._client.analyzer(self._submit_dir, verbose=verbose)

    # should wait until wf is done or else we will just get msg:
    # pegasus-monitord still running. Please wait for it to complete.
    @_chained
    @_needs_submit_dir
    @_needs_client
    def statistics(self, *, verbose: int = 0):
        """
        statistics(self, verbose: int = 0)
        Generate statistics about the workflow run.

        :param verbose:  verbosity, defaults to 0
        :type verbose: int, optional
        :raises PegasusClientError: pegasus-statistics encountered an error
        :return: self
        """
        self._client.statistics(self._submit_dir, verbose=verbose)

    @_chained
    @_needs_client
    def graph(
        self,
        include_files: bool = True,
        no_simplify: bool = True,
        label: str = "label",
        output: Optional[str] = None,
        remove: Optional[List[str]] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
    ):
        """
        graph(self, include_files: bool = True, no_simplify: bool = True, label: Literal["label", "xform", "id", "xform-id", "label-xform", "label-id"] = "label", output: Optional[str] = None, remove: Optional[List[str]] = None, width: Optional[int] = None, height: Optional[int] = None)
        Convert workflow into a graphviz dot format

        :param include_files: include files as nodes, defaults to True
        :type include_files: bool, optional
        :param no_simplify: when set to :code:`False` a transitive reduction is performed to remove extra edges, defaults to True
        :type no_simplify: bool, optional
        :param label: what attribute to use for labels, defaults to "label"
        :type label: Literal["label", "xform", "id", "xform-id", "label-xform", "label-id"], optional
        :param output: Write output to a file. If none is given dot output is written to stdout. If output is a file name with any of the following extensions: "jpg", "jpeg", "pdf", "gif", or "svg", :code:`dot -T<ext> -o <output>` will be invoked to draw the graph. If any other extension is given, the dot representation of the graph will be output. defaults to None
        :type output: Optional[str], optional
        :param remove: remove one or more nodes by transformation name, defaults to None
        :type remove: Optional[List[str]], optional
        :param width: width of the digraph, defaults to None
        :type width: Optional[int], optional
        :param height: height of the digraph, defaults to None
        :type height: Optional[int], optional
        :return: self
        :raises PegasusError: workflow must be written to a file prior to invoking :py:class:`~Pegasus.api.workflow.Workflow.graph` with :py:class:`~Pegasus.api.workflow.Workflow.write` or :py:class:`~Pegasus.api.workflow.Workflow.plan`
        :raises ValueError: label must be one of :code:`label`, :code:`xform`, :code:`id`, :code:`xform-id`, :code:`label-xform`, or :code:`label-id`
        """

        # check that workflow has been written
        if not self._path:
            raise PegasusError(
                "Workflow must be written to a file prior to invoking Workflow.graph"
                " using Workflow.write or Workflow.plan"
            )

        # check that correct label parameter is used
        labels = {"label", "xform", "id", "xform-id", "label-xform", "label-id"}
        if label not in labels:
            raise ValueError(
                "Invalid label: {}, label must be one of {}".format(label, labels)
            )

        self._client.graph(
            workflow_file=self._path,
            include_files=include_files,
            no_simplify=no_simplify,
            label=label,
            output=output,
            remove=remove,
            width=width,
            height=height,
        )

    @_chained
    def add_jobs(self, *jobs: Union[Job, SubWorkflow]):
        """
        add_jobs(self, *jobs: Union[Job, SubWorkflow])
        Add one or more jobs at a time to the Workflow

        :raises DuplicateError: a job with the same id already exists in this workflow
        :return: self
        """
        for job in jobs:
            if job._id is None:
                job._id = self._get_next_job_id()

            if job._id in self.jobs:
                raise DuplicateError(
                    "Job with id {} already added to this workflow".format(job._id)
                )

            if isinstance(job, SubWorkflow):
                self._has_subworkflow_jobs = True

            self.jobs[job._id] = job

            log.info("{workflow} added {job}".format(workflow=self.name, job=job))

    def get_job(self, _id: str):
        """Retrieve the job with the given id

        :param _id: id of the job to be retrieved from the Workflow
        :type _id: str
        :raises NotFoundError: a job with the given id does not exist in this workflow
        :return: the job with the given id
        :rtype: Job
        """
        if _id not in self.jobs:
            raise NotFoundError(
                "job with _id={} not found in this workflow".format(_id)
            )

        return self.jobs[_id]

    def _get_next_job_id(self):
        """Get the next job id from a sequence specific to this workflow

        :return: a new unique job id
        :rtype: str
        """
        next_id = None
        while not next_id or next_id in self.jobs:
            next_id = "ID{:07d}".format(self.sequence)
            self.sequence += 1

        return next_id

    @_chained
    def add_site_catalog(self, sc: SiteCatalog):
        """
        add_site_catalog(self, sc: SiteCatalog)
        Add a :py:class:`~Pegasus.api.site_catalog.SiteCatalog` to this workflow. The contents fo the site catalog
        will be inlined into the same file as this workflow when it is written
        out.

        :param sc: the :py:class:`~Pegasus.api.site_catalog.SiteCatalog` to be added
        :type sc: SiteCatalog
        :raises TypeError: sc must be of type :py:class:`~Pegasus.api.site_catalog.SiteCatalog`
        :raises DuplicateError: a :py:class:`~Pegasus.api.site_catalog.SiteCatalog` has already been added
        :return: self
        """
        if not isinstance(sc, SiteCatalog):
            raise TypeError(
                "invalid catalog: {}; sc must be of type SiteCatalog".format(sc)
            )

        if self.site_catalog is not None:
            raise DuplicateError(
                "a SiteCatalog has already been added to this workflow"
            )

        self.site_catalog = sc
        log.info("{workflow} added inline SiteCatalog".format(workflow=self.name))

    @_chained
    def add_replica_catalog(self, rc: ReplicaCatalog):
        """
        add_replica_catalog(self, rc: ReplicaCatalog)
        Add a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog` to this workflow.
        The contents fo the replica catalog will be inlined into the same file as
        this workflow when it is written.

        :param rc: the :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog` to be added
        :type rc: ReplicaCatalog
        :raises TypeError: rc must be of type :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog`
        :raises DuplicateError: a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog` has already been added
        :return: self
        """
        if not isinstance(rc, ReplicaCatalog):
            raise TypeError(
                "invalid catalog: {}; rc must be of type ReplicaCatalog".format(rc)
            )

        if self.replica_catalog is not None:
            raise DuplicateError(
                "a ReplicaCatalog has already been added to this workflow"
            )

        self.replica_catalog = rc
        log.info("{workflow} added inline ReplicaCatalog".format(workflow=self.name))

    @_chained
    def add_transformation_catalog(self, tc: TransformationCatalog):
        """
        add_transformation_catalog(self, tc: TransformationCatalog)
        Add a :py:class:`~Pegasus.api.transformation_catalog.TransformationCatalog`
        to this workflow. The contents fo the transformation catalog will be inlined
        into the same file as this workflow when it is written.

        :param tc: the :py:class:`~Pegasus.api.transformation_catalog.TransformationCatalog` to be added
        :type tc: TransformationCatalog
        :raises TypeError: tc must be of type :py:class:`~Pegasus.api.transformation_catalog.TransformationCatalog`
        :raises DuplicateError: a :py:class:`~Pegasus.api.transformation_catalog.TransformationCatalog` has already been added
        :return: self
        """
        if not isinstance(tc, TransformationCatalog):
            raise TypeError(
                "invalid catalog: {}; rc must be of type TransformationCatalog".format(
                    tc
                )
            )

        if self.transformation_catalog is not None:
            raise DuplicateError(
                "a TransformationCatalog has already been added to this workflow"
            )

        self.transformation_catalog = tc
        log.info(
            "{workflow} added inline TransformationCatalog".format(workflow=self.name)
        )

    @_chained
    def add_dependency(
        self,
        job: Union[Job, SubWorkflow],
        *,
        parents: List[Union[Job, SubWorkflow]] = [],
        children: List[Union[Job, SubWorkflow]] = []
    ):
        """
        add_dependency(self, job: Union[Job, SubWorkflow], *, parents: List[Union[Job, SubWorkflow]] = [], children: List[Union[Job, SubWorkflow]] = [])
        Add parent, child dependencies for a given job.

        .. code-block::python

            # Example 1: set parents of a given job
            wf.add_dependency(job3, parents=[job1, job2])

            # Example 2: set children of a given job
            wf.add_dependency(job1, children=[job2, job3])

            # Example 2 equivalent:
            wf.add_dependency(job1, children=[job2])
            wf.add_dependency(job1, children=[job3])

            # Example 3: set parents and children of a given job
            wf.add_dependency(job3, parents=[job1, job2], children=[job4, job5])


        :param job: the job to which parents and children will be assigned
        :type job: AbstractJob
        :param parents: jobs to be added as parents to this job, defaults to []
        :type parents: list, optional
        :param children: jobs to be added as children of this job, defaults to []
        :type children: list, optional
        :raises ValueError: the given job(s) do not have ids assigned to them
        :raises DuplicateError: a dependency between two jobs already has been added
        :return: self
        """
        # ensure that job, parents, and children are all valid and have ids
        if job._id is None:
            raise ValueError(
                "The given job does not have an id. Either assign one to it upon creation or add the job to this workflow before manually adding its dependencies."
            )

        for parent in parents:
            if parent._id is None:
                raise ValueError(
                    "One of the given parents does not have an id. Either assign one to it upon creation or add the parent job to this workflow before manually adding its dependencies."
                )

        for child in children:
            if child._id is None:
                raise ValueError(
                    "One of the given children does not have an id. Either assign one to it upon creation or add the child job to this workflow before manually adding its dependencies."
                )

        # for each parent, add job as a child
        for parent in parents:
            if parent._id not in self.dependencies:
                self.dependencies[parent._id] = _JobDependency(parent._id, {job._id})
            else:
                if job._id in self.dependencies[parent._id].children_ids:
                    raise DuplicateError(
                        "A dependency already exists between parent id: {} and job id: {}".format(
                            parent._id, job._id
                        )
                    )

                self.dependencies[parent._id].children_ids.add(job._id)

        # for each child, add job as a parent
        if len(children) > 0:
            if job._id not in self.dependencies:
                self.dependencies[job._id] = _JobDependency(job._id, set())

            for child in children:
                if child._id in self.dependencies[job._id].children_ids:
                    raise DuplicateError(
                        "A dependency already exists between job id: {} and child id: {}".format(
                            job._id, child._id
                        )
                    )
                else:
                    self.dependencies[job._id].children_ids.add(child._id)

    def _infer_dependencies(self):
        """Internal function for automatically computing dependencies based on
        Job input and output files. This is called when Workflow.infer_dependencies is
        set to True.
        """

        if self.infer_dependencies:
            log.info("inferring {workflow} dependencies".format(workflow=self.name))
            mapping = OrderedDict()

            """
            create a mapping:
            {
                <filename>: (set(), set())
            }

            where mapping[filename][0] are jobs that use this file as input
            and mapping[filename][1] are jobs that use this file as output
            """
            for _id, job in self.jobs.items():
                if job.stdin:
                    if job.stdin.lfn not in mapping:
                        mapping[job.stdin.lfn] = (set(), set())

                    mapping[job.stdin.lfn][0].add(job)

                if job.stdout:
                    if job.stdout.lfn not in mapping:
                        mapping[job.stdout.lfn] = (set(), set())

                    mapping[job.stdout.lfn][1].add(job)

                if job.stderr:
                    if job.stderr.lfn not in mapping:
                        mapping[job.stderr.lfn] = (set(), set())

                    mapping[job.stderr.lfn][1].add(job)

                """
                for _input in job.inputs:
                    if _input.file.lfn not in mapping:
                        mapping[_input.file.lfn] = (set(), set())

                    mapping[_input.file.lfn][0].add(job)

                for output in job.outputs:
                    if output.file.lfn not in mapping:
                        mapping[output.file.lfn] = (set(), set())

                    mapping[output.file.lfn][1].add(job)
                """
                for io in job.uses:
                    if io.file.lfn not in mapping:
                        mapping[io.file.lfn] = (set(), set())

                    if io._type == _LinkType.INPUT.value:
                        mapping[io.file.lfn][0].add(job)
                    elif io._type == _LinkType.OUTPUT.value:
                        mapping[io.file.lfn][1].add(job)

            """
            Go through the mapping and for each file add dependencies between the
            job producing a file and the jobs consuming the file
            """
            for _, io in mapping.items():
                inputs = io[0]

                if len(io[1]) > 0:
                    # only a single job should produce this file
                    output = io[1].pop()

                    for _input in inputs:
                        try:
                            self.add_dependency(output, children=[_input])
                        except DuplicateError:
                            pass

    @_chained
    def write(self, file: Optional[Union[str, TextIO]] = None, _format: str = "yml"):
        """
        write(self, file: Optional[Union[str, TextIO]] = None, _format: str = "yml")
        Write this workflow to a file. If no file is given,
        it will written to workflow.yml

        :param file: path or file object (opened in "w" mode) to write to, defaults to None
        :type file: Optional[Union[str, TextIO]]
        :param _format: serialized format of the workflow object (this should be left as its default)
        :type _format: str, optional
        :return: self
        """
        # default file name
        if file is None:
            file = self._DEFAULT_FILENAME

        tracker=PegasusTracker(local_storage_dir=Path.cwd(),wf_dir=Path.cwd(),wf=self,rc=self.replica_catalog,tc=self.transformation_catalog,dagfile=file)
        
        if self.tracker_type =="full":
            tracker.full_tracker()
        elif self.tracker_type == "inputs":
            tracker.track_input()
        elif self.tracker_type == "outputs":
            tracker.track_output()
        elif self.tracker_type == "auto":
            tracker.auto_logger()
        else:
            print("No tracking option choosen using the full tracking")
            tracker.full_tracker()
        # ensure user is aware that SiteCatalog and TransformationCatalog are
        # not inherited by SubWorkflows when those two catalogs are embedded
        # into the root workflow
        if (
            self.site_catalog is not None or self.transformation_catalog is not None
        ) and self._has_subworkflow_jobs:
            log.warning(
                "SiteCatalog and TransformationCatalog objects embedded into the root Workflow are not inherited by SubWorkflow jobs. To set SiteCatalog and TransformationCatalog objects in SubWorkflows, ensure that they are embedded into those SubWorkflows."
            )

        

        self._infer_dependencies()

        # serialize Workflow objects added as SubWorkflows
        for _id, job in self.jobs.items():
            if isinstance(job, SubWorkflow) and isinstance(job.file, Workflow):
                # serialize job (Workflow instance) to be <wf-name>_<id>.yml
                workflow_file = Path.cwd() / "{}_{}.yml".format(job.file.name, job._id)
                job.file.write(str(workflow_file))

                # update job.file to be the file name just created
                job.file = workflow_file.name

                # add job.file as an input to the SubWorkflow job
                job.add_inputs(job.file)

                # add to inline replica catalog (create one if none exists)
                rc_kwargs = {
                    "site": "local",
                    "lfn": workflow_file.name,
                    "pfn": workflow_file,
                }
                if not self.replica_catalog:
                    self.replica_catalog = ReplicaCatalog()

                self.replica_catalog.add_replica(**rc_kwargs)

        Writable.write(self, file, _format=_format)

        log.info(
            "workflow {workflow} with {num_jobs} jobs generated and written to {dst}".format(
                workflow=self.name, num_jobs=len(self.jobs), dst=file
            )
        )

        
        # save path so that it can be used by Client.plan()
        if isinstance(file, str):
            self._path = file
        elif hasattr(file, "read"):
            self._path = file.name if hasattr(file, "name") else None

    def __json__(self):
        # remove 'pegasus' from tc, rc, sc as it is not needed when they
        # are included in the Workflow which already contains 'pegasus'
        rc = None
        if self.replica_catalog is not None:
            rc = json.loads(json.dumps(self.replica_catalog, cls=_CustomEncoder))
            del rc["pegasus"]

        tc = None
        if self.transformation_catalog is not None:
            tc = json.loads(json.dumps(self.transformation_catalog, cls=_CustomEncoder))
            del tc["pegasus"]

        sc = None
        if self.site_catalog is not None:
            sc = json.loads(json.dumps(self.site_catalog, cls=_CustomEncoder))
            del sc["pegasus"]

        hooks = None
        if len(self.hooks) > 0:
            hooks = OrderedDict(
                [
                    (hook_name, [hook for hook in values])
                    for hook_name, values in self.hooks.items()
                ]
            )

        profiles = None
        if len(self.profiles) > 0:
            profiles = OrderedDict(sorted(self.profiles.items(), key=lambda _: _[0]))

        metadata = None
        if len(self.metadata) > 0:
            metadata = self.metadata

        jobs = [job for _, job in self.jobs.items()]
        job_dependencies = [dependency for _id, dependency in self.dependencies.items()]

        return _filter_out_nones(
            OrderedDict(
                [
                    ("pegasus", PEGASUS_VERSION),
                    ("name", self.name),
                    ("hooks", hooks),
                    ("profiles", profiles),
                    ("metadata", metadata),
                    ("siteCatalog", sc),
                    ("replicaCatalog", rc),
                    ("transformationCatalog", tc),
                    ("jobs", jobs),
                    ("jobDependencies", job_dependencies),
                ]
            )
        )
