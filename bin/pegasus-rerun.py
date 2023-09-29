#!/usr/bin/env python3

from argparse import ArgumentParser
import configparser
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from tqdm import tqdm
import yaml
import collections , yaml
import os
import io
import pickle
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from Pegasus.api import *

_mapping_tag = yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG

# Define the scope and the path to the credentials file
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

DEFAULT_CONFIG_DIR = os.path.expanduser("~/.pegasus/")
DEFAULT_CONFIG_FILE = os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf")
METADATA_FILE=  os.path.join(DEFAULT_CONFIG_DIR, "metadata.yaml")
CURRENT_DIR=str(Path(__file__).parent.resolve())

config = configparser.ConfigParser()
try:
    config.read(DEFAULT_CONFIG_FILE)
except:
    print("No configuration file found please use pegasus-data to add necessairy configuration if you want data tracking")
    sys.exit(0)

def copy_large_file(source_path, destination_path, chunk_size=1024*1024):
    try:
        with open(source_path, 'rb') as source_file, open(destination_path, 'wb') as destination_file:
            while True:
                chunk = source_file.read(chunk_size)
                if not chunk:
                    break
                destination_file.write(chunk) #\033[32m✓ Jobs Sites updated \033[0m
        print("\033[32m✓File copied successfully\033[0m")
    except FileNotFoundError:
        print("Source file not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

def delete_jobs_with_id_starting_with(data, prefixes):
    jobs_data = data.get("jobs", [])
    data["jobs"] = [job for job in jobs_data if not any(job.get("id", "").startswith(prefix) for prefix in prefixes)]

    dependencies_data = data.get("jobDependencies", [])
    updated_dependencies = []
    for dep in dependencies_data:
        if not any(dep["id"].startswith(prefix) for prefix in prefixes):
            updated_children = [child for child in dep.get("children", []) if not any(child.startswith(prefix) for prefix in prefixes)]
            dep["children"] = updated_children
            updated_dependencies.append(dep)
    data["jobDependencies"] = updated_dependencies

    return data

def dict_representer(dumper, data):
  return dumper.represent_mapping(_mapping_tag, data.iteritems())

def dict_constructor(loader, node):
  return collections.OrderedDict(loader.construct_pairs(node))

def is_local_path(path):
    return os.path.isabs(path) and os.path.exists(path)

def get_relative_path(parent_dir, absolute_path):
    relative_path = os.path.relpath(absolute_path, start=parent_dir)
    parts = relative_path.split(os.path.sep)
    if len(parts) > 2:
            return os.path.join(parts[-2], parts[-1])
    return relative_path

def delete_entries_from_replica_catalog(data):
    replicas_data = data.get("replicaCatalog", {}).get("replicas", [])
    lfns_to_delete = ["cred", "bucket", "metadata", "wf", "data_tracker", "combiner"]
    data["replicaCatalog"]["replicas"] = [entry for entry in replicas_data if entry.get("lfn") not in lfns_to_delete]
    return data

def delete_entries_from_transformation_catalog(data):
    transformations_data = data.get("transformationCatalog", {}).get("transformations", [])
    names_to_delete = ["cred", "data_tracker", "combiner" ,"cp"]
    data["transformationCatalog"]["transformations"] = [entry for entry in transformations_data if entry.get("name") not in names_to_delete]
    return data

def update_replica(data, parent_directory,CURRENT_DIR=None):
    dir_mapper={}
    replicas_data = data.get("replicaCatalog", {}).get("replicas", [])
    for replica_entry in replicas_data:
        lfn=replica_entry.get("lfn", [])
        pfns = replica_entry.get("pfns", [])
        if pfns:
            pfn = pfns[0].get("pfn", "")
            absolute_path = os.path.join(parent_directory, pfn.lstrip("/"))
            relative_path = get_relative_path(parent_directory, absolute_path)
            replica_entry["pfns"][0]["pfn"] = os.path.join(CURRENT_DIR, relative_path)
            dir_mapper[lfn]= {relative_path.split('/')[-1]:relative_path}
            create_directory(CURRENT_DIR,relative_path.split('/')[-2])
    return data,dir_mapper

def update_transformation(data, parent_directory,local=True,remote=False,CURRENT_DIR=None):
    transformations_data = data.get("transformationCatalog", {}).get("transformations", [])
    for transformation_entry in transformations_data:
        sites = transformation_entry.get("sites", [])
        if sites:
            pfn = sites[0].get("pfn", "")
            absolute_path = os.path.join(parent_directory, pfn.lstrip("/"))
            relative_path = get_relative_path(parent_directory, absolute_path)
            transformation_entry["sites"][0]["pfn"] = os.path.join(CURRENT_DIR, relative_path) 
            create_directory(CURRENT_DIR,relative_path.split('/')[-2])
            print(f"\033[32m - Try to copy transformations file \033[0m")
            if local:
                    copy_file(pfn,transformation_entry["sites"][0]["pfn"])
            else: 
                print("remote transformation not yet supported")
    return data

def update_container(data, parent_directory,CURRENT_DIR=None):
    containers_data = data.get("transformationCatalog", {}).get("containers", [])
    if containers_data :
        for container in range(len(containers_data)):
            if containers_data[container].get("image.site")=="local":
                container_path=containers_data[container].get("image","")
                container_path_relative=get_relative_path(parent_directory, container_path)
                containers_data[container]['image']=os.path.join(CURRENT_DIR, container_path_relative.split('/')[-2], container_path_relative.split('/')[-1])
                create_directory(CURRENT_DIR, container_path_relative.split('/')[-2])
                print(f"\033[32m - Try to copy container file \033[0m")
                copy_large_file(container_path,os.path.join(CURRENT_DIR, container_path_relative.split('/')[-2], container_path_relative.split('/')[-1]))
    return data

def update_sites(data, parent_directory,CURRENT_DIR=None):
    sites_data = data.get("siteCatalog", {}).get("sites", [])
    for site in sites_data:
        if site["name"]=="local":
            for dir in site["directories"]:
                abs_path=dir["path"]
                relative_path=get_relative_path(parent_directory,abs_path)
                dir["path"]= os.path.join(CURRENT_DIR, relative_path.split('/')[-1])       
                dir["fileServers"][0]["url"]="file://"+os.path.join(CURRENT_DIR, relative_path.split('/')[-1])
    return data     
                 
def get_wf(section=None,parent_directory=None,CURRENT_DIR=None):
    # Iterate through the items in the main dictionary
    for filename, file_data_list in section.items():
        # Iterate through the list of dictionaries for each filename
        for file_data in file_data_list:
            # Check if the 'type' key has a value of 'workflow'
            if file_data.get('type') == 'workflow':
                full_path = os.path.join(CURRENT_DIR, get_relative_path(parent_directory,file_data.get('path').split('/')[-1]))
                return {filename:{filename:full_path}},full_path
            

def update_absolute_paths_to_relative(data, parent_directory):
    dir_mapper={}
    transformation_local=True
    replicas_data = data.get("replicaCatalog", {}).get("replicas", [])
    transformations_data = data.get("transformationCatalog", {}).get("transformations", [])
    containers_data = data.get("transformationCatalog", {}).get("containers", [])
    sites_data = data.get("siteCatalog", {}).get("sites", [])
    jobs_data = data.get("jobs", [])

    for replica_entry in replicas_data:
        lfn=replica_entry.get("lfn", [])
        pfns = replica_entry.get("pfns", [])
        if pfns:
            pfn = pfns[0].get("pfn", "")
            absolute_path = os.path.join(parent_directory, pfn.lstrip("/"))
            relative_path = get_relative_path(parent_directory, absolute_path)
            replica_entry["pfns"][0]["pfn"] = os.path.join(CURRENT_DIR, relative_path)
            dir_mapper[lfn]= {relative_path.split('/')[-1]:relative_path}
            create_directory(CURRENT_DIR,relative_path.split('/')[-2])
                
    for transformation_entry in transformations_data:
        sites = transformation_entry.get("sites", [])
        if sites:
            pfn = sites[0].get("pfn", "")
            absolute_path = os.path.join(parent_directory, pfn.lstrip("/"))
            relative_path = get_relative_path(parent_directory, absolute_path)
            transformation_entry["sites"][0]["pfn"] = os.path.join(CURRENT_DIR, relative_path) 
            create_directory(CURRENT_DIR,relative_path.split('/')[-2])
            if transformation_local:
                    copy_file(pfn,relative_path)

    for site in sites_data:
        if site["name"]=="local":
            for dir in site["directories"]:
                abs_path=dir["path"]
                relative_path=get_relative_path(parent_directory,abs_path)
                dir["path"]= os.path.join(CURRENT_DIR, relative_path.split('/')[-1])       
                dir["fileServers"][0]["url"]="file://"+os.path.join(CURRENT_DIR, relative_path.split('/')[-1])       
                             
    if containers_data :
        for container in range(len(containers_data)):
            if containers_data[container].get("image.site")=="local":
                container_path=containers_data[container].get("image","")
                container_path_relative=get_relative_path(parent_directory, container_path)
                containers_data[container]['image']=os.path.join(CURRENT_DIR, container_path_relative.split('/')[-2], container_path_relative.split('/')[-1])
                create_directory(CURRENT_DIR, container_path_relative.split('/')[-2])
                copy_large_file(container_path,os.path.join(CURRENT_DIR, container_path_relative.split('/')[-2], container_path_relative.split('/')[-1]))
               
    return data,dir_mapper
   
def find_key_in_nested_dict(dictionary, target_key):
        for key, value in dictionary.items():
            if key == target_key:
                return value
            elif isinstance(value, dict):
                result = find_key_in_nested_dict(value, target_key)
                if result is not None:
                    return result
        return None

def create_directory(parent_dir, directory_name):
    try:
        full_path = os.path.join(parent_dir, directory_name)
        os.makedirs(full_path)
        print(f"\033[32m✓ Directory created: {full_path} \033[0m")
    except OSError as e:
        print(f"Error: {e}")
        
def copy_file(source_path, destination_path):
    try:
        print(f"start creating a copy of {source_path} in {destination_path} ")
        shutil.copy(source_path, destination_path)
        print(f"\033[32m✓ File '{source_path}' copied to '{destination_path}' \033[0m")
    except FileNotFoundError:
        print(f"\033[31m✗Source file '{source_path}' not found. \033[0m")
    except PermissionError:
         print(f"\033[31m✗ Permission error. Make sure the program has read access to the source file and write access to the destination directory. \033[0m")
                
def authenticate_google_drive_api(credentials_path):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=SCOPES
    )
    return credentials

def download_file_from_google_drive(file_id, save_path, credentials):
    drive_service = build("drive", "v3", credentials=credentials)

    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(save_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%")

def gdrive_files(files=None, section=None, CREDENTIALS_FILE=None,CURRENT_DIR=None):
    for dir,dir_path in files.items():

        # Define the file ID and the local path to save the downloaded file

        file_id = section[list(dir_path.keys())[0]][0]['gdrive_remote_storage_url']
        local_save_path = dir

        # Authenticate Google Drive API
        credentials = authenticate_google_drive_api(CREDENTIALS_FILE)
        

        # Download the file
        #download_file_from_google_drive(file_id, local_save_path,credentials)
        # Authenticate Google Drive API using service account credentials
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )

        # Build the Google Drive API service
        drive_service = build("drive", "v3", credentials=credentials)

        # Define the folder ID of the Google Drive folder you want to download files from
        folder_id = section[list(dir_path.keys())[0]][0]['gdrive_remote_storage_url']
        # Retrieve the list of files in the folder
        results = drive_service.files().list(q=f"'{folder_id}' in parents", fields="files(id, name)" ).execute()

        # Loop through the files and download each one
        for file in results.get("files", []):
            file_id = file["id"]
            file_name = file["name"]
            save_path = f"{os.path.join(CURRENT_DIR,list(dir_path.values())[0])}"

            # Use the get_media method to download binary content files
            request = drive_service.files().get_media(fileId=file_id)

            fh = io.FileIO(save_path, "wb")
            downloader = MediaIoBaseDownload(fh, request)

            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"Download {int(status.progress() * 100)}%: {file_name}")

def bucket_files(bucket_files,section,bucket_cridentials_file=None,CURRENT_DIR=None):
        transfer_list = []
        for dir,dir_path in bucket_files.items():

            file_id = section[list(dir_path.keys())[0]][0]['bucket_storage_url']
            file_path = os.path.join(CURRENT_DIR,list(dir_path.values())[0])
            dest_urls = [{"site_label": "local", "url": f"file://{file_path}"}]
            src_urls = [{"site_label": "osn", "url": f"s3://{file_id}"}]
            transfer_dict = {
                        "type": "transfer",
                        "src_urls": src_urls,
                        "dest_urls": dest_urls
                    }
                    
            transfer_list.append(transfer_dict)
        output_file_path = 'output_data.in'
        with open(output_file_path, 'w') as output_file:
            json.dump(transfer_list, output_file, indent=4)

        pegasus_transfer_command = ['pegasus-transfer', '-f', output_file_path]

        try:
            # Run the pegasus-transfer command
            subprocess.run(pegasus_transfer_command, check=True)
            print("Pegasus transfer completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"\033[31m✗ Error occurred while running pegasus-transfer: \033[0m")
            print(f" {e}")
        return transfer_list

def run_workflow(workflow_path=None):
    try:
        # Run the "pegasus-plan" command to plan the workflow
        subprocess.run(["pegasus-plan", "-S", "-s","condorpool","-o","local",workflow_path])
        pass 
        # Run the "pegasus-run" command to execute the workflow
        #subprocess.run(["pegasus-run", "workdir"], check=True)
    except subprocess.CalledProcessError as e:
        print("Error:", e)

def workflow(workflow_UUID=None,Metadata=METADATA_FILE,bucket=False,gdrive=False,gdrive_credentials=None,bucket_credentials=None,CURRENT_DIR=None):
    yaml.add_representer( collections.OrderedDict , dict_representer )
    yaml.add_constructor( _mapping_tag, dict_constructor )	
    create_directory(directory_name=workflow_UUID,parent_dir=CURRENT_DIR)
    CURRENT_DIR=os.path.join(CURRENT_DIR, workflow_UUID)
    # File path of the YAML file
    # Load YAML data from the file
    
    with open(METADATA_FILE, "r") as file:
        workflow_data = yaml.safe_load(file)
    try:
        section=find_key_in_nested_dict(workflow_data,workflow_UUID)
        workflow,full_path=get_wf(section=section,CURRENT_DIR=CURRENT_DIR)
    except:
        print(f"\033[31m✗ No workflow with {workflow_UUID} was found in metadata file \033[0m")
        return
    if bucket:
        section=find_key_in_nested_dict(workflow_data,workflow_UUID)
        try:
            bucket_credentials=config.get("Bucket","credentials_file")
        except:
            print(f"\033[31m✗ No bucket configured \033[0m")
        bucket_files(workflow,section,bucket_cridentials_file=bucket_credentials,CURRENT_DIR=CURRENT_DIR)
    elif gdrive:
        section=find_key_in_nested_dict(workflow_data,workflow_UUID)
        try:
            credentials_file=config.get("GoogleDrive","gdrive_credentials_file")
        except:
            print(f"\033[31m✗ No google drive service configured \033[0m")
        gdrive_files(files=workflow,section=section,CREDENTIALS_FILE=credentials_file,CURRENT_DIR=CURRENT_DIR)
    else : 
        print("Storage no provided")
        return 
    
    file_path = full_path
    
    # Set the desired parent directory for relative paths
    # The parent directory should be the directory where the YAML file is located
    parent_directory = os.path.dirname(os.path.abspath(file_path))

    # Load YAML data from the file
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)
    # Create a copy of the original data
    original_data = yaml.safe_load(yaml.dump(data))
    try:
        data = delete_jobs_with_id_starting_with(data, ["tracker_data", "tracker_wf","metadata_combiner","copy_file"])
        print(f"\033[32m✓ Jobs section prepered \033[0m")
    except Exception as e:
            print(f"\033[31m✗  FAILED to prepered jobs  \033[0m")
            print(f"An error occurred: {e}")

    try:
        data = delete_entries_from_replica_catalog(data)
        print(f"\033[32m✓ Jobs replicas prepered \033[0m")
    except Exception as e:
            print(f"\033[31m✗  FAILED to prepered replicas  \033[0m")
            print(f"An error occurred: {e}")
    
    try:
        data = delete_entries_from_transformation_catalog(data)
        print(f"\033[32m✓ Jobs transformation prepered \033[0m")
    except Exception as e:
        print(f"\033[31m✗  FAILED to prepered transformation  \033[0m") 
        print(f"An error occurred: {e}")


    try:
        updated_data,mapper=update_replica(data,parent_directory=CURRENT_DIR,CURRENT_DIR=CURRENT_DIR)
        print(f"\033[32m✓  replicas updated \033[0m")
    except Exception as e:
        print(f"\033[31m✗  FAILED to update replicas  \033[0m")
        print(f"An error occurred: {e}")

    try:
        updated_data=update_transformation(updated_data,parent_directory=CURRENT_DIR,CURRENT_DIR=CURRENT_DIR,local=True)
        print(f"\033[32m✓  transformation updated \033[0m")
    except Exception as e:
        print(f"\033[31m✗  FAILED to update transformation  \033[0m")
        print(f"An error occurred: {e}")
    try:
        updated_data=update_sites(updated_data,parent_directory=CURRENT_DIR,CURRENT_DIR=CURRENT_DIR)
        print(f"\033[32m✓  Sites updated \033[0m")
    except Exception as e:
        print(f"\033[31m✗  FAILED to update Sites  \033[0m")
        print(f"An error occurred: {e}")

    try:
        updated_data=update_container(updated_data,parent_directory=CURRENT_DIR,CURRENT_DIR=CURRENT_DIR)
        print(f"\033[32m✓ Jobs container updated \033[0m")
    except Exception as e:
        print(f"\033[31m✗  FAILED to update container  \033[0m")
        print(f"An error occurred: {e}")
    
    # Save the updated YAML data to a new file
    output_file_path = CURRENT_DIR+"/workflow_updated.yml"
    
    with open(output_file_path, "w") as file:
        yaml.dump(updated_data, file,default_flow_style=False, sort_keys=False)

    if bucket:
        bucket_credentials=config.get("Bucket","credentials_file")
        bucket_files(mapper,section,bucket_cridentials_file=bucket_credentials,CURRENT_DIR=CURRENT_DIR)
    elif gdrive:
        credentials_file=config.get("GoogleDrive","gdrive_credentials_file")
        gdrive_files(files=mapper,section=section,CREDENTIALS_FILE=credentials_file,CURRENT_DIR=CURRENT_DIR)
    else : 
        print("Storage no provided")
        return  

    return output_file_path


if __name__ == "__main__":
    CURRENT_DIR=str(Path(__file__).parent.resolve())
    
    

    #CREDENTIALS_FILE = "/home/poseidon/workflows/FL-workflow/federated-learning-fedstack-PM/Versionning/cred3.json".

    parser = ArgumentParser(description="Pegasus Federated Learning Workflow Example")
    parser.add_argument('-uuid', type=str , help='workflow uuid in metadata file')
    parser.add_argument('-metadata', type=str , help='metadata file path')
    parser.add_argument("-gcredentials", default=None, type=str , help="Files credentials")
    parser.add_argument("-gdrive", default=False, action="store_true", help="Use google drive")
    parser.add_argument("-bucket", default=False, action="store_true", help="Use bucket or s3")
    parser.add_argument("-run", default=False, action="store_true", help="Use bucket or s3")
    parser.add_argument("-bcredentials", default=None, type=str , help="Files credentials")
    parser.add_argument("-plan", default=False, action="store_true", help="plan the workflow")

    args = parser.parse_args()
    
    if not any(vars(args).values()):
        parser.print_help()
        sys.exit()

    rerun_dir="rerun"
    if not os.path.exists(rerun_dir):
        os.makedirs(rerun_dir)
        print(f"Created directory: {rerun_dir}")
    else:
        print(f"Directory '{rerun_dir}' already exists.")

    os.chdir(rerun_dir)

    wf_path=workflow(args.uuid, args.metadata,args.bucket,args.gdrive,args.gcredentials,args.bcredentials,os.getcwd())

    if args.plan:
        run_workflow(wf_path)

    
        

    


   
   
            

