#!/usr/bin/env python3

from argparse import ArgumentParser
import base64
import configparser
from datetime import datetime
import glob
import hashlib 
import os
from pathlib import Path
import sys
import yaml
import argparse
import requests
import json

import argparse


DEFAULT_CONFIG_DIR = os.path.expanduser("~/.pegasus/")
DEFAULT_DB_PATH = os.path.join(DEFAULT_CONFIG_DIR, "workflow.db")
METADATA_FILE=  os.path.join(DEFAULT_CONFIG_DIR, "metadata.yaml")
CURRENT_DIR=str(Path(__file__).parent.resolve())
DEFAULT_CONFIG_FILE=DEFAULT_CONFIG_DIR+"pegasus_data_config.conf"

class GitHubFilePusher:
    def __init__(self, token, repo_owner, repo_name, branch_name, local_file_path, commit_message=None):
        self.token = token
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        self.branch_name = branch_name
        self.local_file_path = local_file_path
        self.commit_message = commit_message

    def create_branch(self, base_branch='main'):
        # Replace hyphens with underscores in the branch name
        self.branch_name = self.branch_name.replace('-', '_')

        # Base URL for the GitHub API
        api_base_url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}"

        # Authenticate using your personal access token
        headers = {
            "Authorization": f"token {self.token}",
        }

        # Get the SHA of the base branch
        base_branch_url = f"{api_base_url}/git/refs/heads/{base_branch}"
        base_branch_response = requests.get(base_branch_url, headers=headers)
        if base_branch_response.status_code != 200:
            print(f"Failed to get the SHA of base branch '{base_branch}': {base_branch_response.text}")
            return

        base_branch_data = base_branch_response.json()
        if 'object' not in base_branch_data or 'sha' not in base_branch_data['object']:
            print(f"Failed to get the SHA of base branch '{base_branch}': SHA not found in response.")
            return

        base_branch_sha = base_branch_data['object']['sha']

        # Check if the branch already exists
        branch_exists_url = f"{api_base_url}/branches/{self.branch_name}"
        branch_response = requests.get(branch_exists_url, headers=headers)

        if branch_response.status_code == 404:
            # Branch doesn't exist, create it
            create_branch_url = f"{api_base_url}/git/refs"
            data = {
                "ref": f"refs/heads/{self.branch_name}",
                "sha": base_branch_sha
            }
            response = requests.post(create_branch_url, json=data, headers=headers)
            if response.status_code == 201:
                print(f"Branch '{self.branch_name}' created successfully.")
            else:
                print(f"Failed to create branch '{self.branch_name}': {response.text}")
        else:
            print(f"Branch '{self.branch_name}' already exists.")

    def push_file(self):
        commit_sha=None
        # Base URL for the GitHub API
        api_base_url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}"

        # Authenticate using your personal access token
        headers = {
            "Authorization": f"token {self.token}",
        }
        
        self.create_branch()
        # Read the local file content
        with open(self.local_file_path, 'rb') as file:
            file_content = file.read()
            file_content_base64 = base64.b64encode(file_content).decode('utf-8')

        # Prepare the file path for the API request
        file_path = self.local_file_path

        # Check if the file exists in the repository
        check_file_url = f"{api_base_url}/contents/{file_path}?ref={self.branch_name}"
        response = requests.get(check_file_url, headers=headers)

        if response.status_code == 200:
            # File already exists, update it
            file_sha = response.json()["sha"]
            update_file_url = f"{api_base_url}/contents/{file_path}"
            data = {
                "message": self.commit_message if self.commit_message else "Update file via API",
                "content": file_content_base64,
                "sha": file_sha,
                "branch": self.branch_name,
            }
            response = requests.put(update_file_url, json=data, headers=headers)
            if response.status_code == 200:
                print(f"File '{file_path}' updated successfully.")
                commit_info = response.json().get("commit", {})
                commit_sha = commit_info.get("sha", "Unknown")
            else:
                print(f"Failed to update file '{file_path}': {response.text}")
        elif response.status_code == 404:
            # File doesn't exist, create it
            create_file_url = f"{api_base_url}/contents/{file_path}"
            data = {
                "message": self.commit_message if self.commit_message else "Create file via API",
                "content": file_content_base64,
                "branch": self.branch_name,
            }
            response = requests.put(create_file_url, json=data, headers=headers)
            if response.status_code == 201:
                print(f"File '{file_path}' created successfully.")
                 # Extract information about the commit
                commit_info = response.json().get("commit", {})
                commit_sha = commit_info.get("sha", "Unknown")
                commit_message = commit_info.get("message", "No message")
            else:
                print(f"Failed to create file '{file_path}': {response.text}")
        else:
            print(f"Failed to check file '{file_path}': {response.text}")
        
        return commit_sha


class TrasnformationVersioning:

    def __init__(self,use_git=False,token=None,owner=None,repo=None,branch=None,pull=False,metadata_file="pegasus-data/metadata_transformations.yaml") -> None:
        config = configparser.ConfigParser()
        self.git_config_dict=None
        self.use_git=use_git
        self.token=token
        self.owner=owner
        self.repo=repo
        self.branch=branch
        self.pull=pull
        self.metadata_file=metadata_file
        self.base_directory = "pegasus-data"
        self.metadata_format="yaml"
    def create_empty_metadata_file(self):
        if not os.path.exists(self.metadata_file):
            metadata = {"versions": [], "files": {}}
            self.save_metadata(metadata)

    def load_metadata(self):
        with open(self.metadata_file, "r") as f:
            if self.metadata_format == "json":
                return json.load(f)
            elif self.metadata_format == "yaml":
                return yaml.safe_load(f)
            else:
                print("Unsupported metadata format. Please use 'json' or 'yaml'.")
                return {}
            
    def save_metadata(self, metadata):
        with open(self.metadata_file, "w") as f:
            if self.metadata_format == "json":
                json.dump(metadata, f)
            elif self.metadata_format == "yaml":
                yaml.safe_dump(metadata, f)
            else:
                print("Unsupported metadata format. Please use 'json' or 'yaml'.")

    def calculate_sha256(self, file_path):
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def create_directories(self):
        if not os.path.exists(self.base_directory):
            os.makedirs(self.base_directory)

    def Version(self,Files,names,paths):
        self.create_directories()
        self.create_empty_metadata_file()
        metadata = self.load_metadata()
        File_tracking_version=[]
        metadata_file=[]
        transformation_commit_sha=""
        for file_id,file in enumerate(Files):
            try:
                #if self.use_git:
                pusher=GitHubFilePusher(repo_owner=self.owner,token=self.token,local_file_path=names[file_id],repo_name=self.repo,branch_name=os.getenv('PEGASUS_WF_LABEL'), commit_message= f"Last transformations file name {names[file_id]} used in workflow run ID : {os.getenv('PEGASUS_WF_UUID')} of workflow name :  {os.getenv('PEGASUS_WF_LABEL')}",)
                transformation_commit_sha=pusher.push_file()
            except Exception as e:
                    transformation_commit_sha="push Failed"
                    print(e)
            transformations_dict = {
                        os.getenv("PEGASUS_WF_LABEL"): {
                            os.getenv('PEGASUS_WF_UUID'): {
                                        "version": self.calculate_sha256(names[file_id]),
                                        "path": paths[file_id],
                                        "timestamp": str(datetime.now()),
                                        "git_url": "",
                                        "size": os.path.getsize(names[file_id]),
                                        "last_modification": os.path.getmtime(names[file_id]),
                                        "code_commit":transformation_commit_sha,
                                        "git_url":f"https://github.com/{self.owner}/{self.repo}/blob/{transformation_commit_sha}/{file.split('/')[-1]}",
                                        "type":"Transormations"
                                    }
                                }
                            }
            File_tracking_version.append(transformations_dict)

        metadata.setdefault("versions", []).extend(File_tracking_version)

        self.save_metadata(metadata)
     
if __name__ == '__main__':
    parser = ArgumentParser(description="Pegasus Federated Learning Workflow Example")

    parser.add_argument("-files", default=None, type=str, nargs='+' , help="Files replicas")
    parser.add_argument("-names", default=None, type=str, nargs='+' , help="Files Name")
    parser.add_argument("-path", default=None, type=str, nargs='+' , help="Files physical path")
    parser.add_argument("-metadata-file",default="pegasus-data/metadata_transformations.yaml",type=str,help="Metadata File name")

    # git configuration
    parser.add_argument("--use-git", action="store_false", help="Use git repo configuration")
    parser.add_argument("-token", default="" , help="Your GitHub personal access token")
    parser.add_argument("-owner", default="" , help="Repository owner's username")
    parser.add_argument("-repo", default="" , help="Repository name")
    parser.add_argument("-branch", default="" , help="Branch name")
    args = parser.parse_args()
        
    Transformations_tracker= TrasnformationVersioning(args.use_git,args.token, args.owner,args.repo, args.branch,args.metadata_file)
    Transformations_tracker.Version(args.files,args.names,args.path)




class TrasnformationVersioning():

    def __init__(self) -> None:
        pass