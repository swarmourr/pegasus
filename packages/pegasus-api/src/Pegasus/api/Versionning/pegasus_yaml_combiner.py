#!/usr/bin/env python3

from argparse import ArgumentParser
import base64
import configparser
import glob 
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

    def push_file(self):
        # Base URL for the GitHub API
        api_base_url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}"

        # Authenticate using your personal access token
        headers = {
            "Authorization": f"token {self.token}",
        }

        # Read the local file content
        with open(self.local_file_path, 'rb') as file:
            file_content = file.read()
            file_content_base64 = base64.b64encode(file_content).decode('utf-8')

        # Prepare the file path for the API request
        file_path = self.local_file_path

        # Check if the file exists in the repository
        check_file_url = f"{api_base_url}/contents/{file_path}"
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
            else:
                print(f"Failed to create file '{file_path}': {response.text}")
        else:
            print(f"Failed to check file '{file_path}': {response.text}")

    def pull_file(self):
        # Base URL for the GitHub API
        api_base_url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/contents/"

        # Authenticate using your personal access token
        headers = {
            "Authorization": f"token {self.token}",
        }
        file_path = self.local_file_path
        # Prepare the file path for the API request
        file_url = f"{api_base_url}{file_path}?ref={self.branch_name}"

        # Make a GET request to retrieve the file content
        response = requests.get(file_url, headers=headers)

        if response.status_code == 200:
            # Decode the content from base64
            content_base64 = response.json()["content"]
            file_content = base64.b64decode(content_base64).decode("utf-8")
            return file_content
        else:
            print(f"Failed to pull file '{file_path}': {response.text}")
            return None
    

class metadata_class:

    def __init__(self,use_git=False,token=None,owner=None,repo=None,branch=None,pull=False) -> None:
        config = configparser.ConfigParser()
        self.git_config_dict=None
        self.use_git=use_git
        self.token=token
        self.owner=owner
        self.repo=repo
        self.branch=branch
        self.pull=pull
        
    def combine_files(self,output_file_path,file_paths,type):
        for k, v in sorted(os.environ.items()):
            print(k+':', v)

        if self.pull:
            print("i am pulling the code")
            pusher=GitHubFilePusher(repo_owner=self.owner,token=self.token,branch_name=self.branch,local_file_path=output_file_path+"."+type,repo_name=self.repo,commit_message= f"The metadata file updated by workflow ID : {os.getenv('PEGASUS_WF_UUID')} of workflow name :  {os.getenv('PEGASUS_WF_LABEL')}")
            combined_data = yaml.safe_load(pusher.pull_file())
        else:
            try:
                # Load existing YAML data (if available)
                with open(output_file_path, 'r') as output_file:
                    combined_data = yaml.safe_load(output_file)
            except FileNotFoundError:
                combined_data = {}
                
        self.create_directories("pegasus-data")
        for file_path in file_paths:
            with open(file_path, 'r') as file:
                yaml_data = yaml.safe_load(file)

                # Check if the file has 'versions' field
                if 'versions' in yaml_data:
                    versions_data = yaml_data['versions']
                    for version in versions_data:
                        federated_example_key = list(version.keys())[0]
                        wf_uuid=list(version[federated_example_key].keys())[0]
                        version_data = version.get(federated_example_key, {})
                        file=version_data[wf_uuid]["path"].split("/")[-1]
                        # Use the file path as the second-level key
                        combined_data.setdefault('workflow', {}).setdefault(federated_example_key, {}).setdefault(wf_uuid, {}).setdefault(file, []).append(version_data[wf_uuid])

        # Serialize the combined data into a single YAML file
        with open(output_file_path+"."+type, 'w') as output_file:
            yaml.dump(combined_data, output_file)
        try:
            if self.use_git:
                pusher=GitHubFilePusher(repo_owner=self.owner,token=self.token,branch_name=self.branch,local_file_path=output_file_path+"."+type,repo_name=self.repo,commit_message= f"The metadata file updated by workflow ID : {os.getenv('PEGASUS_WF_UUID')} of workflow name :  {os.getenv('PEGASUS_WF_LABEL')}")
                pusher.push_file()
        except Exception as e:
                print(e)
        return combined_data

    def create_directories(self,base_directory):
        if not os.path.exists(base_directory):
            os.makedirs(base_directory)

        

if __name__ == '__main__':
    parser = ArgumentParser(description="Pegasus Federated Learning Workflow Example")

    parser.add_argument("-files", default=None, type=str, nargs='+' , help="Files to track")
    parser.add_argument("-file_type", default="yaml", help="Files to track")
    parser.add_argument("-output_file_name", default=None, help="Files to track")

    # git configuration
    parser.add_argument("--use-git", action="store_true", help="Use git repo configuration")
    parser.add_argument("-token", default="" , help="Your GitHub personal access token")
    parser.add_argument("-owner", default="" , help="Repository owner's username")
    parser.add_argument("-repo", default="" , help="Repository name")
    parser.add_argument("-branch", default="" , help="Branch name")
    parser.add_argument("-pull", action="store_true", help="Use metadata from github")

    args = parser.parse_args()
        
    combiner= metadata_class(args.use_git,args.token, args.owner,args.repo, args.branch,args.pull)
    

    combined_yaml_data = combiner.combine_files(args.output_file_name,args.files,args.file_type)

