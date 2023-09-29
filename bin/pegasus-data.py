#!/usr/bin/env python3

import base64
import os
import argparse
import configparser
import sqlite3
import subprocess

import requests

DEFAULT_CONFIG_DIR = os.path.expanduser("~/.pegasus/")
DEFAULT_DB_PATH = os.path.join(DEFAULT_CONFIG_DIR, "workflow.db")
METADATA_FILE=  os.path.join(DEFAULT_CONFIG_DIR, "metadata.yaml")
DEFAULT_CONFIG_FILE=DEFAULT_CONFIG_DIR+"pegasus_data_config.conf"

class GitHandler():
    def __init__(self,local_file_path=METADATA_FILE,file_path="metadata.yaml",commit_message=None,commit_sha=None) -> None:
        self.config=configparser.ConfigParser()
        try:
            self.config.read(DEFAULT_CONFIG_FILE)
        except:
            print("No configuration file found please use pegasus-data to add necessairy configuration if you want data tracking")
        
        if self.config.has_section('git'):
            self.use_git=True
            self.token=self.config.get("git","token")
            self.repo_owner=self.config.get("git","owner")
            self.repo_name=self.config.get("git","repo")
            self.branch=self.config.get("git","branch")
            self.pull=self.config.getboolean("git","pull")
            self.api_base_url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}"
            self.headers = {
            "Authorization": f"token {self.token}",
            }
        self.local_file_path=local_file_path
        self.file_path=file_path
        self.commit_sha=commit_sha
        self.commit_message=commit_message

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
        file_path = "metadata.yaml"

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
                "branch": self.branch,
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
                "branch": self.branch,
            }
            response = requests.put(create_file_url, json=data, headers=headers)
            if response.status_code == 201:
                print(f"File '{file_path}' created successfully.")
            else:
                print(f"Failed to create file '{file_path}': {response.text}")
        else:
            print(f"Failed to check file '{file_path}': {response.text}")

    def retrieve_file_at_commit(self):
        # Retrieve the file content at the specified commit
        file_url = f"{self.api_base_url}/contents/{self.file_path}?ref={self.commit_sha}"
        response = requests.get(file_url, headers=self.headers)

        if response.status_code == 200:
            # Decode the content from base64
            content_base64 = response.json()["content"]
            file_content = base64.b64decode(content_base64).decode("utf-8")

            # Save the retrieved content to the local file
            with open(self.local_file_path, "w") as local_file:
                local_file.write(file_content)

            print(f"File '{self.file_path}' at commit '{self.commit_sha}' downloaded to '{self.local_file_path}'")
        else:
            print(f"Failed to download file '{self.file_path}' at commit '{self.commit_sha}': {response.text}")
    
    def get_latest_commit_sha(self):
        # Retrieve the latest commit SHA of the branch
        commits_url = f"{self.api_base_url}/commits/main"  # Specify the branch name
        response = requests.get(commits_url, headers=self.headers)

        if response.status_code == 200:
            return response.json()["sha"]
        else:
            print(f"Failed to retrieve the latest commit SHA: {response.text}")
            return None
        
    def revert(self):
        # Retrieve the file at the specified commit and save it locally
        self.retrieve_file_at_commit()

        # Read the reverted content from the local file
        with open(self.local_file_path, "r") as local_file:
            reverted_content = local_file.read()
        self.push_file()

    def get_tree_sha_for_commit(self, commit_sha):
        # Retrieve the tree SHA associated with a commit
        commit_url = f"{self.api_base_url}/git/commits/{commit_sha}"
        response = requests.get(commit_url, headers=self.headers)

        if response.status_code == 200:
            return response.json()["tree"]["sha"]
        else:
            print(f"Failed to retrieve the tree SHA for commit '{commit_sha}': {response.text}")
            return None

    def update_branch_reference(self, new_commit_sha):
        # Update the branch reference to point to the new commit
        branch_ref_url = f"{self.api_base_url}/git/refs/heads/main"  # Specify the branch
        data = {
            "sha": new_commit_sha,
            "force": True,  # Allow force update
        }
        response = requests.patch(branch_ref_url, json=data, headers=self.headers)

        if response.status_code != 200:
            print(f"Failed to update the branch reference: {response.text}")
    

class ConfigGenerator:
    def __init__(self, db_path=DEFAULT_DB_PATH):
        self.db_path = db_path
        self.connection = self.create_db_connection()

    def set_env_variable(self,config_dict:dict,prefix:str):
        for key, value in config_dict.items():
            print(f"Creating the env varibale {prefix.upper()}_{key.upper()}")
            os.environ[f"{prefix.upper()}_{key.upper()}"] = value
        

    def create_db_connection(self):
        connection = sqlite3.connect(self.db_path)
        return connection

    def create_config_table(self):
        cursor = self.connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pegasus_data_config (
                id INTEGER PRIMARY KEY,
                section_name TEXT NOT NULL,
                setting_name TEXT NOT NULL,
                setting_value TEXT NOT NULL
            )
        ''')
        self.connection.commit()

    def update_config_in_db(self, section_name, setting_name, setting_value):
        cursor = self.connection.cursor()
        cursor.execute('UPDATE pegasus_data_config SET setting_value = ? WHERE section_name = ? AND setting_name = ?',
                       (setting_value, section_name, setting_name))
        self.connection.commit()

    def save_config_to_db(self, section_name, setting_name, setting_value):
        existing_value = self.get_config_from_db(section_name, setting_name)
        if existing_value is not None:
            self.update_config_in_db(section_name, setting_name, setting_value)
        else:
            cursor = self.connection.cursor()
            cursor.execute('INSERT INTO pegasus_data_config (section_name, setting_name, setting_value) VALUES (?, ?, ?)',
                           (section_name, setting_name, setting_value))
            self.connection.commit()

    def get_config_from_db(self, section_name, setting_name):
        cursor = self.connection.cursor()
        cursor.execute('SELECT setting_value FROM pegasus_data_config WHERE section_name = ? AND setting_name = ?',
                       (section_name, setting_name))
        result = cursor.fetchone()
        if result:
            return result[0]
        return None

    def generate_config_file(self, config_filename, args):
        config = configparser.ConfigParser()

        if args.use_mlflow:
            mlflow_config={
                "tracking_uri": args.mlflow_uri,
                "auth_type": args.mlflow_auth_type,
                "username": args.username,
                "password": args.password,
                "experiment_name": args.experiment_name
            }
            config["MLflow"] = mlflow_config
            self.set_env_variable(mlflow_config,"mlflow")
            self.save_config_to_db("MLflow", "tracking_uri", args.mlflow_uri)
            self.save_config_to_db("MLflow", "auth_type", args.mlflow_auth_type)
            self.save_config_to_db("MLflow", "username", args.username)
            self.save_config_to_db("MLflow", "password", args.password)
            self.save_config_to_db("MLflow", "experiment_name", args.experiment_name)

        if args.use_bucket:
            bucket_config={
                "bucket_name": args.bucket,
                "bucket_path": args.bucket_path,
                "credentials_file": args.credentials_file
            }
            config["Bucket"] = bucket_config
            self.set_env_variable(bucket_config,"bucket")
            self.save_config_to_db("Bucket", "bucket_name", args.bucket)

        if args.use_gdrive:
            gdrive_config={
                "gdrive_credentials_file": args.gdrive_credentials_file,
                "gdrive_parent_folder": args.gdrive_parent_folder
            }
            config["GoogleDrive"] = gdrive_config
            self.set_env_variable(gdrive_config,"gdrive")
            self.save_config_to_db("GoogleDrive", "gdrive_credentials_file", args.gdrive_credentials_file)
            self.save_config_to_db("GoogleDrive", "gdrive_parent_folder", args.gdrive_parent_folder)
        
        if args.use_git:
            git={
                "token": args.token,
                "owner": args.owner,
                "repo": args.repo,
                "branch": args.branch,
                "pull":str(args.pull)
            }
            config["git"] = git
            self.set_env_variable(git,"git")
            self.save_config_to_db("git", "token",args.token)
            self.save_config_to_db("git", "owner", args.owner)
            self.save_config_to_db("git", "owner", args.owner)
            self.save_config_to_db("git", "repo", args.repo)
            self.save_config_to_db("git", "branch", args.branch)
            self.save_config_to_db("git", "pull", args.pull)
            



        # create meta data file
        if not os.path.exists(METADATA_FILE):
            with open(METADATA_FILE, 'w') as file:
                pass  # This does nothing, effectively creating an empty 
            print(f"Empty file {METADATA_FILE} created.")
        else:
            print(f"File '{METADATA_FILE}' already exists.")
        
        config["metadata"] = {
                "path": METADATA_FILE,
            }
          
        with open(config_filename, 'w') as configfile:
            config.write(configfile)
        
        if os.path.abspath(config_filename) != os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf"):
            config = configparser.ConfigParser()
            if os.path.exists(os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf")):
                # Read the configuration
                config.read(os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf"))
                
                if config.has_section('pegasusData') and config.has_option('pegasusData', 'path'):
                    # Read the current value
                    current_value = config.get('pegasusData', 'path')
                    print(f"Current configuration value: {current_value}")

                    # Modify the value
                    config.set('pegasusData', 'path',  os.path.abspath(config_filename))

                    # Write the modified configuration back to the file
                    with open(os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf"), 'w') as configfile:
                        config.write(config)
            else:
                    config["pegasusData"] = {
                        "path":  os.path.abspath(config_filename)
                    }              
                    with open(os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf"), 'w') as configfile:
                            config.write(configfile)            
            
                                           

    def close_connection(self):
        self.connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a configuration file")
    #parser.add_argument("--config-file", default=os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf"), help="Name of the configuration file to create")
    #parser.add_argument("--metadata-file", default="metadata.yaml", help="Name of the metadata file to create")


    # MLflow configuration
    parser.add_argument("--use-mlflow", action="store_true", help="Use MLflow configuration")
    parser.add_argument("--server", action="store_true", help="Use MLflow local server")
    parser.add_argument("--mlflow-uri", default="http://127.0.0.1:5000", help="MLflow tracking URI")
    parser.add_argument("--mlflow-auth-type", choices=["username_password", "token", "non_auth"], default="non_auth", help="MLflow authentication type")
    parser.add_argument("--username", default="", help="MLflow username for authentication")
    parser.add_argument("--password", default="", help="MLflow password for authentication")
    parser.add_argument("--experiment-name", default="default_experiment", help="MLflow experiment name")

    # Bucket configuration
    parser.add_argument("--use-bucket", action="store_true", help="Use bucket configuration")
    parser.add_argument("--bucket", default="default_bucket", help="Bucket name")
    parser.add_argument("--bucket-path", default="", help="bucket path")
    parser.add_argument("--credentials-file", default=f"{DEFAULT_CONFIG_DIR}/credentials.conf", help="Path to the file containing bucket credentials")

    # Google Drive configuration
    parser.add_argument("--use-gdrive", action="store_true", help="Use Google Drive configuration")
    parser.add_argument("--gdrive-credentials-file", default="", help="Path to the file containing Google Drive service account credentials")
    parser.add_argument("--gdrive-parent-folder", default="", help="Parent folder ID for Google Drive files")

    # git configuration
    parser.add_argument("--use-git", action="store_true", help="Use git repo configuration")
    parser.add_argument("-token", help="Your GitHub personal access token")
    parser.add_argument("-owner", help="Repository owner's username")
    parser.add_argument("-repo", help="Repository name")
    parser.add_argument("-branch", help="Branch name")
    parser.add_argument("-pull", action="store_true", help="pull metadata file from github")
    
    #revert to an old version
    parser.add_argument("-revert", action="store_true", help="pull metadata file from github")
    parser.add_argument("-commit_sha", help="Commit SHA of the version to revert to")
    parser.add_argument("-commit_message", help="Commit message for the update")




    args = parser.parse_args()
    if args.revert:
        reverter=GitHandler(commit_sha=args.commit_sha,commit_message=args.commit_message)
        reverter.revert()
    else:
        config_generator = ConfigGenerator()
        config_generator.create_config_table()
        config_generator.generate_config_file(os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf"), args)
        config_generator.close_connection()
        
        if args.server:
            url=args.mlflow_uri
            # Remove the "http://" or "https://" part
            if url.startswith("http://"):
                url = url.replace("http://", "")
            elif url.startswith("https://"):
                url = url.replace("https://", "")

            # Split the URL by ":" to separate host and port
            parts = url.split(":")
            host = parts[0]
            port = int(parts[1]) if len(parts) > 1 else 5000
            mlflow_server_command = f"mlflow server --host {host} --port {port} &"

            # Run the MLflow server as a background subprocess
            try:
                process = subprocess.Popen(mlflow_server_command, shell=True)
                mlflow_server_pid = process.pid
                print(f"MLflow server started in the background with PID: {mlflow_server_pid}")
            except Exception as e:
                print(f"Error starting MLflow server: {e}")

