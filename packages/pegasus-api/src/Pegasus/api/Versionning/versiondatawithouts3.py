#!/usr/bin/env python3

from argparse import ArgumentParser
import os
import json
import yaml
import hashlib
import shutil
from datetime import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

class DataVersioning:
    def __init__(self, base_directory="", metadata_format="json", gdrive_credentials_file="gdrive_credentials.json",gdrive_folder_id=None,metadata_file_name=""):
        self.base_directory = base_directory
        self.local_cache_directory = os.path.join(base_directory, "cache")
        self.metadata_file = metadata_file_name + "."+metadata_format
        self.metadata_format = metadata_format
        self.gdrive_credentials_file = gdrive_credentials_file
        self.gdrive_folder_id = gdrive_folder_id
        print(self.metadata_file)
        self.create_directories()
        self.create_empty_metadata_file()
        self.authenticate_gdrive()

    def create_directories(self):
        if not os.path.exists(self.base_directory):
            os.makedirs(self.base_directory)

        if not os.path.exists(self.local_cache_directory):
            os.makedirs(self.local_cache_directory)

    def create_empty_metadata_file(self):
        if not os.path.exists(self.metadata_file):
            metadata = {"versions": [], "files": {}}
            self.save_metadata(metadata)

    def calculate_sha256(self, file_path):
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

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

    def create_version(self, source_files, store_remote=False, upload_to_gdrive=False,type="inputs",pfns=[]):
        metadata = self.load_metadata()

        version_data_list = []

        for source_file,pfn in  zip(source_files, pfns):
            if os.path.exists(source_file):
                sha256_hash = self.calculate_sha256(source_file)

                dest_dir = os.path.join(self.local_cache_directory, sha256_hash)

                if not os.path.exists(dest_dir):
                    os.makedirs(dest_dir)
                    shutil.copy(source_file, os.path.join(dest_dir, os.path.basename(source_file)))
                    if upload_to_gdrive:
                        folder_id = self.upload_to_gdrive(dest_dir,type)
                    else:
                        folder_id = None
                    print(pfn)
                    print(source_file)
                    source_file_value=  pfn if pfn is not None else source_file
                    wf_dict = {
                                os.getenv("PEGASUS_WF_LABEL"): {
                                    os.getenv('PEGASUS_WF_UUID'): {
                                        "version": sha256_hash,
                                        "path": source_file_value,
                                        "timestamp": str(datetime.now()),
                                        "remote_storage_url": folder_id,
                                        "size": os.path.getsize(source_file),
                                        "last_modification": os.path.getmtime(source_file),
                                        "type": type
                                    }
                                }
                            }
                    version_data_list.append(wf_dict)
                    print(f"Version for file '{source_file}' created successfully.")
                else:
                    print(f"Version for file '{source_file}' already exists. Skipping.")

                # Add file hash to the metadata with its original filename
                metadata["files"][os.path.basename(source_file)] = sha256_hash
            else:
                print(f"File '{source_file}' not found. Skipping.")

        metadata.setdefault("versions", []).extend(version_data_list)

        self.save_metadata(metadata)

    print("Version created successfully.")

    def verify_local_cache(self):
        metadata = self.load_metadata()
        for root, _, files in os.walk(self.local_cache_directory):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, self.local_cache_directory)
                expected_hash = metadata.get("files", {}).get(os.path.basename(relative_path))

                if expected_hash:
                    actual_hash = self.calculate_sha256(file_path)
                    if expected_hash != actual_hash:
                        print(f"File {relative_path} has changed!")
                    else:
                        print(f"File {relative_path} Not changed!")


    def revert_to_version(self, version_hash):
        metadata = self.load_metadata()
        filename=None
        version_found = False
        for version_data in metadata.get("versions", []):
            if version_data.get("version") == version_hash:
                filename=version_data.get("source_file").split("/")[-1]
                version_found = True
                break
        print(filename)
        if not version_found:
            print(f"Version with hash {version_hash} not found. Aborting.")
            return

        # Remove the current local cache files
        #for root, _, files in os.walk(self.local_cache_directory):
        #    for file in files:
        #        os.remove(os.path.join(root, file))

        # Copy files from the specified version directory back to the local cache directory
        src_version_dir = os.path.join(self.base_directory, "cache", version_hash)
        for root, _, files in os.walk(src_version_dir):
            for file in files:
                shutil.copy(os.path.join(root, file), self.local_cache_directory)

        print(f"Reverted to version with hash {version_hash}")

        # Update the metadata to remove versions created after the specified version
        versions = metadata.get("versions", [])
        for i, version_data in enumerate(versions):
            if version_data.get("version") == version_hash:
                metadata["versions"] = versions
                break

        for i, files in enumerate(metadata.get("files", [])):
            
            if files == filename:
                metadata["files"][files.split("/")[-1]]=version_hash
                break
        
        self.save_metadata(metadata)
    
    def authenticate_gdrive(self):
        creds = service_account.Credentials.from_service_account_file(
            self.gdrive_credentials_file,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        self.gdrive_credentials = creds

    def upload_to_gdrive_file(self, file_path):
        if not os.path.exists(file_path):
            print(f"File '{file_path}' not found. Aborting upload to Google Drive.")
            return

        service = build("drive", "v3", credentials=self.gdrive_credentials)

        file_metadata = {
            "name": os.path.basename(file_path),
            "parents": [self.gdrive_folder_id],  # Replace self.gdrive_folder_id with the ID of the folder you want to upload the file to
        }
        media = MediaFileUpload(file_path, resumable=True)
        print(media.__dict__)
        file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        print(file)
        file_id = file.get("id")
        print(f"File '{file_path}' uploaded to Google Drive with ID: {file_id}")
 
    def upload_to_gdrive(self, directory_path,type):
        if not os.path.exists(directory_path):
            print(f"Directory '{directory_path}' not found. Aborting upload to Google Drive.")
            return

        service = build("drive", "v3", credentials=self.gdrive_credentials)

        # Split the parent folder path into individual folder names
        remote_path=os.getenv("PEGASUS_WF_LABEL")+'/'+os.getenv("PEGASUS_WF_UUID")+'/'+type
        remote=remote_path.split("/")
        folder_names = directory_path.split('/')
        
        # Verify the existence of the parent folder
        parent_folder_id = self.gdrive_folder_id
        for folder_name in remote:
            query = f"name='{folder_name}'"
            if parent_folder_id is not None:
                query += f" and '{parent_folder_id}' in parents"

            folder_results = service.files().list(q=query, fields='files(id)').execute()
            folders = folder_results.get('files', [])

            if folders:
                # Parent folder with the same name exists, use the first one found
                parent_folder_id = folders[0]['id']
                print(f"Found existing parent folder '{folder_name}' with ID: {parent_folder_id}.")
            else:
                folder_metadata = {
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder'
                }
                if parent_folder_id is not None:
                    folder_metadata['parents'] = [parent_folder_id]

                folder = service.files().create(body=folder_metadata, fields='id').execute()
                parent_folder_id = folder.get('id')
                print(f"Folder '{folder_name}' created on Google Drive with ID: {parent_folder_id}")


        # Create the current workflow project directory
        folder_name = folder_names[-1]
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_folder_id is not None:
            folder_metadata['parents'] = [parent_folder_id]

        folder = service.files().create(body=folder_metadata, fields='id').execute()
        folder_id = folder.get('id')
        print(f"Folder '{folder_name}' created on Google Drive with ID: {folder_id}")

        # Upload files to the created folder
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, directory_path)
                file_metadata = {
                    "name": os.path.basename(file_path),
                    "parents": [folder_id],
                }
                media = MediaFileUpload(file_path, resumable=True)
                file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()

                file_id = file.get("id")
                #self.metadata["files"][relative_path] = file_id
                print(f"File '{file_path}' uploaded to Google Drive with ID: {file_id}")

        return folder_id
    

if __name__ == '__main__':
    parser = ArgumentParser(description="Track Workflow data" )
    parser.add_argument("-files", default=None, type=str, nargs='+' , help="Files to track")
    parser.add_argument("-pfn", default=None, type=str, nargs='+' , help="pfn Files to track")
    parser.add_argument("-credentials", default=None, type=str , help="Files credentials")
    parser.add_argument("-metadata_format", default="yaml", type=str , help="Files credentials")
    parser.add_argument("-remote_id", default=None, type=str , help="remote folder")
    parser.add_argument("-gdrive", default=False, action="store_true", help="Use google drive")
    parser.add_argument("-bucket", default=False, action="store_true", help="Use bucket or s3")
    parser.add_argument("-data_dir", default=".", type=str , help="Folder to store cache")
    parser.add_argument("-o", default="", type=str , help="metadata file name")
    parser.add_argument("-file_type", default="", type=str , help="metadata file name")
    args = parser.parse_args()



    # Initialize DataVersioning with your desired configurations
    base_directory = args.data_dir  # You can change this to your desired base directory
    metadata_format = args.metadata_format # Change to "json" if you prefer JSON format
    gdrive_credentials_file = args.credentials  # Update with your Google Drive credentials file
    gdrive_folder_id=args.remote_id
    gdrive_Flag=args.gdrive
    store_remote_Flag=args.bucket
    print(gdrive_Flag)
    print(store_remote_Flag)
    print(args.o)

    versioning = DataVersioning(base_directory,metadata_format,args.credentials,gdrive_folder_id=gdrive_folder_id,metadata_file_name=args.o)

    # Create versions for multiple files
    source_files = args.files#["/home/poseidon/workflows/FL-workflow/federated-learning-fedstack-PM/wf.png","/home/poseidon/workflows/FL-workflow/federated-learning-fedstack-PM/data/oneyeardata.csv","/home/poseidon/workflows/FL-workflow/federated-learning-fedstack-PM/data/oneyeardatacopy.csv"]
    versioning.create_version(source_files, store_remote=store_remote_Flag, upload_to_gdrive=gdrive_Flag,type=args.file_type,pfns=args.pfn)

    # Verify if files in the local cache have changed
    #versioning.verify_local_cache()

    # Revert to a specific version
    #version_hash_to_revert = "491ca86e6d6fc6ce6b672998aa108a786b8a1ec22cec66820b0253b97c8034f4"
    #versioning.revert_to_version(version_hash_to_revert)

