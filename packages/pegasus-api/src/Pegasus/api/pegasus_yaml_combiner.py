#!/usr/bin/env python3

from argparse import ArgumentParser
import glob 
import os
import yaml

class metadata_class:

    def __init__(self) -> None:
        pass
        
    def combine_files(self,output_file_path,file_paths,type):
        for k, v in sorted(os.environ.items()):
            print(k+':', v)
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

        return combined_data

    def create_directories(self,base_directory):
        if not os.path.exists(base_directory):
            os.makedirs(base_directory)

        

if __name__ == '__main__':
    parser = ArgumentParser(description="Pegasus Federated Learning Workflow Example")

    parser.add_argument("-files", default=None, type=str, nargs='+' , help="Files to track")
    parser.add_argument("-file_type", default="yaml", help="Files to track")
    parser.add_argument("-output_file_name", default=None, help="Files to track")


    args = parser.parse_args()
        
    combiner= metadata_class()
    combined_yaml_data = combiner.combine_files(args.output_file_name,args.files,args.file_type)

