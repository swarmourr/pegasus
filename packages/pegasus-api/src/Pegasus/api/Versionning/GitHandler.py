import argparse
import requests
import json

import argparse
from github import Github

class GitHubFilePusher:
    def __init__(self, token, repo_owner, repo_name, branch_name, local_file_path, commit_message=None):
        self.token = token
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        self.branch_name = branch_name
        self.local_file_path = local_file_path
        self.commit_message = commit_message

    def push_file(self):
        # Authenticate with GitHub using your personal access token
        g = Github(self.token)

        # Get the repository
        repo = g.get_repo(f"{self.repo_owner}/{self.repo_name}")

        # Get the branch
        branch = repo.get_branch(self.branch_name)

        try:
            with open(self.local_file_path, 'rb') as file:
                # Read the local file content
                file_content = file.read()
                file_content_str = file_content.decode('utf-8')

                # Check if the file exists in the repository
                try:
                    file_obj = repo.get_contents(self.local_file_path, ref=branch.name)

                    # Update the file with the existing "sha"
                    repo.update_file(
                        path=self.local_file_path,
                        message=self.commit_message if self.commit_message else "Update file via PyGithub",
                        content=file_content_str,
                        sha=file_obj.sha,
                        branch=branch.name
                    )

                    print(f"File '{self.local_file_path}' updated successfully.")
                except Exception as e:
                    # File doesn't exist, create it
                    print(f"File '{self.local_file_path}' not found. Creating the file...")

                    repo.create_file(
                        path=self.local_file_path,
                        message=self.commit_message if self.commit_message else "Create file via PyGithub",
                        content=file_content_str,
                        branch=branch.name
                    )

                    print(f"File '{self.local_file_path}' created successfully.")

        except Exception as e:
            print(f"Failed to push file '{self.local_file_path}': {str(e)}")


