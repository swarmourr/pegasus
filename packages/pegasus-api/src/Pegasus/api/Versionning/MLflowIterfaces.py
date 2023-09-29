import os
import mlflow

class MLflowManager:
    def __init__(self, tracking_uri, auth_type=None, **kwargs):
        self.tracking_uri = tracking_uri
        self.auth_type = auth_type
        self.auth_kwargs = kwargs
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_registry_uri(tracking_uri)

    def _authenticate(self):
        if self.auth_type == "token":
            os.environ['MLFLOW_TRACKING_TOKEN'] = self.auth_kwargs.get("token")
            mlflow.set_tracking_uri(self.tracking_uri)
            mlflow.set_registry_uri(self.tracking_uri)
        elif self.auth_type == "username_password":
            os.environ['MLFLOW_TRACKING_USERNAME'] = self.auth_kwargs.get("username")
            os.environ['MLFLOW_TRACKING_PASSWORD'] = self.auth_kwargs.get("password")
            mlflow.set_tracking_uri(self.tracking_uri)
            mlflow.set_registry_uri(self.tracking_uri)
        elif self.auth_type == "non_auth":
            mlflow.set_tracking_uri(self.tracking_uri)
            mlflow.set_registry_uri(self.tracking_uri)
        else:
            raise ValueError("Invalid authentication type.")

    def create_experiment(self, experiment_name):
        self._authenticate()
        existing_experiment = mlflow.get_experiment_by_name(experiment_name)
        if existing_experiment:
            print(f"Experiment '{experiment_name}' already exists.")
            return existing_experiment.experiment_id

        mlflow.create_experiment(experiment_name)
        print(f"Experiment '{experiment_name}' created.")

    def get_experiment_id(self, experiment_name):
        self._authenticate()
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment:
            return experiment.experiment_id
        else:
            raise ValueError(f"Experiment '{experiment_name}' does not exist.")

    def create_run(self, experiment_name , run_name):
        self._authenticate()
        experiment_id = self.get_experiment_id(experiment_name)

        with mlflow.start_run(experiment_id=experiment_id, run_name=run_name) as run:
            return run.info.run_id

    def list_runs(self, experiment_name):
        self._authenticate()
        experiment_id = self.get_experiment_id(experiment_name)
        runs = mlflow.search_runs(experiment_ids=[experiment_id])
        return runs

    def get_run(self, run_id):
        self._authenticate()
        return mlflow.get_run(run_id)

    def update_run_tags(self, run_id, new_tags):
        self._authenticate()
        mlflow.set_tags(run_id, new_tags)
        print(f"Tags updated for run {run_id}.")

    def delete_run(self, run_id):
        self._authenticate()
        mlflow.delete_run(run_id)
        print(f"Run {run_id} has been deleted.")

    def log_parameters(self, run_id, parameters):
        self._authenticate()
        with mlflow.start_run(run_id=run_id):
            mlflow.log_params(parameters)

    def update_run_and_log_params(self, run_id, parameters=None):
        self._authenticate()
        with mlflow.start_run(run_id=run_id):
            if parameters:
                mlflow.log_params(parameters)

    def log_metric(self, run_id, key, value):
        self._authenticate()
        with mlflow.start_run(run_id=run_id):
            mlflow.log_metric(key, value)

    def log_artifact(self, run_id, local_path):
        self._authenticate()
        with mlflow.start_run(run_id=run_id):
            mlflow.log_artifact(local_path)

    def set_run_status(self, run_id, status):
        self._authenticate()
        mlflow.set_run_status(run_id, status)

    def get_experiment_url(self, experiment_id):
        self._authenticate()
        return mlflow.get_experiment_url(experiment_id)
    
    def create_and_list_runs(self, experiment_name, run_names):
        created_run_ids = []
        for run_name in run_names:
            run_id = self.create_run(experiment_name, run_name)
            created_run_ids.append(run_id)
        runs_list = self.list_runs(experiment_name)
        return created_run_ids, runs_list
