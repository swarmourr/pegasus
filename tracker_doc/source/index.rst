.. pegasus-wms-Tracking-pkg documentation master file, created by
   sphinx-quickstart on Tue May  7 18:26:43 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pegasus-wms Tracking pkg's documentation!
====================================================

.. toctree::
   :maxdepth: 3

   ./code.rst
   ./tutorials.rst

Overview
-----------

In order to track the workflows in Pegasus-WMS, we extend the metadata function of the main class composing Pegasus workflows. We utilize the `File <https://pegasus.isi.edu/documentation/python/Pegasus.api.html#Pegasus.api.replica_catalog.File>`_ class, responsible for data management, from the Pegasus API documentation. Additionally, we leverage the `Transformations <https://pegasus.isi.edu/documentation/python/Pegasus.api.html#Pegasus.api.transformation_catalog.Transformation>`_ class, responsible for managing executable code. Finally, we add another parameter for the `Workflow <https://pegasus.isi.edu/documentation/python/Pegasus.api.html#Pegasus.api.workflow.Workflow>`_ class to instantiate the workflow.

.. warning::

    In this documentation, we describe how to use the tracking tags with Pegasus-WMS for workflow implementation. For detailed instructions and examples, please visit the `full documentation <https://pegasus.isi.edu/documentation/index.html>`_.



Implementation
---------------

.. figure:: img/archi.png
   
   System Design


In this implementation, we extend the Python API of Pegasus-WMS by introducing a new component that incorporates the ML functionalities.

System Design Figure illustrates the main steps of Pegasus-WMS to execute a workflow:

1. Create phase: the user uses the Pegasus API to generate an abstract workflow description, outlining tasks, input/output files, and dependencies, without specific file formats.
   
2. Plan phase: Pegasus converts the abstract description into a concrete one, adding specific execution details and generating a formatted workflow DAG.
   
3. Run phase: Pegasus delegates the DAG to HTCondor for job queue management and execution supervision.

In this implementation, the Pegasus-data component is integrated into the Pegasus API as the second substep in the creation phase to add necessary tracking jobs based on the abstract workflows defined by the user. This component is invoked during workflow generation using the abstract workflow described by the user to analyze the tracking and experiments tags defined by the user. This analysis leads to the creation of MLflow experiments and runs, generating a unique run ID for each job, which is then passed to the execution environment for each job as environment variables like **MLFLOW_RUN_ID**, **MLFLOW_EXPERIMENT_NAME**, **MLFLOW_TRACKING_URI**, **MLFLOW_CREDENTIALS**, and others based on configuration (auto or custom). Additionally, users simply need to use these variables to authenticate to MLflow and log all necessary information, such as parameters, metrics, and figures, using the MLflow Python API provided by default in all Pegasus execution environments.

In parallel with MLflow run creation, the Pegasus-data component adds data tracking jobs to the abstract workflow. These jobs execute concurrently with the main jobs defined by the user, collecting all necessary information about the data for each job and transmitting it to remote storage, which currently supports buckets and Google Drive based on user configuration. These jobs utilize the environment variable **MLFLOW_ENABLED** to determine if MLflow is enabled; if enabled, the data tracking jobs log information about the workflow to MLflow, primarily the unique ID of the workflow execution. Additionally, they log the remote storage URL and the metadata file of all data used in the jobs to maintain the relationship and facilitate mapping between workflow execution, data versioning, and experiment outputs.

It is important to note that all connection information and configurations are stored in a central local configuration file and duplicated in the main database of Pegasus, housed in the central node of Pegasus-WMS, and passed as needed as environment variables in the execution environment.

