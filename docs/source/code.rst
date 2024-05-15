Pegasus-WMS Tracking 
====================================================

Pegasus-wms definitions
-------------------------

Data
~~~~~~~~~~~~~~~~ 
Pegasus-wms File/Data Class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        Pegasus provides the `File` class to manage the data in workflow. This class is used to represent the inputs and outputs of a Job.
        This class is defined as follows:

        .. py:class:: File(lfn: str, size: int | None = None, for_planning: bool | None = False)

            Bases: MetadataMixin

            A workflow File. This class is used to represent the inputs and outputs of a Job.

            Example::
            
                input_file = File("input_data.txt").add_metadata(creator="ryan")
                output_file = File("output_data.txt").add_metadata(creator="ryan")

            :param lfn: a unique logical filename
            :type lfn: str
            :param size: size in bytes, defaults to None
            :type size: int, optional
            :param for_planning: indicate that a file is to be used for planning purposes, defaults to False
            :type for_planning: bool, optional

Code
~~~~~~~~~~~~~~~~ 
Pegasus-wms Code/Transformation Class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        A transformation, which can be a standalone executable, or one that requires other executables. 
        The transformation class is defined as follows:

        .. py:class:: Transformation(ProfileMixin, HookMixin, MetadataMixin)

            :param name: the name of the transformation
            :type name: str
            :param namespace: the namespace of the transformation, defaults to None
            :type namespace: str, optional
            :param version: the version of the transformation, defaults to None
            :type version: str, optional
            :param site: the site where the transformation is located, defaults to None
            :type site: str, optional
            :param pfn: the physical filename of the transformation, defaults to None
            :type pfn: str or Path, optional
            :param is_stageable: indicates if the transformation is stageable, defaults to False
            :type is_stageable: bool, optional
            :param bypass_staging: indicates if the transformation bypasses staging, defaults to False
            :type bypass_staging: bool, optional
            :param arch: the architecture of the transformation, defaults to None
            :type arch: Arch, optional
            :param os_type: the operating system type of the transformation, defaults to None
            :type os_type: OS, optional
            :param os_release: the operating system release of the transformation, defaults to None
            :type os_release: str, optional
            :param os_version: the operating system version of the transformation, defaults to None
            :type os_version: str, optional
            :param container: the container used for the transformation, defaults to None
            :type container: Container or str, optional
            :param checksum: the checksum of the transformation, defaults to None
            :type checksum: Dict[str, str], optional


Workflow
~~~~~~~~~~~~~~~~
Pegasus-wms Workflow Class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        Represents multi-step computational steps as a directed acyclic graph.

        .. py:class:: Workflow(Writable, HookMixin, ProfileMixin, MetadataMixin)

            :param name: name of the workflow
            :type name: str
            :param infer_dependencies: whether or not to automatically compute job dependencies based on input and output files used by each job, defaults to True
            :type infer_dependencies: bool, optional
            :raises ValueError: workflow name may not contain any / or spaces

            This class is defined as follows:

            .. py:method:: __init__(self, name: str, infer_dependencies: bool = True)

                Constructor method.

                :param name: name of the workflow
                :type name: str
                :param infer_dependencies: whether or not to automatically compute job dependencies based on input and output files used by each job, defaults to True
                :type infer_dependencies: bool, optional
                :raises ValueError: workflow name may not contain any / or spaces

                Example::
                
                    wf = Workflow("blackdiamond")

workflow example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        Example::

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

Metadata
~~~~~~~~~~~~~~~~
Metadata Function
^^^^^^^^^^^^^^^^^^
        All previous class provides the possibility to define additional metadata information, which can be used as a dictionary as described in the :meth:`add_metadata` function.

        .. py:method:: add_metadata(self, *args: Dict[str, str | int | float | bool], **kwargs)


            Example 1::
            
                job.add_metadata({"key1": "value1"})

            Example 2::
            
                job.add_metadata(key1="value1", key2="value2")

            :param args: dictionary of key value pairs to add as metadata
            :type args: Dict[str, Union[str, int, float, bool]]
            :raises TypeError: each arg in args must be a dict
            :returns: self

Workflow Tracking
-------------------------

Input data tracking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Input  tracking parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

        This class provides the possibility to define additional metadata information, including MLflow tracking behavior. If `mlflow` is set to "auto", MLflow tracking will be automatically enabled. For custom MLflow configuration, provide a dictionary with "experiment_name" and "run_name" keys. If `None`, MLflow tracking will be disabled. The `input_track` parameter indicates whether the file is tracked or not.        
        
        .. py:method:: add_metadata(self, input_track=False, mlflow="auto", *args: Dict[str, str | int | float | bool], **kwargs)


            Example 1::
            
                input_file.add_metadata(input_track=True, mlflow="auto", {"key1": "value1"})

            Example 2::
            
                input_file.add_metadata(input_track=True, mlflow={"experiment_name": "my_experiment", "run_name": "my_run"}, {"key1": "value1"})

            :param input_track: indicates whether the file is to be tracked, defaults to False
            :type input_track: bool, optional
            :param mlflow: specifies MLflow tracking behavior, defaults to "None". If set to "auto", MLflow tracking will be automatically enabled. If a dictionary is provided, MLflow tracking will be configured with the provided parameters (e.g., {"experiment_name": "my_experiment", "run_name": "my_run"}). If `None`, MLflow tracking will be disabled.
            :type mlflow: str, Dict[str, str], or None, optional
            :param args: dictionary of key value pairs to add as metadata
            :type args: Dict[str, Union[str, int, float, bool]]
            :raises TypeError: each arg in args must be a dict
            :returns: self


Output Data Tracking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Output  tracking parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        This method provides the possibility to define additional metadata information, including MLflow tracking behavior. If `mlflow` is set to "auto", MLflow tracking will be automatically enabled. For custom MLflow configuration, provide a dictionary with "experiment_name" and "run_name" keys. If `None`, MLflow tracking will be disabled. The `output_track` parameter indicates whether the file is tracked or not.

        .. py:method:: add_metadata(self, output_track=False, mlflow="auto", *args: Dict[str, str | int | float | bool], **kwargs)

            Example 1::
                
                    output_file.add_metadata(output_track=True, mlflow="auto", {"key1": "value1"})

            Example 2::
                
                    output_file.add_metadata(output_track=True, mlflow={"experiment_name": "my_experiment", "run_name": "my_run"}, {"key1": "value1"})

            :param output_track: indicates whether the file is to be tracked, defaults to False
            :type output_track: bool, optional
            :param mlflow: specifies MLflow tracking behavior, defaults to "auto". If set to "auto", MLflow tracking will be automatically enabled. If a dictionary is provided, MLflow tracking will be configured with the provided parameters (e.g., {"experiment_name": "my_experiment", "run_name": "my_run"}). If `None`, MLflow tracking will be disabled.
            :type mlflow: str, Dict[str, str], or None, optional
            :param args: dictionary of key value pairs to add as metadata
            :type args: Dict[str, Union[str, int, float, bool]]
            :raises TypeError: each arg in args must be a dict
            :returns: self
    .. important::


        MLflow Tracking:

            - MLflow tracking involves creating MLflow experiments and runs and adding the necessary information automatically for tracking as environment variables in the execution environment of the jobs.

        Data Tracking:

            - Data tracking involves creating additional jobs to generate files' metadata and versioning them by using Git. The data is then sent to remote storage for tracking purposes.

Code Tracking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Transformations tracking parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

        .. py:method:: add_metadata(self, trans_track=False, *args: Dict[str, str | int | float | bool], **kwargs)

            Add metadata key-value pairs to this object.

            Example::

                trans.add_metadata(trans_track=True, {"key1": "value1"})

            :param trans_track: indicates whether the transformation is to be tracked, defaults to False
            :type trans_track: bool, optional
            :param args: dictionary of key-value pairs to add as metadata
            :type args: Dict[str, Union[str, int, float, bool]]
            :raises TypeError: each arg in args must be a dict
            :returns: self
        .. important::
            
            Transformation Tracking

                - Transformation tracking involves creating a branch in a configured Git repository with the name of the workflow and pushing all used transformations in the workflow instance to this branch.


Workflow Tracking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Workflow tracking parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        .. py:method:: add_metadata(self, wf_track=False, *args: Dict[str, str | int | float | bool], **kwargs)

            Add metadata key-value pairs to this object.

            Example::

                wf.add_metadata(wf_track=True, {"key1": "value1"})

            :param wf_track: indicates whether the abstract workflow is to be tracked, defaults to False
            :type wf_track: bool, optional
            :param args: dictionary of key-value pairs to add as metadata
            :type args: Dict[str, Union[str, int, float, bool]]
            :raises TypeError: each arg in args must be a dict
            :returns: self


Pegasus Tracking options
-----------------------------

High-Level Tracking Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    For the workflow management system, we added a high-level parameter called "tracker_type" that defines the WMS tracking policy.

    .. py:class:: Workflow(Writable, HookMixin, ProfileMixin, MetadataMixin)

        Represents multi-step computational steps as a directed acyclic graph.

        :param name: name of the workflow
        :type name: str
        :param infer_dependencies: whether or not to automatically compute job dependencies based on input and output files used by each job, defaults to True
        :type infer_dependencies: bool, optional
        :param tracker_type: defines the workflow management system tracking policy. It supports the following policies:

            - "none": Indicates that no tracking will be performed for the workflow. No metadata or tracking information will be recorded. No predefined tags are needed.

            - "full": Enables full tracking of the workflow, including input and output files, transformations, the abstract workflow, MLflow preparation, and any other relevant metadata. No predefined tags are needed.

            - "basic": Tracks only the input and output files of the workflow. No predefined tags are needed.

            - "inputs": Focuses on tracking only input files used by each job in the workflow, recording information such as their location, size, and any associated metadata. Predefined tags for input files may be required.

            - "outputs": Similar to input tracking, this policy tracks output files produced by each job, recording information such as their location, size, and any associated metadata. Predefined tags for output files may be required.

            - "transformations": Tracks only the transformations used in the workflow, recording information such as their location, version, and any associated metadata. Predefined tags for transformations may be required.

        :type tracker_type: str, optional
        :raises ValueError: workflow name may not contain any / or spaces

        This class is defined as follows:

        .. py:method:: __init__(self, name: str, infer_dependencies: bool = True, tracker_type: str = "none")

            Constructor method.

            :param name: name of the workflow
            :type name: str
            :param infer_dependencies: whether or not to automatically compute job dependencies based on input and output files used by each job, defaults to True
            :type infer_dependencies: bool, optional
            :param tracker_type: defines the workflow management system tracking policy. Default is "none".
            :type tracker_type: str, optional
            :raises ValueError: workflow name may not contain any / or spaces
            
    Example::

        wf = Workflow("blackdiamond", tracker_type="full")


Metadata Files
--------------------
Overview
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

During each run, a number of files are utilized by jobs, and these are versioned by collecting information such as file type, last modification timestamp, and local path. Furthermore, details about the execution environment are captured, including maximum CPU and RAM limitations, system specifications, and the execution environment type. This comprehensive approach ensures that all relevant aspects of the workflow execution are documented and versioned, promoting reproducibility and maintaining consistency across different runs.

In this metadata file, we also add a direct link to the run in the ML experimentation platform (currently MLflow) corresponding to the job that uses the file specified in the "mlflow\_url" parameter.

Additionally, a checksum is calculated before copying files to remote storage destinations like S3 and Google Drive. The remote locations are then added to the metadata file. Upon completion of the workflow, an additional job is introduced to aggregate all metadata files generated throughout the workflow. These files are consolidated in the central node, encompassing all runs from various experiments executed on this node.

To ensure ongoing tracking, the central metadata file can be managed using versioning tools such as Git or GitHub, establishing a robust data versioning pipeline for complete traceability and reproducibility.

In the remote storage, the folder structure mirrors that of the metadata file. Each experiment is represented by a folder containing subfolders for individual runs. Within each run folder, there are three subfolders: one for the workflow copy, another for input data, and the last one for output data.

This versioning approach simplifies experiment reproduction, making it easily accessible and reproducible from anywhere with access to the Pegasus workflow executed and the shared metadata file containing the run identifier. We added also, a new command to Pegasus enabling users to copy all files from the remote storage, including the workflow, and rerun it. This process ensures that the experiment can be rerun with the same data and identified by the same run ID, facilitating consistent results without the risk of version mismatch or data modification.

The table presents general information about workflows and files, encompassing details like workflow names, unique identifiers, file names, storage URLs, timestamps, sizes, and associated MLflow run links.

.. list-table::  Workflow and File Tracked Information with Examples
   :widths: 25 40 15 50
   :header-rows: 1

   * - **Field**
     - **Description**
     - **Actors**
     - **Example**
   * - Workflow Name
     - Name of the workflow Experimentation
     - WMS
     - federated-learning-example
   * - Workflow ID
     - Unique identifier for a run in the workflow experimentation workflow
     - WMS
     - 006c8ea9-560c-49d9-9adf-5e6fa82cbeb6
   * - File Name
     - Name of the file versioned by the job
     - Versioning Jobs
     - clusters.json
   * - Bucket Storage URL
     - Storage URL in a bucket system
     -
     - pegasus@osn/asc190064-bucket01/federated-learning/federated-learning-example/006c8ea9-560c-49d9-9adf-5e6fa82cbeb6/outputs/clusters.json
   * - Google Drive Remote URL
     - Remote storage URL for Google Drive
     -
     - 1mYUTXi9AMJ66Ap4Tw2uq6vmD_POE_7vU
   * - Last Modification
     - Timestamp indicating last modification time
     -
     - 1691724673.7948298
   * - Path
     - File path in the local filesystem
     -
     - /home/poseidon/workflows/FL-workflow/federated-learning-fedstack-PM/pegasus-data/clusters.json
   * - Size
     - Size of the file in bytes
     -
     - 433 bytes
   * - Timestamp
     - Timestamp when the file was last modified
     -
     - 2023-08-11 03:31:41.638165
   * - Type
     - Type of the file (e.g., Inputs, Outputs, wf)
     -
     - Outputs
   * - Version
     - Version identifier associated with the file
     -
     - 091fa8cf61dcbf33996d109c363443fb890d087ededd40d200366a396618944c
   * - env
     - Information about the execution environment
     -
     - Details regarding the computational resources where the workflow is run, including operating system, available memory, CPU architecture
   * - mlflow_url
     - Direct link to the MLflow run associated with the job
     -
     - https://dagshub.com/swarmourr/FL-WF.mlflow/#/experiments/8/runs/73ce10d8e29b4fbd987b83a33261ed4d
       8e29b4fbd987b83a33261ed4d

Pegasus-WMS Tracking Capabilities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: Pegasus-WMS Tracking Capabilities
   :widths: 15 15 15 15 15 15 15
   :header-rows: 1

   * - **Tracking Type**
     - **Dynamic**
     - 
     - **Custom**
     - 
     - 
     - 
   * - **Elements & Tags**
     - **Basic Tracking**
     - **Full Tracking**
     - **Input Tracking**
     - **Output Tracking**
     - **WF Tracking**
     - **Transformation Tracking**
   * - **Workflow description**
     - 
     - ✔️
     - 
     - 
     - ✔️
     - ❌
   * - **Workflow instance/version**
     - 
     - ✔️
     - ✔️
     - ✔️
     - ✔️
     - ❌
   * - **Logical file names**
     - ✔️
     - ✔️
     - ✔️
     - ✔️
     - ✔️
     - ❌
   * - **Physical Input files**
     - ✔️
     - ✔️
     - ✔️
     - ❌
     - ❌
     - ❌
   * - **Physical Output files**
     - ✔️
     - ✔️
     - ❌
     - ✔️
     - ❌
     - ❌
   * - **Physical Intermediate files**
     - ✔️
     - ✔️
     - ✔️
     - ✔️
     - ❌
     - ❌
   * - **Logical Transformations**
     - 
     - ✔️
     - ❌
     - ❌
     - ❌
     - ✔️
   * - **Physical Transformations**
     - 
     - ✔️
     - ❌
     - ❌
     - ❌
     - ✔️
   * - **Codes**
     - 
     - ✔️
     - ❌
     - ❌
     - ❌
     - ✔️
   * - **Declaration location**
     - Workflow Declaration
     - Workflow Declaration
     - File Declaration
     - File Declaration
     - Workflow Declaration
     - Transformations Declaration
