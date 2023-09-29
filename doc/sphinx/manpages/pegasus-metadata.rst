.. _cli-pegasus-metadata:

================
pegasus-metadata
================

Query metadata collected for Pegasus workflows

   ::

      pegasus-metadata COMMAND [options] <SUBMIT_DIR>



Description
===========

**pegasus-metadata** is a tool to query metadata collected for a
workflow. The tools can query workflow, task, and file metadata.



Commands
========

**workflow**
   Query metadata for a workflow

**task**
   Query metadata for a workflow task

**file**
   Query metadata for files



Global Options
==============

**-v**; \ **--verbose**
   Increase logging verbosity

**-h**; \ **--help**
   Prints a usage summary with all the available command-line options.



Workflow Options
================

**-r**; \ **--recursive**
   Query workflow metadata for the entire workflow; including
   sub-workflows



Task Options
============

**-i** *ABS_TASK_ID*; \ **--task-id** *ABS_TASK_ID*
   Specifies the absolute task id whose metadata should be queried.



File Options
============

**-l**; \ **--list**
   Queries metadata for all files

**-n** *FILE_NAME*; \ **--file-name** *FILE_NAME*
   Specifies name of the file whose metadata should be queried.

**-t**; \ **--trace**
   Queries metadata for the file, the task that generated the file, the
   workflow which contains the task, and the root workflow which
   contains the task.



Examples
========

::

   # Query metadata for a workflow
   $ pegasus-metadata workflow /path/to/submit-dir

   # Query metadata for all workflows i.e. including sub-workflows
   $ pegasus-metadata workflow --recursive /path/to/submit-dir

   # Query task metadata for a given task
   $ pegasus-metadata task --task-id ID0000001 /path/to/submit-dir

   # Query metadata for all files
   $ pegasus-metadata file --list /path/to/submit-dir

   # Query metadata for a given file
   $ pegasus-metadata file --file-name f.a /path/to/submit-dir

   # Trace entire metadata for a given file
   $ pegasus-metadata file --file-name f.a --trace /path/to/submit-dir

