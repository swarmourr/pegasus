.. _pegasus-aws-batch:

=================
pegasus-aws-batch
=================

pegasus-aws-batch - a client to run tasks on Amazon AWS Batch.
::

      pegasus-aws-batch [-h]
                          [-C propsfile]
                          [-L  <error, warn, info, debug, trace>]
                          [-l logfile]
                          [--merge-logs prefix]
                          [-a AWS account id]
                          [-r AWS region]
                          [--create]
                          [--delete]
                          [-p prefix]
                          [-j jsonfile or arn or job-definition name]
                          [--ce jsonfile or arn or compute-environment name]
                          [-q jsonfile or arn or queue name]
                          [-s  s3 bucket URL]
                          [-f file1[,file2 …]]
                          job submit file



Description
===========

**pegasus-aws-batch** a client to run tasks on Amazon AWS Batch. Also
allows you to create and delete entities such as job definition, compute
environment and job queue required by AWS Batch Service. The tool also
allows you to upload files from your local machine to the S3 bucket
specified on the command line or the properties. This allows you to ship
data to S3 that your jobs running in AWS Batch require. The tool will
automatically fetch the stdout of your jobs from the CloudWatch Logs and
place it on the local filesystem.



Options
=======

**-h**; \ **--help**
   Show help message for subcommand and exit

**-C** *propsfile*; \ **--conf**\ =\ *propsfile*
   Path to the properties file containing the properties to configure
   the too.

**-L** *<error, warn, info, debug, trace>*; \ **-log-level** *<error, warn, info, debug, trace>*
   Sets the log level for the tool.

**-l** *logfile*; \ **--log-file**\ =\ *logfile*
   Path to the file where you want the client to log to. By default,
   client logs to it’s stdout and stderr.

**-m** *prefix*; \ **--merge-logs**\ =\ *prefix*
   By default, the tool pulls down the task stdout and stderr to
   separate files with the name being determined by the job name
   specified in the task submit file. The prefix is used for merging all
   the tasks stdout to a single file starting with the name prefix and
   ending in .out. Similar behavior is applied for the tasks stderr.

**-a** *AWS account id*; \ **--account**\ ='AWS account id '
   the AWS account to use for running jobs on AWS Batch. Can also be
   specified in the properites using the property
   **pegasus.aws.account**.

**-a** *AWS region*; \ **--account**\ =\ *AWS region*
   the AWS region in which the S3 bucket and other batch entitites
   required by AWS batch exist. Can also be specified in the properites
   using the property **pegasus.aws.region**.

**-c**; \ **--create**
   Only create the batch entities specified by -j,--ce,-q,--s3 options

   1. Don’t run any jobs.

**-d**; \ **--delete**
   Delete the batch entities specified by -j,--ce,-q,--s3 options

   1. Don’t run any jobs.

**-p** *prefix*; \ **--prefix**\ =\ *prefix*
   The prefix to use for naming the batch entities created. Default
   suffixes -job-definition, -compute-environment, -job-queue, -bucket
   are added depending on the batch entity being created.

**-j** *jsonfile or arn or job-definition name*; \ **--job-definition** *jsonfile or arn or job-definition name*
   the json file containing job definition specification to register for
   executing jobs or the ARN of existing job definition or basename of
   an existing job definition. The JSON file format is same as the AWS
   Batch format
   https://docs.aws.amazon.com/batch/latest/userguide/job-definition-template.html
   A sample job definition file is listed in the configuration section.

   The value for this option can also be specified in the properites
   using the property **pegasus.aws.batch.job_definition**.

**--ce** *jsonfile or arn or compute environment name*; \ **--compute-environment** *jsonfile or arn or compute environment name*
   the json file containing compute environment specification to create
   in Amazon cloud for executing jobs or the ARN of existing compute
   environment or basename of an existing compute environment. The JSON
   file format is same as the AWS Batch format
   https://docs.aws.amazon.com/batch/latest/userguide/compute-environment-template.html
   A sample compute-environment file is listed in the configuration
   section.

   The value for this option can also be specified in the properites
   using the property **pegasus.aws.batch.compute_environment**.

**-q** *jsonfile or arn or job queue name*; \ **--job-queue** *jsonfile or arn or job queue name*
   the json file containing job queue specification to create in Amazon
   cloud for managing jobs. The queue is associated with the compute
   environment on which the jobs are run, or basename of an existing job
   queue. The JSON file format is same as the AWS Batch format
   https://docs.aws.amazon.com/batch/latest/userguide/job-queue-template.html
   A sample job-queue file is listed in the configuration section.

   The value for this option can also be specified in the properites
   using the property **pegasus.aws.batch.job_queue**.

**-s** *s3 URL*; \ **--s3** *s3 URL*
   The S3 bucket to use for lifecycle of the client. If not specifed
   then a bucket is created based on the prefix passed.

   The value for this option can also be specified in the properites
   using the property **pegasus.aws.batch.s3_bucket**.

**-f** *file*\ [,*file*,…]; \ **--files** *file*\ [,*file*,…]
   A comma separated list of files that need to be copied to the
   associated s3 bucket before any task starts.

*job submit file* A JSON formatted file that contains the job
description of the jobs that need to be executed. A sample job
description file is listed in the configuration section.

.. _AWS_CONFIGURATION:

Configuration
=============

Each user should specify a configuration file that **pegasus-aws-batch**
will use to authentication tokens. It is the same as standard Amazon EC2
credentials file and default Amazon search path semantics apply.



Sample File
-----------

$ cat ~/.aws/credentials

aws_access_key_id = XXXXXXXXXXXX aws_secret_access_key = XXXXXXXXXXX



Configuration Properties
------------------------

**endpoint** (site)
   The URL of the web service endpoint. If the URL begins with *https*,
   then SSL will be used.

**pegasus.aws.account** (aws account) The AWS region to use. Can alse be
specified by -a option.

**pegasus.aws.region** (region) The AWS region to use. Can alse be
specified by -r option.

**pegasus.aws.batch.job_definition** (the json file or existing ARN or
basename) Can alse be specified by -j option.

**pegasus.aws.batch.compute_environment** (the json file or existing ARN
or basename) Can alse be specified by --ce option.

**pegasus.aws.batch.job_queue** (the json file or existing ARN or
basename) Can alse be specified by -q option.

**pegasus.aws.batch.s3_bucket** (the S3 URL) Can alse be specified by
--s3 option.



Example JSON Files
------------------

Example JSON files are listed below



Job Definition File
===================

A sample job definition file. Update to reflect your settings.

::

   $ cat  sample-job-definition.json

   {
     "containerProperties": {
       "mountPoints": [],
       "image": "XXXXXXXXXXX.dkr.ecr.us-west-2.amazonaws.com/awsbatch/fetch_and_run",
       "jobRoleArn": "batchJobRole"  ,
       "environment": [ {
               "name": "PEGASUS_EXAMPLE",
               "value": "batch-black"
            }],
       "vcpus": 1,
       "command": [
         "/bin/bash",
         "-c",
         "exit $AWS_BATCH_JOB_ATTEMPT"
       ],
       "volumes": [],
       "memory": 500,
       "ulimits": []
     },
     "retryStrategy": {
       "attempts": 1
     },
     "parameters": {},
     "type": "container"
   }



Compute Environment File
========================

A sample job definition file. Update to reflect your settings.

::

   $ cat conf/sample-compute-env.json
   {

     "state": "ENABLED",
     "type": "MANAGED",
     "computeResources": {
       "subnets": [
         "subnet-a9bb63cc"
       ],
       "type": "EC2",
       "tags": {
         "Name": "Batch Instance - optimal"
       },
       "desiredvCpus": 0,
       "minvCpus": 0,
       "instanceTypes": [
         "optimal"
       ],
       "securityGroupIds": [
         "sg-91d645f4"
       ],
       "instanceRole": "ecsInstanceRole" ,
       "maxvCpus": 2,
       "bidPercentage": 20
     },
     "serviceRole": "AWSBatchServiceRole"
   }



Job Queue File
==============

A sample job definition file. Update to reflect your settings.

::

   $  cat conf/sample-job-queue.json
   {
     "priority": 10,
     "state": "ENABLED",
     "computeEnvironmentOrder": [
       {
         "order": 1
       }
     ]
   }



Job Submit File
===============

A sample job submit file that lists the bag of jobs that need to be
executed on AWS Batch

::

   $ cat merge_diamond-findrange-4_0_PID2_ID1.in
   {
     "SubmitJob" : [ {
       "jobName" : "findrange_ID0000002",
       "executable" : "pegasus-aws-batch-launch.sh",
       "arguments" : "findrange_ID0000002.sh",
       "environment" : [ {
         "name" : "S3CFG_aws_batch",
         "value" : "s3://pegasus-batch-bamboo/mybatch-bucket/run0001/.s3cfg"
       }, {
         "name" : "TRANSFER_INPUT_FILES",
         "value" : "/scitech/input/pegasus-worker-4.9.0dev-x86_64_rhel_7.tar.gz,/scitech/input/00/00/findrange_ID0000002.sh"
       }, {
         "name" : "BATCH_FILE_TYPE",
         "value" : "script"
       }, {
         "name" : "BATCH_FILE_S3_URL",
         "value" : "s3://pegasus-batch-bamboo/mybatch-bucket/run0001/pegasus-aws-batch-launch.sh"
       } ]
     }, {
       "jobName" : "findrange_ID0000003",
       "executable" : "pegasus-aws-batch-launch.sh",
       "arguments" : "findrange_ID0000003.sh",
       "environment" : [ {
         "name" : "S3CFG_aws_batch",
         "value" : "s3://pegasus-batch-bamboo/mybatch-bucket/run0001/.s3cfg"
       }, {
         "name" : "TRANSFER_INPUT_FILES",
         "value" : "/scitech/input/pegasus-worker-4.9.0dev-x86_64_rhel_7.tar.gz,/scitech/input/00/00/findrange_ID0000003.sh"
       }, {
         "name" : "BATCH_FILE_TYPE",
         "value" : "script"
       }, {
         "name" : "BATCH_FILE_S3_URL",
         "value" : "s3://pegasus-batch-bamboo/mybatch-bucket/run0001/pegasus-aws-batch-launch.sh"
       } ]
     } ]
   }



File Transfers
==============

The tool allows you to upload files to the associated S3 bucket from the
local filesystem in two ways. a. Common Files Required For All Jobs

+ You can the command line option **--files** to give a comma separated
  list of files to transfer.

+ b. TRANSFER_INPUT_FILES Environment Variable

+ You can also associate in the job submit a file, an enviornment
  variable named **TRANSFER_INPUT_FILES** for each job that the tool will
  transfer at the time of job submission. The value for the environment
  variable is a comma separated list of files.


Return Value
============

**pegasus-aws-batch** returns a zero exist status if the operation is
successful. A non-zero exit status is returned in case of failure. If
you run any jobs using the tool, then tool will return with a non zero
exitcode in case one or more of your tasks fail.


