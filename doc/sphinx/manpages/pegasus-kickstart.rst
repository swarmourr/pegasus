.. _cli-pegasus-kickstart:

=================
pegasus-kickstart
=================

Pegasus remote job wrapper
::

      pegasus-kickstart [-n tr] [-N dv] [-H] [-R site] [-W | -w dir]
                        [-L lbl -T iso] [-s p | @fn] [-S p | @fn] [-i fn]
                        [-o fn] [-e fn] [-X] [-l fn sz] [-F] (-I fn | app [appflags])
      pegasus-kickstart -V



Description
===========

**pegasus-kickstart** is a wrapper program which manages and monitors
the execution of jobs on remote resources.

Sitting in between the remote scheduler and the application process, it
is possible for **pegasus-kickstart** to gather additional information
about the process' run-time behavior and resource usage, including the
exit status of jobs. This information is important for Pegasus
invocation tracking as well as detecting Globus GRAM job failures.

**pegasus-kickstart** allows the optional execution of jobs before and
after the main application job that run in chained execution with the
main application job. See section `SUBJOBS` for details
about this feature.

It also allows stdin, stdout, and stderr to be redirected from/to
specific files.

All jobs with relative path specifications to the application are part
of search relative to the current working directory (yes, this is
unsafe), and by prepending each component from the *PATH* environment
variable. The first match is used. Jobs that use absolute pathnames,
starting in a slash, are exempt. Using an absolute path to your
executable is the safe and recommended option.

**pegasus-kickstart** rewrites the command line of any job (pre, post
and main) with variable substitutions from Unix environment variables.
See section `VARIABLE REWRITING` below for
details on this feature.



Options
=======

**-n** *tr*
   In order to associate the minimal performance information of the job
   with the invocation records, the jobs needs to carry which
   *transformation* was responsible for producing it. The format is the
   textual notation for fully-qualified definition names, like
   namespace::name:version, with only the name portion being mandatory.

   There is no default. If no value is given, "null" will be reported.

**-N** *dv*
   The jobs may carry which instantiation of a transformation was
   responsible for producing it. The format is the textual notation for
   fully-qualified definition names, like namespace::name:version, with
   only the name portion being mandatory.

   There is no default. If no value is given, "null" will be reported.

**-H**
   This option avoids pegasus-kickstart writing the XML preamble
   (entity), if you need to combine multiple pegasus-kickstart records
   into one document.

   Additionally, if specified, the environment and the resource usage
   segments will not be written, assuming that a in a concatenated
   record version, the initial run will have captured those settings.

**-R** *site*
   In order to provide the greater picture, pegasus-kickstart can
   reflect the site handle (resource identifier) into its output.

   There is no default. If no value is given, the attribute will not be
   generated.

**-L** *lbl*; \ **-T** *iso*
   These optional arguments denote the workflow label (from DAX) and the
   workflow’s last modification time (from DAX). The label *lbl* can be
   any sensible string of up to 32 characters, but should use C
   identifier characters. The timestamp *iso* must be an ISO 8601
   compliant time-stamp.

**-S** *l=p*
   If stat information on any file is required *before* any jobs were
   started, logical to physical file mappings to stat can be passed
   using the **-S** option. The LFN and PFN are concatenated by an
   equals (=) sign. The LFN is optional: If no equals sign is found, the
   argument is taken as sole PFN specification without LFN.

   This option may be specified multiple times. To reduce and overcome
   command line length limits, if the argument is prefixed with an at
   (@) sign, the argument is taken to be a textual file of LFN to PFN
   mappings. The optionality mentioned above applies. Each line inside
   the file argument is the name of a file to stat. Comments (#) and
   empty lines are permitted.

   Each PFN will incur a *statcall* record (element) with attribute *id*
   set to value *initial*. The optional *lfn* attribute is set to the
   LFN stat’ed. The filename is part of the *statinfo* record inside.

   There is no default.

**-s** *fn*
   If stat information on any file is required *after* all jobs have
   finished, logical to physical file mappings to stat can be passed
   using the **-s** option. The LFN and PFN are concatenated by an
   equals (=) sign. The LFN is optional: If no equals sign is found, the
   argument is taken as sole PFN specification without LFN.

   This option may be specified multiple times. To reduce and overcome
   commandline length limits, if the argument is prefixed with an at (@)
   sign, the argument is taken to be a textual file of LFN to PFN
   mappings. The optionality mentioned above applies. Each line inside
   the file argument is the name of a file to stat. Comments (#) and
   empty lines are permitted.

   Each PFN will incur a *statcall* record (element) with attribute *id*
   set to value *final*. The optional *lfn* attribute is set to the LFN
   stat’ed. The filename is part of the *statinfo* record inside.

   There is no default.

**-i** *fn*
   This option allows **pegasus-kickstart** to re-connect the stdin of
   the application that it starts. Use a single hyphen to share *stdin*
   with the one provided to **pegasus-kickstart**.

   The default is to connect *stdin* to */dev/null*.

**-o** *fn*
   This option allows **pegasus-kickstart** to re-connect the *stdout*
   of the application that it starts. The mode is used whenever an
   application produces meaningful results on its *stdout* that need to
   be tracked by Pegasus. The real *stdout* of Globus jobs is staged via
   GASS (GT2) or RFT (GT4). The real *stdout* is used to propagate the
   invocation record back to the submit site. Use the single hyphen to
   share the application’s *stdout* with the one that is provided to
   **pegasus-kickstart**. In that case, the output from
   **pegasus-kickstart** will interleave with application output. For
   this reason, such a mode is not recommended.

   In order to provide an un-captured *stdout* as part of the results,
   it is the default to connect the *stdout* of the application to a
   temporary file. The content of this temporary file will be
   transferred as payload data in the **pegasus-kickstart** results. The
   content size is subject to payload limits, see the **-B** option. If
   the content grows large, only the last portion will become part of
   the payload. If the temporary file grows too large, it may flood the
   worker node’s temporary space. The temporary file will be deleted
   after **pegasus-kickstart** finishes.

   If the filename is prefixed with an exclamation point, the file will
   be opened in append mode instead of overwrite mode. Note that you may
   need to escape the exclamation point from the shell.

   The default is to connect *stdout* to a temporary file.

**-e** *fn*
   This option allows **pegasus-kickstart** to re-connect the *stderr*
   of the application that it starts. This option is used whenever an
   application produces meaningful results on *stderr* that needs
   tracking by Pegasus. The real *stderr* of Globus jobs is staged via
   GASS (GT2) or RFT (GT4). It is used to propagate abnormal behavior
   from both, **pegasus-kickstart** and the application that it starts,
   though its main use is to propagate application dependent data and
   heartbeats. Use a single hyphen to share *stderr* with the *stderr*
   that is provided to **pegasus-kickstart**. This is the backward
   compatible behavior.

   In order to provide an un-captured *stderr* as part of the results,
   by default the *stderr* of the application will be connected to a
   temporary file. Its content is transferred as payload data in the
   **pegasus-kickstart** results. If too large, only the last portion
   will become part of the payload. If the temporary file grows too
   large, it may flood the worker node’s temporary space. The temporary
   file will be deleted after **pegasus-kickstart** finishes.

   If the filename is prefixed with an exclamation point, the file will
   be opened in append mode instead of overwrite mode. Note that you may
   need to escape the exclamation point from the shell.

   The default is to connect *stderr* to a temporary file.

**-l** *logfn*
   allows to append the performance data to the specified file. Thus,
   multiple XML documents may end up in the same file, including their
   XML preamble. *stdout* is normally used to stream back the results.
   Usually, this is a GASS-staged stream. Use a single hyphen to
   generate the output on the *stdout* that was provided to
   **pegasus-kickstart**, the default behavior.

   Default is to append the invocation record onto the provided
   *stdout*.

**-w** *dir*
   permits the explicit setting of a new working directory once
   pegasus-kickstart is started. This is useful in a remote scheduling
   environment, when the chosen working directory is not visible on the
   job submitting host. If the directory does not exist,
   **pegasus-kickstart** will fail. This option is mutually exclusive
   with the **-W** *dir* option.

   Default is to use the working directory that the application was
   started in. This is usually set up by a remote scheduling
   environment.

**-W** *dir*
   permits the explicit creation and setting of a new working directory
   once pegasus-kickstart is started. This is useful in a remote
   scheduling environment, when the chosen working directory is not
   visible on the job submitting host. If the directory does not exist,
   **pegasus-kickstart** will attempt to create it, and then change into
   it. Both, creation and directory change may still fail. This option
   is mutually exclusive with the **-w** *dir* option.

   Default is to use the working directory that the application was
   started in. This is usually set up by a remote scheduling
   environment.

**-X**
   make an application executable, no matter what. It is a work-around
   code for a weakness of **globus-url-copy** which does not copy the
   permissions of the source to the destination. Thus, if an executable
   is staged-in using GridFTP, it will have the wrong permissions.
   Specifying the **-X** flag will attempt to change the mode to include
   the necessary x (and r) bits to make the application executable.

   Default is not to change the mode of the application. Note that this
   feature can be misused by hackers, as it is attempted to call chmod
   on whatever path is specified.

**-B** *sz*
   Changes the amount of stdout and stderr data to include in the
   output. The last *sz* bytes of the stdout and stderr of the process
   will be copied into kickstart’s output. All other data will be
   discarded. The special value *all* can be used to capture all the
   stdout/stderr of the process. The default is 256KB.

**-F**
   This flag will issue an explicit **fsync()** call on kickstart’s own
   *stdout* file. Typically you won’t need this flag. Albeit, certain
   shared file system situations may improve when adding a flush after
   the written invocation record.

   The default is to just use kickstart’s NFS alleviation strategy by
   locking and unlocking *stdout*.

**-I** *fn*
   In this mode, the application name and any arguments to the
   application are specified inside of file *fn*. The file contains one
   argument per line. Escaping from Globus, Condor and shell meta
   characters is not required. This mode permits to use the maximum
   possible command line length of the underlying operating system, e.g.
   128k for Linux. Using the **-I** mode stops any further command line
   processing of **pegasus-kickstart** command lines.

   Default is to use the *app flags* mode, where the application is
   specified explicitly on the command-line.

**-f**
   This flag causes kickstart to output full information, including the
   environment and resource limits under which the job ran, and any
   useful auxilliary statcalls. If the job fails, then **-f** is
   implied.

**-k** *S*
   This flag causes kickstart to send the job a SIGTERM if it is still
   running after S seconds. The default value is 0, which disables the
   timeout.

**-K** *S*
   This flag causes kickstart to send the job a SIGKILL if it is still
   running S seconds after recieving a SIGTERM sent as a result of the
   **-k** flag. The default value is 5. If **-k** is not set, or is set
   to 0, then this flag is ignored.

**-t**
   This flag causes kickstart to use ptrace() to collect resource usage
   info for the process by intercepting the process start and stop
   events. This flag only exists when kickstart is compiled for Linux.

**-z**
   This flag causes kickstart to use ptrace() to intercept system calls
   and report a list of files accessed and I/O performed. This flag only
   exists when kickstart is compiled for Linux.

**-Z**
   This flag causes kickstart to use LD_PRELOAD to intercept library
   calls and report a list of files accessed and I/O performed. This
   flag only exists when kickstart is compiled for Linux. There are
   several environment variables documented below that control what file
   accesses are traced.

**-q**
   This flag causes kickstart to omit the <data> part of the <statcall>
   records when the job exits successfully. This is designed to reduce
   the size of the output logs for large workflows.

**-c**
   This flag causes kickstart to output <data> from stdout and stderr as
   a CDATA section instead of quoting it.

*app*
   The path to the application has to be completely specified. The
   application is a mandatory option.

*appflags*
   Application may or may not have additional flags.



Return Value
============

**pegasus-kickstart** will return the return value of the main job. In
addition, the error code 127 signals that the call to exec failed, and
126 that reconnecting the stdio failed. A job failing with the same exit
codes is indistinguishable from **pegasus-kickstart** failures.



See Also
========

pegasus-plan(1), condor_submit_dag(1), condor_submit(1), getrusage(3c).

.. _SUBJOBS:

Subjobs
=======

Subjobs are a new feature and may have a few wrinkles left.

In order to allow specific setups and assertion checks for compute
nodes, **pegasus-kickstart** allows the optional execution of a
*prejob*. This *prejob* is anything that the remote compute node is
capable of executing. For modern Unix systems, this includes #! scripts
interpreter invocations, as long as the x bits on the executed file are
set. The main job is run if and only if the prejob returned regularly
with an exit code of zero.

With similar restrictions, the optional execution of a *postjob* is
chained to the success of the main job. The postjob will be run, if the
main job terminated normally with an exit code of zero.

In addition, a user may specify a *setup* and a *cleanup* job. The
*setup* job sets up the remote execution environment. The *cleanup* job
may tear down and clean-up after any job ran. Failure to run the setup
job has no impact on subsequent jobs. The cleanup is a job that will
even be attempted to run for all failed jobs. No job information is
passed. If you need to invoke multiple setup or clean-up jobs, bundle
them into a script, and invoke the clean-up script. Failure of the
clean-up job is not meant to affect the progress of the remote workflow
(DAGMan). This may change in the future.

The setup-, pre-, and post- and cleanup-job run on the same compute node
as the main job to execute. However, since they run in separate
processes as children of **pegasus-kickstart**, they are unable to
influence each others nor the main jobs environment settings.

All jobs and their arguments are subject to variable substitutions as
explained in the next section.

To specify the prejob, insert the the application invocation and any
optional commandline argument into the environment variable
*KICKSTART_PREJOB*. If you are invoking from a shell, you might want to
use single quotes to protect against the shell. If you are invoking from
Globus, you can append the RSL string feature. From Condor, you can use
Condor’s notion of environment settings. In Pegasus use the *profile*
command to set generic scripts that will work on multiple sites, or the
transformation catalog to set environment variables in a pool-specific
fashion. Please remember that the execution of the main job is chained
to the success of the prejob.

To set up the postjob, use the environment variable *KICKSTART_POSTJOB*
to point to an application with potential arguments to execute. The same
restrictions as for the prejob apply. Please note that the execution of
the post job is chained to the main job.

To provide the independent setup job, use the environment variable
*KICKSTART_SETUP*. The exit code of the setup job has no influence on
the remaining chain of jobs. To provide an independent cleanup job, use
the environment variable *KICKSTART_CLEANUP* to point to an application
with possible arguments to execute. The same restrictions as for prejob
and postjob apply. The cleanup is run regardless of the exit status of
any other jobs.

.. _VARIABLE_REWRITING:

Variable Rewriting
==================

Variable substitution is a new feature and may have a few wrinkles left.

The variable substitution employs simple rules from the Bourne shell
syntax. Simple quoting rules for backslashed characters, double quotes
and single quotes are obeyed. Thus, in order to pass a dollar sign to as
argument to your job, it must be escaped with a backslash from the
variable rewriting.

For pre- and postjobs, double quotes allow the preservation of
whitespace and the insertion of special characters like \\a (alarm), \\b
(backspace), \\n (newline), \\r (carriage return), \\t (horizontal tab),
and \\v (vertical tab). Octal modes are *not* allowed. Variables are
still substituted in double quotes. Single quotes inside double quotes
have no special meaning.

Inside single quotes, no variables are expanded. The backslash only
escapes a single quote or backslash.

Backticks are not supported.

Variables are only substituted once. You cannot have variables in
variables. If you need this feature, please request it.

Outside quotes, arguments from the pre- and postjob are split on linear
whitespace. The backslash makes the next character verbatim.

Variables that are rewritten must start with a dollar sign either
outside quotes or inside double quotes. The dollar may be followed by a
valid identifier. A valid identifier starts with a letter or the
underscore. A valid identifier may contain further letters, digits or
underscores. The identifier is case sensitive.

The alternative use is to enclose the identifier inside curly braces. In
this case, almost any character is allowed for the identifier, including
whitespace. This is the *only* curly brace expansion. No other Bourne
magic involving curly braces is supported.

One of the advantages of variable substitution is, for example, the
ability to specify the application as *$HOME/bin/app1* in the
transformation catalog, and thus to gridstart. As long as your home
directory on any compute node has a *bin* directory that contains the
application, the transformation catalog does not need to care about the
true location of the application path on each pool. Even better, an
administrator may decide to move your home directory to a different
place. As long as the compute node is set up correctly, you don’t have
to adjust any Pegasus data.

Mind that variable substitution is an expert feature, as some degree of
tricky quoting is required to protect substitutable variables and quotes
from Globus, Condor and Pegasus in that order. Note that Condor uses the
dollar sign for its own variables.

The variable substitution assumptions for the main job differ slightly
from the prejob and postjob for technical reasons. The pre- and postjob
command lines are passed as one string. However, the main jobs command
line is already split into pieces by the time it reaches
**pegasus-kickstart**. Thus, any whitespace on the main job’s command
line must be preserved, and further argument splitting avoided.

It is highly recommended to experiment on the Unix command line with the
*echo* and *env* applications to obtain a feeling for the different
quoting mechanisms needed to achieve variable substitution.



Example
=======

You can run the **pegasus-kickstart** executable locally to verify that
it is functioning well. In the initial phase, the format of the
performance data may be slightly adjusted.

::

   $ env KICKSTART_PREJOB='/bin/usleep 250000' \\
     KICKSTART_POSTJOB='/bin/date -u' \\
     pegasus-kickstart -l xx \\$PEGASUS_HOME/bin/keg -T1 -o-
   $ cat xx
   <?xml version="1.0" encoding="ISO-8859-1"?>
     ...
     </statcall>
   </invocation>

Please take note a few things in the above example:

The output from the postjob is appended to the output of the main job on
*stdout*. The output could potentially be separated into different data
sections through different temporary files. If you truly need the
separation, request that feature.

The log file is reported with a size of zero, because the log file did
indeed barely exist at the time the data structure was (re-)
initialized. With regular GASS output, it will report the status of the
socket file descriptor, though.

The file descriptors reported for the temporary files are from the
perspective of **pegasus-kickstart**. Since the temporary files have the
close-on-exec flag set, **pegasus-kickstart**\ *s file descriptors are
invisible to the job processes. Still, the 'stdio* of the job processes
are connected to the temporary files.

Even this output already appears large. The output may already be too
large to guarantee that the append operation on networked pipes (GASS,
NFS) are atomically written.

The current format of the performance data is as follows:



Timeouts
========

Kickstart sets timeouts for the job based on the **-k** and **-K**
flags. The **-k** flag sets the time kickstart will wait before it sends
the job a SIGTERM, and the **-K** flag sets the time kickstart will wait
after delivering a SIGTERM until it delivers a SIGKILL. The **-K**
timeout is designed to give the job some time to write a checkpoint,
which it can trigger by handling the SIGTERM. If the job runs for longer
than the timeout specified using **-k**, then then the job exits with a
non-zero exit status.

If the job has KICKSTART_SETUP, KICKSTART_PREJOB, or KICKSTART_POSTJOB,
then their runtimes are included in the timeout and they will be sent
SIGTERM/SIGKILL in the same manner as the main job. If KICKSTART_CLEANUP
is set, then it will run regardless of whether processes from the other
stages were signalled. If KICKSTART_SETUP is specified, and it runs
longer than the timeout, then it will be signalled, and the other stages
will be skipped.



Output Format
=============

Refer to
https://pegasus.isi.edu/documentation/schemas/iv-2.2/iv-2.2.html for an
up-to-date description of elements and their attributes. Check with
https://pegasus.isi.edu/documentation for invocation schemas with a
higher version number.



Restrictions
============

There is no version for the Condor *standard* universe. It is simply not
possible within the constraints of Condor.

Due to its very nature, **pegasus-kickstart** will also prove difficult
to port outside the Unix environment.

Any of the pre-, main-, cleanup and postjob are unable to influence one
another’s visible environment.

Do not use a Pegasus transformation with just the name *null* and no
namespace nor version.

First Condor, and then Unix, place a limit on the length of the command
line. The additional space required for the gridstart invocation may
silently overflow the maximum space, and cause applications to fail. If
you suspect to work with many argument, try an argument-file based
approach.

A job failing with exit code 126 or 127 is indistinguishable from
**pegasus-kickstart** failing with the same exit codes. Sometimes,
careful examination of the returned data can help.

If the logfile is collected into a shared file, due to the size of the
data, simultaneous appends on a shared filesystem from different
machines may still mangle data. Currently, file locking is not even
attempted, although all data is written atomically from the perspective
of **pegasus-kickstart**.

The upper limit of characters of command line characters is currently
not checked by **pegasus-kickstart**. Thus, some variable substitutions
could potentially result in a command line that is larger than
permissible.

If the output or error file is opened in append mode, but the
application decides to truncate its output file, as in the above example
by opening */dev/fd/1* inside *keg*, the resulting file will still be
truncated. This is correct behavior, but sometimes not obvious.



Files
=====

**/usr/share/pegasus/schema/iv-2.2.xsd**
   is the suggested location of the latest XML schema describing the
   data on the submit host.

.. _METADATA:

Metadata
========

Kickstart creates a file to which the job should write metadata
"key=value" pairs. The contents of the file are inserted into the
invocation record by Kickstart, and transferred with the job’s stdio. If
the job is run under Pegasus, then pegasus-monitord will parse this
metadata and merge it with the metadata for the job in the Pegasus
workflow database. Kickstart uses the environment variable
**KICKSTART_METADATA** to tell the job to which file it should write its
metadata.



Environment Variables
=====================

Note: Pegasus 4.6 deprecated the "GRIDSTART" prefix for environment
variables and replaced it with "KICKSTART". The "GRIDSTART" versions
of the old variables should still work.

**KICKSTART_TMP**
   is the hightest priority to look for a temporary directory, if
   specified. This rather special variable was introduced to overcome
   some peculiarities with the FNAL cluster.

**TMP**
   is the next hightest priority to look for a temporary directory, if
   specified.

**TEMP**
   is the next priority for an environment variable denoting a temporary
   files directory.

**TMPDIR**
   is next in the checklist. If none of these are found, either the
   *stdio* definition *P_tmpdir* is taken, or the fixed string */tmp*.

**KICKSTART_SETUP**
   contains a string that starts a job to be executed unconditionally
   before any other jobs, see above for a detailed description.

**KICKSTART_PREJOB**
   contains a string that starts a job to be executed before the main
   job, see above for a detailed description.

**KICKSTART_POSTJOB**
   contains a string that starts a job to be executed conditionally
   after the main job, see above for a detailed description.

**KICKSTART_CLEANUP**
   contains a string that starts a job to be executed unconditionally
   after any of the previous jobs, see above for a detailed description.

**KICKSTART_PREPEND_PATH**
   the value of this variable is prepended to the PATH variable seen by
   Kickstart and passed to the job. The modified PATH is also used to
   look up executables for the main job and any pre/post/setup/cleanup
   jobs.

**KICKSTART_WRAPPER**
   the value of this variable is prepended to the job arguments. It can
   be used to wrap the task with a wrapper or launcher. For example, you
   can set it to "mpiexec -n 128" to run an MPI job, or you can set it
   to "tau_exec" to profile the job with TAU.

**KICKSTART_TRACE_ALL** If this variable is set, then the **-Z** option
will trace everything, including stdio and directories. By default,
stdio and directories are ignored.

**KICKSTART_TRACE_CWD** If this variable is set, then the **-Z** option
will only trace files in the current working directory of the process.

**KICKSTART_TRACE_MATCH**
   If this variable is set, then the **-Z** option will only trace files
   that match one of the patterns specified. The value of this variable
   should be a list of **fnmatch()** patterns separated by *:*.

**KICKSTART_TRACE_IGNORE** This is the inverse of
**KICKSTART_TRACE_MATCH**. Any files matching one of the patterns will
be ignored, and all other files will be traced.

**KICKSTART_METADATA** Kickstart passes this environment variable to the
job. The value of the variable is the path to the metadata file to which
the job should write its metadata. See the `METADATA <#METADATA>`__
section for more information.



History
=======

As you may have noticed, **pegasus-kickstart** had the name
**kickstart** in previous incantations. We are slowly moving to the new
name to avoid clashes in a larger OS installation setting. However,
there is no pertinent need to change the internal name, too, as no name
clashes are expected.



Notes
=====

sha256 implementation by Dr Brian Gladman, Worcester, UK. Please see
the source code at https://github.com/pegasus-isi for the full
copyright notice.

