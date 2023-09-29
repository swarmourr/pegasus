.. _cli-pegasus-keg:

===========
pegasus-keg
===========

kanonical executable for grids

   ::

      pegasus-keg [-a appname] [-t interval |-T interval] [-l logname]
                  [-P prefix] [-o fn [..]] [-i fn [..]] [-G sz [..]] [-m memory]
                  [-C] [-e env [..]] [-p parm [..]] [-u data_unit]



Description
===========

The kanonical executable is a stand-in for regular binaries in a DAG -
but not for their arguments. It allows to trace the shape of the
execution of a DAG, and thus is an aid to debugging DAG related issues.

Key feature of **pegasus-keg** is that it can copy any number of input
files, including the *generator* case, to any number of output files,
including the *datasink* case. In addition, it protocols the IPv4 and
hostname of the host it ran upon, the current timestamp, and the run
time from start til the point of logging the information, the current
working directory and some information on the system environment.
**pegasus-keg** will also report all input files, the current output
files and any requested string and environment value.

The workflow of the Keg tool is as follows: - if **-m** - allocate a
memory buffer of the specified amount - if **-i** - read all input files
into the memory buffer - if **-o** - write either the input files
content (or a generated content if **-G**) to output files - if **-T** -
generate CPU load for the specified time period decreased by the time
period spent on IO stuff; if the IO stuff time period exceeds the time
period specified here the program exits with code status 3 - if **-t** -
wait/sleep for the specified time period decreased by time periods spent
on IO stuff (and CPU load generating if any); if the time period spent
on previous activities exceeds the amount specified here the program
exits with code status 3 - if **-l** - write info to the specified log
file.



Arguments
=========

The **-e**, **-i**, **-o**, **-p** and **-G** arguments allow lists with
arbitrary number of arguments. These options may also occur repeatedly
on the command line. The file options may be provided with the special
filename - to indicate *stdout* in append mode for writing, or *stdin*
for reading. The **-a**, **-l** , **-P** , **-T** and **-t** arguments
should only occur a single time with a single argument.

If **pegasus-keg** is called without any arguments, it will display its
usage and exit with success.

**-a appname**
   This option allows **pegasus-keg** to display a different name as its
   applications. This mode of operation is useful in make-believe mode.
   The default is the basename of *argv[0]*.

**-e env [..]**
   This option names any number of environment variables, whose value
   should be reported as part of the data dump. By default, no
   environment variables are reported.

**-i infile [..]**
   The **pegasus-keg** binary can work on any number of input files. For
   each output file, every input file will be opened, and its content
   copied to the output file. Textual input files are assumed. Each
   input line is indented by two spaces. The input file content is
   bracketed between an start and end section, see below. By default,
   **pegasus-keg** operates in *generator* mode.

**-l logfile**
   The *logfile* is the name of a file to append atomically the
   self-info, see below. The atomic write guarantees that the multi-line
   information will not interleave with other processes that
   simultaneously write to the same file. The default is not to use any
   log file.

**-o outfile [..]**
   The **pegasus-keg** can work on any number of output files. For each
   output file, every input file will be opened, and its content copied
   to the output file. Textual input files are assumed. Each input line
   is indented by two spaces. The input file content is bracketed
   between an start and end section, see 2nd example. After all input
   files are copied, the data dump from this instance of **pegasus-keg**
   is appended to the output file. Without output files, **pegasus-keg**
   operates in *data sink* mode. Accept also
   *<filename>=<filesize><data_unit>* form, where <data_unit> is a
   character supported by the **-u** switch.

**-G size [..]**
   If you want **pegasus-keg** to generate a lot of output, the
   generator option will do that for you. Just specify how much, in
   bytes (but you can change it with **-u** switch), you want. You can
   specify more than 1 value here if you specify more than 1 output
   file. Subsequent values specified here will correspond to sizes of
   subsequent output files. This option is off by default.

**-u data_unit**
   By default, the output data generator (the **-G** switch) generates
   the specified amount of data in Bytes. You can alter this behavior
   with this switch. It accepts one of the following characters as
   *data_unit* value: B for Bytes, K for KiloBytes, M for MegaBytes, and
   G for GigaBytes.

**-C**
   This option causes **pegasus-keg** to list all environment variables
   that start with the prefix *\\_CONDOR* The option is useful, if .B
   pegasus-keg is run as (part of) a Condor job. This option is off by
   default.

**-p string [..]**
   Any number of parameters can be reported, without being specific on
   their content. Effectively, these strings are copied straight from
   the command line. By default, no extra arguments are shown.

**-P prefix**
   Each line from every input file is indented with a prefix string to
   visually emphasize the provenance of an input files through multiple
   instances of **pegasus-keg**. By default, two spaces are used as
   prefix string.

**-t interval**
   The interval is an amount of sleep time that the **pegasus-keg**
   executable is to sleep in seconds. This can be used to emulate light
   work without straining the pool resources. If used together with the
   **-T** spin option, the sleep interval comes before the spin
   interval. The default is no sleep time.

**-T interval**
   The interval is an amount of busy spin time that the **pegasus-keg**
   executable is to simulate intense computation in seconds. The
   simulation is done by random julia set calculations. This option can
   be used to emulate an intense work to strain pool resources. If used
   together with the **-t** sleep option, the sleep interval comes
   before the spin interval. The default is no spin time.

**-m memory**
   The amount of memory ([MB]) the Keg process should use. This option
   can be used to emulated application’s memory requirements. The
   default is not to allocate anything.



Return Value
============

Execution as planned will return 0. The failure to open an input file
will return 1, the failure to open an output file, including the log
file, will return with exit code 2. If the time spent on IO exceeds the
specified time CPU load period with **-T** or the time spent on IO and
CPU load exceeds the specified wall time with **-T** the return code
will be 3.



Example
=======

The example shows the bracketing of an input file, and the copy produced
on the output file. For illustration purposes, the output file is
connected to *stdout* :

::

   $ date > xx
   $ pegasus-keg -i xx -p a b c -o -
   --- start xx ----
     Thu May  5 10:55:45 PDT 2011
   --- final xx ----
   Timestamp Today: 20110505T105552.910-07:00 (1304618152.910;0.000)
   Applicationname: pegasus-keg [3661M] @ 128.9.xxx.xxx (xxx.isi.edu)
   Current Workdir: /opt/pegasus/default/bin/pegasus-keg
   Systemenvironm.: x86_64-Linux 2.6.18-238.9.1.el5
   Processor Info.: 4 x Intel(R) Core(TM) i5 CPU         750  @ 2.67GHz @ 2660.068
   Load Averages  : 0.298 0.135 0.104
   Memory Usage MB: 11970 total, 8089 free, 0 shared, 695 buffered
   Swap Usage   MB: 12299 total, 12299 free
   Filesystem Info: /                        ext3    62GB total,    20GB avail
   Filesystem Info: /lfs/balefire            ext4  1694GB total,  1485GB avail
   Filesystem Info: /boot                    ext2   493MB total,   447MB avail
   Output Filename: -
   Input Filenames: xx
   Other Arguments: a b c



Restrictions
============

The input file must be textual files. The behaviour with binary files is
unspecified.

The host address is determined from the primary interface. If there is
no active interface besides loopback, the host address will default to
0.0.0.0. If the host address is within a *virtual private network*
address range, only *(VPN)* will be displayed as hostname, and no
reverse address lookup will be attempted.

The *processor info* line is only available on Linux systems. The line
will be missing on other operating systems. Its information is assuming
symmetrical multi processing, reflecting the CPU name and speed of the
last CPU available in */dev/cpuinfo* .

There is a limit of *4 \* page size* to the output buffer of things that
.B pegasus-keg can report in its self-info dump. There is no such
restriction on the input to output file copy.


