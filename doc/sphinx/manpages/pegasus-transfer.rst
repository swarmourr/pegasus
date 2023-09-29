.. _cli-pegasus-transfer:

================
pegasus-transfer
================

Handles data transfers for Pegasus workflows.
   ::

      pegasus-transfer [-h]
                         [--file inputfile]
                         [--threads number_threads]
                         [--max-attempts attempts]
                         [--threads threads]
                         [--symlink]
                         [--debug]

Description
===========

**pegasus-transfer** takes a JSON defined list of urls, either through
stdin or with an input file, determines the correct tool to use for the
transfer and executes the transfer. Some of the protocols
pegasus-transfer can handle are GridFTP, SCP, SRM, Amazon S3, Google
Storage, XRootD, HTTP, Docker, Singularity, and local cp/symlinking.
Failed transfers are retried.

Note that pegasus-transfer is a tool mostly used internally in Pegasus
workflows, but the tool can be used stand alone as well.

Options
=======

**-h**; \ **--help**
   Prints a usage summary with all the available command-line options.

**-f** *FILE*; \ **--file=FILE**
   File containing URL pairs to be transferred. If not given, list is
   read from stdin.

**-m** *MAX_ATTEMPTS*; \ **--max-attempts=MAX_ATTEMPTS**
   Number of attempts allowed for each transfer. Default is 3.

**-n** *THREADS*; \ **--threads=THREADS**
   Number of threads to process transfers. Default is 8. This option can
   also be set via the PEGASUS_TRANSFER_THREADS environment variable.
   The command line option takes precedence over the environment
   variable.

**-s**; \ **--symlink**
   Allow symlinking of file URLs. If the source and destination URLs
   chosen are both file URLs with the same site_label then the source
   file will be symlinked to the destination rather than being copied.

**-d**; \ **--debug**
   Enables debugging ouput.

Example
=======

::

   $ pegasus-transfer
   [
    { "type": "transfer",
      "id": 1,
      "src_urls": [ { "site_label": "web", "url": "http://pegasus.isi.edu" } ],
      "dest_urls": [ { "site_label": "local", "url": "file:///tmp/index.html" } ]
    }
   ]
   CTRL+D


Protocols Supported
===================

pegasus-transfer currently supports the following data transfer protocols:

Amazon S3
cp/symlinking
Docker Pull
Globus Online
Google Storage
GridFTP
HPSS
HTTP/HTTPS
iRODS
SCP
Singularity Library
SRM
StashCache
WebDAV

With the exception of Globus Online and HPSS, pegasus-transfer can handle
transfers between seemingly incompatible protocols by inserting a file://
intermediary. For example, if you ask pegasus-transfer for a transfer
between Docker and S3, it will be converted to two transfers, such that:

docker:// -> file://
file:// -> s3://


Credential Handling
===================

Credentials used for transfers can be specified with a combination of
site labels in the input JSON format and environment variables. For
example, give the following input file:

::

   [
    { "type": "transfer",
      "id": 1,
      "src_urls": [ { "site_label": "isi", "url": "gsiftp://workflow.isi.edu/data/file.dat" } ],
      "dest_urls": [ { "site_label": "tacc_stampede", "url": "gsiftp://gridftp.stampede.tacc.utexas.edu/scratch/file.dat" } ]
    }
   ]

pegasus-transfer will expect either one environment variable specifying
one credential to be used on both end of the connection
(X509_USER_PROXY), or two separate environment variables specifying two
different credentials to be used on the two ends of the connection. In
the latter case, the environment variables are derived from the site
labels. In the example above, the environment variables would be named
X509_USER_PROXY_isi and X509_USER_PROXY_tacc_stampede


Threading
=========

In order to speed up data transfers, pegasus-transfer will start a set
of transfers in parallel using threads.


Retries
=======

Failed transfers are retried, with an exponential backoff between the
tries. If there are a lot of transfers failing in on attempt,
pegasus-transfer might choose to short-circuit and fail early instead
of trying all transfers multiple times.


Preference of GFAL over GUC
===========================

JGlobus is no longer actively supported and is not in compliance RFC
2818. As a result cleanup jobs using pegasus-gridftp client would fail
against the servers supporting the strict mode. We have removed the
pegasus-gridftp client and now use gfal clients as globus-url-copy does
not support removes. If gfal is not available, globus-url-copy is used
for cleanup by writing out zero bytes files instead of removing them.

If you want to force globus-url-copy to be preferred over GFAL, set the
PEGASUS_FORCE_GUC=1 environment variable in the site catalog for the
sites you want the preference to be enforced. Please note that we expect
globus-url-copy support to be completely removed in future releases of
Pegasus due to the end of life of Globus Toolkit in 2018.


Author
======

Pegasus Team http://pegasus.isi.edu
