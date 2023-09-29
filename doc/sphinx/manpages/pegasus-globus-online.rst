.. _cli-pegasus-globus-online:

=====================
pegasus-globus-online
=====================

Interfaces with Globus Online for managed transfers.
::

      pegasus-globus-online [--mkdir]
                            [--transfer]
                            [--remove]
                            [--file inputfile]
                            [--debug]



Description
===========

**pegasus-globus-online** takes a JSON input from the pegasus-transfer
tool and executes the list by interacting with the Globus Online
service.

It assumes that the endpoints already have been activated using the web
interface. To authenticate with Globus Online, OAuth tokens must be
provided inside the JSON that defines the operation. Tokens can be
initialized with **pegasus-globus-online-init** tool.

Note that pegasus-globus-online is a tool mostly used internally in
Pegasus workflows, in particular by pegasus-transfer.



Options
=======

**--mkdir**
   The input JSON is for a mkdir request

**--transfer**
   The input JSON is for a transfer request

**--remove**
   The input JSON is for a remove request

**--file** *inputfile*
   JSON transfer specification. If not given, stdin will be used.

**--debug**
   Enables debugging output.


