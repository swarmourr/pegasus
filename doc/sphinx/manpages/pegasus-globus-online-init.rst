.. _cli-pegasus-globus-online-init:

==========================
pegasus-globus-online-init
==========================

Initializes OAuth tokens for Globus Online authentication.
::

      pegasus-globus-online-init  [-h]
                                  [--permanent]



Description
===========

**pegasus-globus-online-init** initializes OAuth tokens, to be used with
Globus Online transfers. It redirects the user to globus website, in
order to authorize Pegasus wms to perform transfers with the user’s
Globus account. By default this tool requests tokens that cannot be
refreshed and could potentially expire within a couple of days. In order
to provide pegasus with refreshable tokens please use --permanent
option. The acquired tokens are placed in globus.conf inside .pegasus
folder of the user’s home directory.

Note this tool should be used before starting a workflow that relies on
Globus Online transfers, unless the user has initialized the tokens with
another way or has acquired refreshable tokens previously.



Options
=======

**-h**; \ **--help**
   Prints a usage summary with all the available command-line options.

**-p**; \ **--permanent**
   Requests a refresh token that can be used indefinetely. Access can be
   revoked from globus web interface (manage consents).

**-e**; \ **--endpoints**
   A list of endpoints that require data_access consent to move data to
   and from them. Access can be revoked from globus web interface.
