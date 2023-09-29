.. _cli-pegasus-db-admin:

================
pegasus-db-admin
================

Manage Pegasus databases.

   ::

      pegasus-db-admin COMMAND [options] [DATABASE_URL]



Description
===========

**pegasus-db-admin** is used to manage Pegasus databases. The tool can
operate directly over a database URL, or can read configuration
parameters from a properties file or a submit directory. In the latter,
a database type should be provided to indicate which properties
should be used to connect to the database. For example, the tool will
seek for pegasus.catalog.replica.db.\* properties to connect to the
JDBCRC database; or seek for pegasus.catalog.master.url (or
pegasus.dashboard.output, which is deprecated) property to connect to
the MASTER database; or seek for the pegasus.catalog.workflow.url (or
pegasus.monitord.output, which is deprecated) property to connect to the
WORKFLOW database. If none of these properties are found, the tool will
connect to the default database.

The **pegasus-db-admin** tool should always be followed by a **COMMAND**
listed below. To see the available options for each command, please use
the **-h** option after the command. For example: **pegasus-db-admin
update -h**



Commands
========

**create DATABASE_URL**
   Creates Pegasus databases from new or empty databases, or updates
   current database to the latest version. If a database already exists,
   it will create a backup of the current database in the database folder.
   Pegasus databases can be created by 1) passing a database URL, 2) from the
   properties file, and 3) from the submit directory. Note that if the
   properties file or the submit directory is used, a database type
   (JDBCRC, MASTER, or WORKFLOW) should be provided.

**update [-a] [-V] DATABASE_URL**
   Updates the database to the latest or a given Pegasus version
   provided with the **-V** or **--version** option. If a database
   already exists, it will create a backup of the current
   database in the database folder. The **-a** or **--all** option will
   also update databases from completed workflows in the MASTER database.

**downgrade [-a] [-V] DATABASE_URL**
   Downgrades the database to the previous or a given Pegasus version
   provided with the **-V** or **--version** option. If a database
   already exists, it will create a backup of the current database in
   the database folder. The **-a** or **--all** option will also downgrade
   databases from completed workflows in the MASTER database.

**check [-V] [-e] DATABASE_URL**
   Verifies whether the database is updated to the latest or a given Pegasus
   version provided with the **-V** or **--version** option.

**version [-V] [-e] DATABASE_URL**
   Prints the current version of the database.



Global Options
==============

**-h**; \ **--help**
   Prints a usage summary with all the available command-line options.

**-c CONFIG_PROPERTIES**; \ **--conf=CONFIG_PROPERTIES**
   Specifies the properties file. This overrides all other property
   files. Should be used with *-t*.

**-s SUBMIT_DIR**; \ **--submitdir=SUBMIT_DIR**
   Specifies the submit directory. Should be used with *-t*.

**-t DB_TYPE**; \ **--type=DB_TYPE**
   Type of the database (JDBCRC, MASTER, or WORKFLOW). Should be used
   with *-c* or *-s*.

**-D PROPERTIES**
   Commandline overwrite for properties. Must be in the *prop=val*
   format.

**-d**; \ **--debug**
   Enables debugging.



Update and Downgrade Options
============================

**-a**; \ **--all**
   Update/Downgrade all databases of completed workflows in MASTER.

**-V PEGASUS_VERSION**; \ **--version=PEGASUS_VERSION**
   Pegasus version that the database will be updated/downgraded to.



Check and Version Options
=========================

**-V PEGASUS_VERSION**; \ **--version=PEGASUS_VERSION**
   Pegasus version that the database will be updated/downgraded to.

**-e**; \ **--version-value**
   Show actual version values (an integer number).



Database Upgrades From Pegasus 4.5.X to Pegasus current version
===============================================================

Databases will be automatically updated when **pegasus-plan** is
invoked, but WORKFLOW databases from past runs may not be updated
accordingly. The pegasus-db-admin tool provides an option to
automatically update all databases from completed workflows in
the MASTER database. To enable this option, run the following command:

::

   $ pegasus-db-admin update -a
   Your database has been updated.
   Your database is compatible with Pegasus version: 5.0.0

   Verifying and updating workflow databases:
   21/21

   Summary:
   Verified/Updated: 21/21
   Failed: 0/21
   Unable to connect: 0/21
   Unable to update (active workflows): 0/21

   Log files:
   20161006T134415-dbadmin.out (Succeeded operations)
   20161006T134415-dbadmin.err (Failed operations)

This option generates a log file for succeeded operations, and a log
file for failed operations. Each file contains the list of URLs of the
succeeded/failed databases.


Examples
========

::

   # Create a database by passing a database URL.
   $ pegasus-db-admin create sqlite:///${HOME}/.pegasus/workflow.db
   $ pegasus-db-admin create mysql://localhost:3306/pegasus

   # Create a database from the properties file. Note that a database
   # type should be provided.
   $ pegasus-db-admin create -c pegasus.properties -t MASTER
   $ pegasus-db-admin create -c pegasus.properties -t JDBCRC
   $ pegasus-db-admin create -c pegasus.properties -t WORKFLOW

   # Create a database from the submit directory. Note that a database
   # type should be provided.
   $ pegasus-db-admin update -s /path/to/submitdir -t WORKFLOW
   $ pegasus-db-admin update -s /path/to/submitdir -t MASTER
   $ pegasus-db-admin update -s /path/to/submitdir -t JDBCRC

   # Update the database schema by passing a database URL.
   $ pegasus-db-admin update sqlite:///${HOME}/.pegasus/workflow.db
   $ pegasus-db-admin update mysql://localhost:3306/pegasus

   # Update the database schema from the properties file. Note that a
   # database type should be provided.
   $ pegasus-db-admin update -c pegasus.properties -t MASTER
   $ pegasus-db-admin update -c pegasus.properties -t JDBCRC
   $ pegasus-db-admin update -c pegasus.properties -t WORKFLOW

   # Update the database schema from the submit directory. Note that a
   # database type should be provided.
   $ pegasus-db-admin update -s /path/to/submitdir -t WORKFLOW
   $ pegasus-db-admin update -s /path/to/submitdir -t MASTER
   $ pegasus-db-admin update -s /path/to/submitdir -t JDBCRC



Troubleshooting
===============

**Error 2013: Lost connection to MySQL server during query when dumping
table**

When updating MySQL databases, pegasus-db-admin uses **mysqldump** to
create a backup .sql file for the current database. For very large
databases, the dump may fail due to timeout limits of the MySQL database
(which are set to 30 seconds for read, and 60 seconds for write). You
can change these limits in the **my.cnf** config file by setting the
following configuration parameters (the values below are only an
example, you should adjust them as you may like):

::

   net_read_timeout = 120
   net_write_timeout = 900

After making these changes to my.cnf you must restart MySQL.



