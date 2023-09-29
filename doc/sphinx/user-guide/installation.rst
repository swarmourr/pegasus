.. _installation:

============
Installation
============

The preferred way to install Pegasus is with native (RPM/DEB) packages.
It is recommended that you also install HTCondor (formerly Condor)
(`yum <http://research.cs.wisc.edu/htcondor/yum/>`__,
`debian <http://research.cs.wisc.edu/htcondor/debian/>`__) from native
packages.

.. _prereqs:

Prerequisites
=============

Pegasus has a few dependencies:

-  **Java 1.8 or higher**. Check with:

   ::

      # java -version
      java version "1.8.0"

-  **Python 3.5 or higher**. Check with:

   ::

      # python3 --version
      Python 3.6.9

   Python3 package ``yaml`` is also required, but it
   will be installed automatically if you use the RPM/DEB packages.

-  **HTCondor (formerly Condor) 9.0 or higher**. See
   http://www.cs.wisc.edu/htcondor/ for more information. You should be
   able to run ``condor_q`` and ``condor_status``.

.. _optional:

Optional Software
=================

-  **mysqlclient**. Python module for MySQL access. Only needed if you
   want to store the runtime database in MySQL (default is SQLite).
   On server side, MySQL server 5.7 or higher is required, as Pegasus
   creates databases encoded in *utf8mb4* charset.

-  **psycopg2**. Python module for PostgreSQL access. Only needed if you
   want to store the runtime database in PostgreSQL (default is SQLite)

.. _env:

Environment
===========

To use Pegasus, you need to have the pegasus-\* tools in your PATH. If
you have installed Pegasus from RPM/DEB packages. the tools will be in
the default PATH, in /usr/bin. If you have installed Pegasus from binary
tarballs or source, add the bin/ directory to your PATH.

Example for bourne shells:

::

   $ export PATH=/some/install/pegasus-5.0.X/bin:$PATH

..

If you want to use the API to generate workflows (:ref:`api-reference`), you might also need to set your PYTHONPATH, PERL5LIB, or CLASSPATH. The right setting can be found by using pegasus-config:

::

   $ export PYTHONPATH=`pegasus-config --python`
   $ export PERL5LIB=`pegasus-config --perl`
   $ export CLASSPATH=`pegasus-config --classpath`

.. _rhel:

RHEL / CentOS / Rocky / Alma / SL
=================================

Binary packages provided for: RHEL 7 x86_64 and RHEL 8 x86_64 (including OSes
derived from RHEL: CentOS, Rocky, AlmaLinux, SL)

.. tabs::

   .. code-tab:: bash CentOS 9

      curl --output /etc/yum.repos.d/pegasus.repo \
            https://download.pegasus.isi.edu/pegasus/rhel/9/pegasus.repo
      dnf install epel-release
      dnf install --enablerepo devel pegasus

   .. code-tab:: bash CentOS 8

      curl --output /etc/yum.repos.d/pegasus.repo \
            https://download.pegasus.isi.edu/pegasus/rhel/8/pegasus.repo
      dnf install epel-release
      dnf install --enablerepo powertools pegasus

   .. code-tab:: bash CentOS 7

      curl --output /etc/yum.repos.d/pegasus.repo \
            https://download.pegasus.isi.edu/pegasus/rhel/7/pegasus.repo
      yum install epel-release
      yum install pegasus


Ubuntu
======

.. tabs::

   .. code-tab:: bash 22.04 LTS (Jammy Jellyfish)

      curl https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -
      echo 'deb https://download.pegasus.isi.edu/pegasus/ubuntu jammy main' >/etc/apt/sources.list.d/pegasus.list
      apt-get update
      apt-get install pegasus

   .. code-tab:: bash 20.04 LTS (Focal Fossa)

      curl https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -
      echo 'deb https://download.pegasus.isi.edu/pegasus/ubuntu focal main' >/etc/apt/sources.list.d/pegasus.list
      apt-get update
      apt-get install pegasus

   .. code-tab:: bash 18.04 LTS (Bionic Beaver)

      curl https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -
      echo 'deb https://download.pegasus.isi.edu/pegasus/ubuntu bionic main' >/etc/apt/sources.list.d/pegasus.list
      apt-get update
      apt-get install pegasus


Debian
======

.. tabs::

   .. code-tab:: bash Debian 11 (Bullseye)

      wget -O - https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -
      echo 'deb https://download.pegasus.isi.edu/pegasus/debian bullseye main' >/etc/apt/sources.list.d/pegasus.list
      apt-get update
      apt-get install pegasus

   .. code-tab:: bash Debian 10 (Buster)

      wget -O - https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -
      echo 'deb https://download.pegasus.isi.edu/pegasus/debian buster main' >/etc/apt/sources.list.d/pegasus.list
      apt-get update
      apt-get install pegasus


.. _macosx:

Mac OS X
========

The easiest way to install Pegasus on Mac OS is to use Homebrew. You
will need to install XCode and the XCode command-line tools, as well as
Homebrew. Then you just need to tap the Pegasus tools repository and
install Pegasus and HTCondor like this:

::

   $ brew tap pegasus-isi/tools
   $ brew install pegasus htcondor


Once the installation is complete, you need to start the HTCondor
service. The easiest way to do that is to use the Homebrew services tap:

::

   $ brew tap homebrew/services
   $ brew services list
   $ brew services start htcondor

You can also stop HTCondor like this:

::

   $ brew services stop htcondor

And you can uninstall Pegasus and HTCondor using ``brew rm`` like this:

::

   $ brew rm pegasus htcondor

..

.. note::

   It is also possible to install the latest development versions of
   Pegasus using the ``--HEAD`` arguments to
   ``brew install``, like this: ``$ brew install --HEAD pegasus``

.. _tarballs:

Pegasus from Tarballs
=====================

The Pegasus prebuild tarballs can be downloaded from the `Pegasus
Download Page <https://pegasus.isi.edu/downloads>`__.

Use these tarballs if you already have HTCondor installed or prefer to
keep the HTCondor installation separate from the Pegasus installation.

-  Untar the tarball

   ::

      $ tar zxf pegasus-*.tar.gz

-  include the Pegasus bin directory in your PATH

   ::

      $ export PATH=/path/to/pegasus-install/bin:$PATH

-  If you do not already have the Python3 package ``yaml``,
   and ``GitPython``, you can create a virtual environment.
   For example:

   ::

      $ python3 -m venv ~/pegasus-env
      $ . ~/pegasus-env/bin/activate
      $ python3 -m pip install pyyaml GitPython


.. _pypi-packages:

Pegasus Python Packages for PyPi
================================

- To install the new Pegasus API.

   ::

      $ pip install pegasus-wms.api


- To install old Python DAX API. **The old DAX API is deprecated and will be
  removed in Pegasus 5.1.0.**

   ::

      $ pip install pegasus-wms.dax

