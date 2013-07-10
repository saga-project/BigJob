.. BigJob documentation master file, created by
   sphinx-quickstart on Mon Dec  3 21:55:42 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

##############################
BigJob |version| User Manual 
##############################

BigJob is a light-weight Python package that provides a Pilot-based Job and Data Management system. BigJob aims to be as flexible and extensible as possible - it installs where you want it (requiring no root access to the target machine). Unlike many other Pilot-Job systems, BigJob natively supports MPI jobs and, because of its integration with saga-python_, works on a variety of backend systems (such as SGE, PBS, SLURM, etc.). BigJob has been shown to work on grids, clouds, and clusters, as well as locally on your personal computer.

**Get Involved or Contact Us:**

+-------+-------------------------+-----------------------------------------------------+
| |Git| | **BigJob on GitHub:**   | https://github.com/saga-project/BigJob/             |
+-------+-------------------------+-----------------------------------------------------+
| |Goo| | **BigJob Mailing List:**| https://groups.google.com/forum/#!forum/bigjob-users|
+-------+-------------------------+-----------------------------------------------------+

.. |Git| image:: images/github.jpg
.. |Goo| image:: images/google.png

.. _saga-python: ttps://github.com/saga-project/saga-python/


Contents
--------

.. toctree::
   :numbered:
   :maxdepth: 2

   intro/index.rst    
   install/index.rst
   usage/index.rst
   patterns/index.rst
   library/index.rst
   tutorial/index.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`

