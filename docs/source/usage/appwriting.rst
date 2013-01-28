######################
Writing BigJob Scripts
######################

This documentation is to help provide a familiarity with the scripts and the different configurable parameters that will help you get started. It is not comprehensive. For complete API documentation, please see `Section 5 <../library/index.html>`_.

======================
BigJob Jargon
======================

Familiarity with the below terms will help you to understand the overview of BigJob functionality.

#. **Application** - A program that is composed of computation and data elements

#. **Pilot-Job (Pilot)** - An entity that actually gets submitted and scheduled on a resource using the resource management system. A Pilot-Job provides application-level control and management of a set of allocated resources. A container for a set of tasks. Allows the logical grouping of compute units (see below)

#. **Compute Unit** - A self-containing piece of work, e.g. a computational task that potentially operates on a set of input data, also an 'application kernel'

#. **Pilot-Data** - Allows the logical grouping of data units (see below). This collection of files can be associated with an extensible set of properties.

#. **Data Unit** - A container for a logical group of data that is often accessed together or comprises a larger set of data, e.g. data files or chunks

#. **Pilot-Manager (PM)** - Stores the information of the compute units and is responsible for orchestrating the interactions between the Pilots

#. **Pilot-Agent** - For each HPC resource specified, a Pilot-Agent is launched. When a resource becomes available, the Pilot-Agent becomes active and pulls the stored information of the compute unit and executes it on that HPC resource.

#. **Coordination System** - A database used by the Pilot-Manager to store the information of Compute Units and orchestrate Pilot-Agents. Active Pilot-Agents use the coordination system to pull the Compute Unit information in order to execute such Compute Units on HPC resources.

======================
Import Python Modules
======================

You can import any number of Python modules, depending on what you want to do in your script. We recommend the following::

	import os
	import time
	import sys
	from pilot import PilotComputeService, ComputeDataService, State

======================
Coordination URL
======================

Identify the coordination system to be used. You should have set up a Redis server as part of the `Setting Up a Redis Server <../install/redis.html>`_ section.

Replace the COORDINATION_URL parameter with the path to your Redis server. The following example shows how to do this for localhost and a remote resource, such as cyder::

	COORDINATION_URL = "redis://localhost:6379"   # uses redis database as coordination system.   
::

	COORDINATION_URL = "redis://cyder.cct.lsu.edu:2525"  # uses redis database on cyder.cct.lsu.edu at port 2525 as coordination system. 

======================
Pilot Compute Description
======================

Pilot description defines the resource specification for managing the jobs on that resource. The following are the resource specifications that need to be provided:

- :code:`service_url` - Specifies the SAGA-Python job adaptor and resource hostname on which jobs can be executed. For remote hosts, password-less login must be enabled. 
- :code:`number_of_processes` - Specifies the total number of processes that need to be allocated to run the jobs.
- :code:`allocation` - Specifies your allocation, if running on an XSEDE resource. This field can be left empty if you are running somewhere that does not require an allocation.
- :code:`queue` - Specifies the job queue to be used. If you are not submitting to a batch queuing system, remove this parameter.
- :code:`working_directory` - Specifies the directory in which the Pilot-Job agent executes.
- :code:`walltime` - Specifies the number of minutes the resources are requested for. ::

	pilot_compute_description = { "service_url": "sge+ssh://localhost",
        	                           "number_of_processes": 12,
                	                   "allocation": "XSEDE12-SAGA",
                        	           "queue": "development",
                                	   "working_directory": os.getenv("HOME")+"/agent",
                                   	   "walltime":10
                                	}

======================
Compute Unit Description
======================

- :code:`executable` - specifies the executable. 
- :code:`arguments`  - specifies the list of arguments to be passed to executable.
- :code:`environment` - specifies the list of environment variables to be set for the successful of job execution.
- :code:`working_directory` - specifies the directory in which the job has to execute. If not specified, the Pilot-Job creates a default directory.
- :code:`number_of_processes` - specifies the number of processes to be assigned for the job execution.
- :code:`spmd_variation` - specifies the type of job. By default it is single job.
- :code:`output` - specifies the file in which the standard output of the job execution to be stored.
- :code:`error` - specifies the file in which the standard error of the job execution to be stored. :: 

	compute_unit_description = { "executable": "/bin/echo",
        	                     "arguments": ["Hello","$ENV1","$ENV2"],
                	             "environment": ['ENV1=env_arg1','ENV2=env_arg2'],
                        	     "number_of_processes": 4,            
                             	     "spmd_variation":"mpi",
                             	     "output": "stdout.txt",
                             	     "error": "stderr.txt"
                           	   }    


======================
Pilot Data Description
======================

======================
Data Unit Description
======================