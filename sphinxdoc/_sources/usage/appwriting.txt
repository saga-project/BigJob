######################
Writing BigJob Scripts
######################

This documentation is to help provide a familiarity with the scripts and the different configurable parameters that will help you get started. It is not comprehensive. For complete API documentation, please see `Section 5 <../library/index.html>`_.

======================
BigJob Terminology
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

You can import any number of Python modules, depending on what you want to do in your script. You must import the pilot module as follows::

	from pilot import PilotComputeService, ComputeDataService, State

======================
Coordination URL
======================

Identify the coordination system to be used. You should have set up a Redis server as part of the `Setting Up a Redis Server <../install/redis.html>`_ section.

Replace the COORDINATION_URL parameter with the path to your Redis server. The following example shows how to do this for localhost:
::
	COORDINATION_URL = "redis://localhost:6379"   # uses redis database as coordination system.   

To do this for a remote resource, simply change localhost to the machine URL and verify which port redis is running on.

======================
NUMBER_JOBS
======================

The number of jobs simply defines how many jobs you wish to run. For instance, 1 Pilot-Job may be submitted to run 100 compute units (sub-jobs). In this case, the NUMBER_JOBS parameter would be set to 100. It should be noted that this is usually at the top of the script for convenience but can be added in a for loop around the Compute Unit Description.

======================
Pilot Compute Service
======================

The PilotComputeService (PCS) is a factory for creating Pilot-Compute objects, where the latter is the individual handle to the resource. The PCS takes the COORDINATION_URL (as defined above) as an argument. This is for coordination of the compute and data units with the redis database. ::

    pilot_compute_service = PilotComputeService(COORDINATION_URL)

======================
Pilot Compute Description
======================

The next step in creating your script is to define the pilot compute description (PCD). The PCD just defines the compute resource in which you will be running on and different attributes required for managing jobs on that resource. Recall that a Pilot-Job requests resources required to run all of the jobs (i.e. it's like one big job instead of many small jobs). There can be any number of pilotcompute instantiated depending on the compute resources available to the application (using two machines rather than 1 requires 2 pilot compute descriptions).

The following are the resource specifications that need to be provided:

- :code:`service_url` - Specifies the SAGA-Python job adaptor (often this is based on the batch queuing system) and resource hostname (for instance, lonestar.tacc.utexas.edu) on which jobs can be executed. For remote hosts, password-less login must be enabled. 
- :code:`number_of_processes` - This refers to the number of cores that need to be allocated to run the jobs
- :code:`project` - Specifies your allocation, if running on an XSEDE resource. This field must be removed if you are running somewhere that does not require an allocation.
- :code:`queue` - Specifies the job queue to be used. If you are not submitting to a batch queuing system, remove this parameter.
- :code:`working_directory` - Specifies the directory in which the Pilot-Job agent executes.
- :code:`wall_time_limit` - Specifies the number of minutes the resources are requested for. ::

	pilot_compute_description = { 	   "service_url": "sge+ssh://localhost",
        	                           "number_of_processes": 12,
                	                   "project": "TG-XXXXXX",
                        	           "queue": "development",
                                	   "working_directory": os.getenv("HOME")+"/agent",
                                   	   "wall_time_limit":10
                                	}

After defining a Pilot Compute Description, we tell the system to create the Pilot-Job by adding the following line::

	pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)


========================
Compute Data Service
========================

The Compute Data Service is created to handle both Pilot Compute and Pilot Data in a holistic way. It represents the central entry point for the application workload. The CDS takes care of the placement of Compute and Data Units. The set of Pilot Computes and Pilot Data available to the CDS can be changed during the application's runtime. The CDS handles different data-compute affinity and will handle compute/data co-locating for the requested data-compute workload. ::

    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)


========================
Compute Unit Description
========================

Next, we must define the actual compute unit that we want to run. These are what constitute the individual jobs that will run within the Pilot. Oftentimes, this will be an executable, which can have input arguments or environment variables.

- :code:`executable` - Specifies the path to the executable, i.e. NAMD, AMBER, etc.
- :code:`arguments`  - Specifies the list of arguments to be passed to executable. This field may not be necessary if your executable does not require input arguments. 
- :code:`environment` - Specifies the list of environment variables to be set for the successful of job execution. This field may also not be necessary depending on your application.
- :code:`working_directory` - Specifies the directory in which the job has to execute. If not specified, the Pilot-Job creates a default directory.
- :code:`number_of_processes` - Specifies the number of cores to be assigned for the job execution.
- :code:`spmd_variation` - Specifies the type of job. By default, it is single job. It can also be an MPI job.
- :code:`output` - Specifies the file in which the standard output of the job execution to be stored.
- :code:`error` - Specifies the file in which the standard error of the job execution to be stored. :: 

	compute_unit_description = { "executable": "/bin/echo",
        	                     "arguments": ["Hello","$ENV1","$ENV2"],
                	             "environment": ['ENV1=env_arg1','ENV2=env_arg2'],
                        	     "number_of_processes": 4,            
                             	     "spmd_variation":"mpi",
                             	     "output": "stdout.txt",
                             	     "error": "stderr.txt"
                           	   }    

After defining a description for the compute units, you want to submit these compute units. The number of compute units you submit depends on the NUMBER_JOBS you defined at the top of the script. You will need a :code:`for` loop in Python in order to submit the correct number of jobs. ::

	 for i in range(NUMBER_JOBS):
		compute_data_service.submit_compute_unit(compute_unit_description)

We then have the CDS wait for all of the compute units to complete and then do some clean up. ::

    print ("Waiting for compute units to complete")
    compute_data_service.wait()

    print ("Terminate Pilot Jobs")
    compute_data_service.cancel()    
    pilot_compute_service.cancel()


=======================
Working Directories
=======================

BigJob creates a unique directory for each PilotCompute instance using the unique identifier of the Pilot::

    <BIGJOB_WORKING_DIRECTORY>/bj-54aaba6c-32ec-11e1-a4e5-00264a13ca4c/
 
If no working_directory for the ComputeUnit is specified, BigJob creates a subdirectory for each ComputeUnit::

    <BIGJOB_WORKING_DIRECTORY>/bj-54aaba6c-32ec-11e1-a4e5-00264a13ca4c/sj-55010912-32ec-11e1-a4e5-00264a13ca4c
   
This subdirectory will be the current working directory of your ComputeUnit. Stdin/Stdout as well as files created by your application will be written to this directory.

=======================
Putting it all Together
=======================

Your first simple BigJob script:

.. code-block:: python

   import sys
   import os
   from pilot import PilotComputeService, ComputeDataService, State

   NUMBER_JOBS = 4
   COORDINATION_URL = "redis://localhost"

   pilot_compute_service = PilotComputeService(COORDINATION_URL)

   # create pilot job service and initiate a pilot job
   pilot_compute_description = {
                            "service_url": 'fork://localhost',
                            "number_of_processes": 1,                             
                            "working_directory": os.path.join(os.getcwd(),"work"),
                               }
   
   pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
        
   compute_data_service = ComputeDataService()
   compute_data_service.add_pilot_compute_service(pilot_compute_service)
   
   # start work unit
   compute_unit_description = {
           "executable": "/bin/date",
           "arguments": [""],
           "number_of_processes": 1,            
           "output": "stdout.txt",
           "error": "stderr.txt",   
   }   
               
   for i in range(NUMBER_JOBS):                                                                                                
   	compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)
   
   compute_data_service.wait()

   compute_data_service.cancel()


Make sure that the working directory of your Pilot exists; create a directory called `work` if necessary.
