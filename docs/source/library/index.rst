#################
Library Reference
#################

.. pilot/impl/pilot_manager.py defines:
..    class ComputeDataService
..
.. pilot/impl/pilotdata_manager.py defines
..    class PilotData
..    class PilotDataService
..    class DataUnit
..
.. pilot/impl/pilotcompute_manager.py defines
..    class PilotCompute
..    class PilotComputeService
..    class ComputeUnit

.. pilot/api/compute/api.py defines
..    class PilotComputeDescription 
..    class State

.. pilot/api/data/api.py defines
..    class PilotDataDescription

Compute and Data Services
*************************

This section is meant to provide a hierarchical overview of the various library components and their interaction. The subsections then provide the API details associated with each component.

The main concepts and classes exposed by the Compute part of the API are:

* **PilotCompute (PC):** a pilot job, which can execute some compute workload (ComputeUnit).
* **PilotComputeDescription (PCD):** description for specifying the requirements of a PilotCompute.
* **PilotComputeService (PCS):** a factory for creating \PilotComputes.

The data side of the Pilot API is symmetric to the compute side. The exposed classes for managing Pilot Data are:

* **PilotData (PD):** a pilot that manages some data workload (DataUnit)
* **PilotDataDescription (PDD):** a abstract description of the requirements of the PD
* **PilotDataService (PDS):** a factory (service) which can create PilotDatas according to some specification

The application workload is represented by so called ComputeUnits and DataUnits:

* **ComputeUnit (CU):** a work item executed on a PilotCompute.
* **DataUnit (DU):** a data item managed by a PilotData

Both Compute and Data Units are specified using an abstract description object:

* **ComputeUnitDescription (CUD):** abstract description of a ComputeUnit.
* **DataUnitDescription (DUD):** abstract description of a DataUnit.

The ComputeDataService represents the central entry point for the application workload:

* **ComputeDataService (CDS):** a service which can map CUs and DUs to a set of Pilot Compute and Pilot Data. The ComputeDataService (CDS) takes care of the placement of Compute and Data Units. The set of PilotComputes and PilotData available to the CDS can be changed during the application's runtime. The CDS different data-compute affinity and will handle compute/data co-locating for the requested data-compute workload.

PilotComputeService
===================

The PilotComputeService (PCS) is a factory for creating Pilot-Compute objects, where the latter is the individual handle to the resource. The PCS takes the COORDINATION_URL (as defined above) as an argument. This is for coordination of the compute units with the redis database.

.. autoclass:: pilot.impl.pilotcompute_manager.PilotComputeService
   :members:


PilotComputeDescription
=======================

The PCD defines the compute resource on which the Pilot agent will be started . Recall that a Pilot-Job requests resources required to run all of the Compute Units (subjobs). There can be any number of Pilot-Computes instantiated depending on the compute resources available to the application (using two machines rather than 1 requires 2 PilotComputeDescriptions).

An example of a Pilot Compute Description is shown below::

     pilot_compute_description = {
                    "service_url": 'pbs+ssh://india.futuregrid.org',
                    "number_of_processes": 8,
                    "processes_per_node":8,                     
                    "working_directory": "/N/u/<username>",
                    'affinity_datacenter_label': "us-east-indiana",                                 'affinity_machine_label': "india" 
                   }

.. class:: PilotComputeDescription

.. data:: service_url
	
	Specifies the SAGA-Python job adaptor (often this is based on the batch queuing system) and resource hostname (for instance, `pbs+ssh://lonestar.tacc.utexas.edu`) on which jobs can be executed.

:type: string

.. data:: number_of_processes


	The number of cores that need to be allocated to run the jobs.

:type: string

.. data:: processes_per_node


	*Optional.* The number of cores per node to be requested from the resource management system.

:type: string

   	.. note:: This argument does not limit the number of processes that can run on a node! This field is required by some XSEDE/Torque clusters. If you have to specify a `ppn` parameter (e.g.`-lnodes=1:ppn=8`) in your `qsub` script, you must need this field in your BigJob script.

.. data:: working_directory

	The directory in which the Pilot-Job agent executes

	:type: string

.. data:: project 

	*Optional.* The project allocation, if running on an XSEDE resource.
 
	:type: string

   	.. note:: This field must be removed if you are running somewhere that does not require an allocation.

.. data:: queue 

	*Optional.* The job queue to be used.

	:type: string

   	.. note:: If you are not submitting to a batch queuing system, remove this parameter.

:type: string

   	.. note::  For remote hosts, password-less login must be enabled.

.. data:: wall_time_limit

	*Optional.* The number of minutes the resources are requested for. Required for some resources (e.g. on TACC machines).

:type: string

.. data:: affinity_datacenter_label

	*Optional.* The data center label used for affinity topology. 

:type: string

	.. note:: Data centers and machines are organized in a logical topology tree (similar to the tree spawned by an DNS topology). The further the distance between two resources, the smaller their affinity.

.. data:: affinity_machine_label

	*Optional.* The machine (resource) label used for affinity topology. 

:type: string

   	.. note:: Data centers and machines are organized in a logical topology tree (similar to the tree spawned by an DNS topology). The further the distance between two resources, the smaller their affinity.




PilotCompute
============
A pilot job, which can execute some compute workload (ComputeUnit).

This is the object that is returned by the PilotComputeService when a new PilotCompute is created based on a PilotComputeDescription.

The PilotCompute object can be used by the application to keep track of active pilots.

A PilotCompute has state, can be queried, and cancelled.

.. autoclass:: pilot.impl.pilotcompute_manager.PilotCompute
   :members:


PilotDataService
================

The PilotDataService (PDS) is a factory for creating Pilot-Data objects. The PDS takes the COORDINATION_URL as an argument. This is for coordination of the data units with the redis database.


.. autoclass:: pilot.impl.pilotdata_manager.PilotDataService
   :members:


PilotDataDescription
=======================
PilotDataDescription objects are used to describe the requirements for a 
:class:`~pilot.impl.pilotdata_manager.PilotData` instance. Currently, the only
generic property that can be set is :data:`size`, all other properties are 
backend-specific security / authentication hints. Example::

	pilot_data_service = PilotDataService(COORDINATION_URL)
    pilot_data_description =    {
                                   'service_url': "ssh://localhost/tmp/pilotdata/",
                               }
    pilot_data = service.create_pilot(pilot_data_description)

.. data:: service_url

    Specifies the file adaptor and resource hostname on which a Pilot-Data will be created. Supported schemes:
	
	* SSH: `ssh://localhost/tmp/pilotdata/` (Password-less login and password-less private key required)
	* iRODS: `irods://gw68/${OSG_DATA}/osg/irods/<username>/?vo=osg&resource-group=osgGridFtpGroup`
	* Globus Online: `go://<user>:<password>@globusonline.org?ep=xsede#lonestar4&path=/work/01131/tg804093/pilot-data-go`
	* Google Storage: `gs://google.com`
	* Amazon S3: `s3://aws.amazon.com`
	* Eucalyptus Walrus: `walrus://<endpoint-ip>`

    :type: string

.. data:: size 

   *Optional.* The storage space required (in Megabyte) on the storage resource. 

   :type: int

   .. note:: The 'size' attribute is not supported by all PilotData backends.

.. data:: userkey

    *For SSH backend.* The SSH private key (for SSH backend). **Attention: This key is put into the Redis service in order to make it available at the Pilot agent. Use with caution and not with your production keys. Do not use with shared Redis server!** The SSH key delegation mechanism is designed for resources where the worker nodes are not directly accessible to install the private key manually (e.g. OSG).

    :type: string

    .. note:: 'userkey' is only supported by backends where worker nodes need private key access. An example of this is OSG.

.. data:: access_key_id

    *For S3/Walrus backend.* The 'username' for Amazon AWS compliant instances. It is an alphanumeric text string that uniquely identifies a user who owns an account. No two accounts can have the same access key.

    :type: string

    .. note:: 'access_key_id' is only supported by AWS complaint EC2 based connections. This applies to Amazon AWS, Eucalpytus, and OpenStack. Please see Amazon's documentation to learn how to obtain your access key id and password.

.. data:: secret_access_key

    *For S3/Walrus backend.* The 'password' for Amazon AWS compliant instances. It's called secret because it is assumed to be known to the owner only. 

    :type: string

    .. note:: 'secret_access_key' is only supported by  AWS complaint EC2 based connections. This applies to Amazon AWS, Eucalpytus, and OpenStack. Please see Amazon's documentation to learn how to obtain your access key id and password.



PilotData
=========
A Pilot-Data, which can store some data (DataUnit).

This is the object that is returned by the PilotDataService when a new PilotData is created based on a PilotDataDescription.

The PilotData object can be used by the application to keep track of active pilots. 

.. autoclass:: pilot.impl.pilotdata_manager.PilotData
   :members:

ComputeDataService
==================

The Compute Data Service is created to handle both Pilot Compute and Pilot Data entities in a holistic way. It represents the central entry point for the application workload. The CDS takes care of the placement of Compute and Data Units. The set of Pilot Computes and Pilot Data available to the CDS can be changed during the application's runtime. The CDS handles different data-compute affinity and will handle compute/data co-locating for the requested data-compute workload.

.. autoclass:: pilot.impl.pilot_manager.ComputeDataService
   :members:


Compute and Data Units
**********************

ComputeUnitDescription
=======================

The ComputeUnitDescription defines the actual compute unit will be run. The executable specified here is what constitutes the individual jobs that will run within the Pilot. This executable can have input arguments or environment variables that must be passed with it in order for it to execute properly.

Example::

           compute_unit_description = {
                   "executable": "/bin/cat",
                   "arguments": ["test.txt"],
                   "number_of_processes": 1,
                   "output": "stdout.txt",
                   "error": "stderr.txt",   
				   "environment": ["MY_SCRATCH_DIR=/tmp"],				   
                   "input_data" : [data_unit.get_url()], # this stages the content of the data unit to the working directory of the compute unit
                   "affinity_datacenter_label": "eu-de-south",              
                   "affinity_machine_label": "mymachine-1" 
               }     


.. class:: ComputeUnitDescription

.. data:: executable

Specifies the path to the executable that will be run

    :type: string

.. data:: arguments

Specifies any arguments that the executable needs. For instance, if running an executable from the command line requires a -p flag, then this -p flag can be added in this section.

    :type: string

.. data:: environment

Specifies any environment variables that need to be passed with the compute unit in order for the executable to work, e.g ["MY_SCRATCH_DIR=/tmp"],				   

    :type: string

.. data:: working_directory

The working directory for the executable

    :type: string

	.. note:: Recommendation: Do not set the working directory! If none, working directory is a sandbox directory of the CU (automatically created by BigJob)

.. data:: input

Specifies the capture of <stdin>

    :type: string

.. data:: output

Specifies the name of the file who captures the output from <stdout>. Default is stdout.txt

    :type: string

.. data:: error

Specifies the name of the file who captures the output from <stderr>. Default is stderr.txt

    :type: string

.. data:: number_of_processes

Defines how many CPU cores are reserved for the application process. 

For instance, if you need 4 cores for 1 MPI executable, this value would be 4.

    :type: string

.. data:: spmd_variation

Defines how the application process is launched. Valid strings for this field are 'single' or 'mpi'. If your executable is :code:`a.out`, "single" executes it as :code:`./a.out`, while "mpi" executes :code:`mpirun -np <number_of_processes> ./a.out` (note: :code:`aprun` is used for Kraken, and :code:`srun/ibrun` is used for Stampede).

    :type: string

.. data:: input_data

Specifies the input data flow for a ComputeUnit. This is used in conjunction with PilotData. The format is :code:`[<data unit url>, … ]`

    :type: string

.. data:: output_data

Specifies the output data flow for a ComputeUnit. This is used in conjunction with PilotData. The format is :code:`[<data unit url>, … ]`

    :type: string

.. data:: affinity_datacenter_label

The data center label used for affinity topology. 

:type: string

	.. note:: Data centers and machines are organized in a logical topology tree (similar to the tree spawned by an DNS topology). The further the distance between two resources, the smaller their affinity.

.. data:: affinity_machine_label

The machine (resource) label used for affinity topology. 

:type: string

   	.. note:: Data centers and machines are organized in a logical topology tree (similar to the tree spawned by an DNS topology). The further the distance between two resources, the smaller their affinity.

ComputeUnitDescription objects are loosely typed. A dictionary containing the respective keys can be passed instead to the ComputeDataService.

ComputeUnit
===========
A ComputeUnit is a work item executed by a PilotCompute. These are what constitute the individual jobs that will run within the Pilot. Oftentimes, this will be an executable, which can have input arguments or environment variables. 

A ComputeUnit is the object that is returned by the ComputeDataService when a new ComputeUnit is submitted based on a ComputeUnitDescription. The ComputeUnit object can be used by the application to keep track of ComputeUnits that are active.

A ComputeUnit has state, can be queried, and can be cancelled. 

.. autoclass:: pilot.impl.pilotcompute_manager.ComputeUnit
   :members:


DataUnitDescription
=======================

The data unit description defines the different files to be moved around. There is currently no support for directories. ::

	data_unit_description = {
        	                        'file_urls': [file1, file2, file3]
                	        }

.. class:: DataUnitDescription

.. data:: file_urls

:type: string

.. data:: affinity_datacenter_label

The data center label used for affinity topology. 

:type: string

	.. note:: Data centers and machines are organized in a logical topology tree (similar to the tree spawned by an DNS topology). The further the distance between two resources, the smaller their affinity.

.. data:: affinity_machine_label

The machine (resource) label used for affinity topology. 

:type: string

   	.. note:: Data centers and machines are organized in a logical topology tree (similar to the tree spawned by an DNS topology). The further the distance between two resources, the smaller their affinity.


DataUnit
========
A DataUnit is a container for a logical group of data that is often accessed together or comprises a larger set of data; e.g. a data file or chunk.  

A DataUnit is the object that is returned by the ComputeDataService when a new DataUnit is submitted based on a DataUnitDescription. The DataUnit object can be used by the application to keep track of DataUnits that are active.

A DataUnit has state, can be queried, and can be cancelled.

.. autoclass:: pilot.impl.pilotdata_manager.DataUnit
   :members:


State Enumeration
******************

Pilots and Compute Units can have state. These states can be queried using the :code:`get_state()` function. States are used for PilotCompute, PilotData, ComputeUnit, DataUnit and ComputeDataService. The following table describes the values that states can have.

.. class:: State

.. cssclass:: table-hover
+------------------------------+
| **State**                    |
+------------------------------+
| .. data:: Unknown='Unknown'  |
+------------------------------+
| .. data:: New='New' 	       |
+------------------------------+
| .. data:: Running=`Running'  |
+------------------------------+
| .. data:: Done=`Done'	       | 
+------------------------------+
| .. data:: Canceled=`Canceled'|
+------------------------------+
| .. data:: Failed=`Failed'    | 
+------------------------------+
| .. data:: Pending=`Pending'  |
+------------------------------+