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
..    class 


Compute and Data Services
*************************

PilotComputeService
===================

The PilotComputeService (PCS) is a factory for creating Pilot-Compute objects, where the latter is the individual handle to the resource. The PCS takes the COORDINATION_URL (as defined above) as an argument. This is for coordination of the compute units with the redis database.

.. autoclass:: pilot.impl.pilotcompute_manager.PilotComputeService
   :members:

PilotComputeDescription
=======================

The PCD defines the compute resource in which you will be running on and different attributes required for managing jobs on that resource. Recall that a Pilot-Job requests resources required to run all of the jobs. There can be any number of Pilot-Computes instantiated depending on the compute resources available to the application (using two machines rather than 1 requires 2 pilot compute descriptions).

.. class:: PilotComputeDescription

.. data:: affinity_datacenter_label

   The data center label used for affinity topology.

   :type: string

   .. note:: Data centers and machines are organized in a logical topology tree (similar to the tree spawned by an DNS topology). The further the distance between two resources, the smaller their affinity.

.. data:: affinity_machine_label

   The machine (resource) label used for affinity topology. 

   :type: string

   .. note:: Data centers and machines are organized in a logical topology tree (similar to the tree spawned by an DNS topology). The further the distance between two resources, the smaller their affinity.

.. data:: file_transfer

   .. warning:: DOCUMENT_ME

   :type: string

.. data:: input

   .. warning:: DOCUMENT_ME

   :type: string

.. data:: output

   Controls the location of the Pilot-Agent standard output file.

   :type: string

.. data:: error

   Controls the location of the Pilot-Agent standard error file.

   :type: string

.. data:: number_of_processes

	The number of cores that need to be allocated to run the jobs

   :type: string

.. data:: processes_per_host

   	The number of cores per host node. 

   :type: string

   .. note:: This field is required by some XSEDE/Torque clusters. If you have to specify ppn when running an MPI job on command line, then you must likely need this field in your BigJob script.

.. data:: project

   	The project allocation, if running on an XSEDE resource.
 
   :type: string

   .. note:: This field must be removed if you are running somewhere that does not require an allocation.

.. data:: queue

	The job queue to be used.

   :type: string

   .. note:: If you are not submitting to a batch queuing system, remove this parameter.

.. data:: service_url
	
	Specifies the SAGA-Python job adaptor (often this is based on the batch queuing system) and resource hostname (for instance, lonestar.tacc.utexas.edu) on which jobs can be executed.

   .. note::  For remote hosts, password-less login must be enabled.

.. data:: wall_time_limit

	The number of minutes the resources are requested for

   :type: string

.. data:: working_directory

	The directory in which the Pilot-Job agent executes

   :type: string


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

    PilotDataDescription pdd()
    pdd.size = 100

    data_pilot = service.create_pilot(pdd)

.. data:: size 

   The storage space required (in Megabyte) on the storage resource. 

   :type: int

   .. note:: The 'size' attribute is not supported by all PilotData backends.

.. data:: userkey

    The SSH private key -- this is required by some systems by the Pilot-Data in order to ensure that the SSH service can be accessed from worker nodes.

    :type: string

    .. note:: 'userkey' is only supported by backends where worker nodes need private key access. An example of this is OSG.

.. data:: access_key_id

    The 'username' for Amazon AWS compliant instances. It is an alphanumeric text string that uniquely identifies a user who owns an account. No two accounts can have the same access key.

    :type: string

    .. note:: 'access_key_id' is only supported by AWS complaint EC2 based connections. This applies to Amazon AWS, Eucalpytus, and OpenStack. Please see Amazon's documentation to learn how to obtain your access key id and password.

.. data:: secret_access_key

    The 'password' for Amazon AWS compliant instances. It's called secret because it is assumed to be known to the owner only. 

    :type: string

    .. note:: 'secret_access_key' is only supported by  AWS complaint EC2 based connections. This applies to Amazon AWS, Eucalpytus, and OpenStack. Please see Amazon's documentation to learn how to obtain your access key id and password.

.. data:: service_url

    Specifies the file adaptor and resource hostname on which a Pilot-Data will be created.

    :type: string

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

PilotComputeDescription
=======================
The PilotComputeDescription defines the compute resource in which you will be running on and different attributes required for managing jobs on that resource. Recall that a Pilot-Job requests resources required to run all of the jobs (i.e. it's like one big job instead of many small jobs). There can be any number of pilotcompute instantiated depending on the compute resources available to the application (using two machines rather than 1 requires 2 pilot compute descriptions).

.. autoclass:: pilot.impl.pilotcompute_manager.PilotComputeDescription
   :members:

ComputeUnit
===========
A ComputeUnit is a work item executed by a PilotCompute. These are what constitute the individual jobs that will run within the Pilot. Oftentimes, this will be an executable, which can have input arguments or environment variables. 

A ComputeUnit is the object that is returned by the ComputeDataService when a new ComputeUnit is submitted based on a ComputeUnitDescription. The ComputeUnit object can be used by the application to keep track of ComputeUnits that are active.

A ComputeUnit has state, can be queried, and can be cancelled. 

.. autoclass:: pilot.impl.pilotcompute_manager.ComputeUnit
   :members:

PilotDataDescription
====================
.. warning:: DOCUMENT_ME 

.. autoclass:: pilot.impl.pilotdata_manager.PilotDataDescription
   :members:

DataUnit
========
.. warning:: DOCUMENT_ME 

.. autoclass:: pilot.impl.pilotdata_manager.DataUnit
   :members:


