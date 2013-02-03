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

Some proper discussion what compute and data services are and how they are supposed to
be used needs to go here. The 'aggregate' 
ComputeDataService needs to be explained here
as well! And examples! Examples are always good.


PilotComputeService
===================

The PilotComputeService (PCS) is a factory for creating Pilot-Compute objects, where the latter is the individual handle to the resource. The PCS takes the COORDINATION_URL (as defined above) as an argument. This is for coordination of the compute and data units with the redis database.

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

   .. warning:: DOCUMENT_ME

   :type: string

.. data:: error

   .. warning:: DOCUMENT_ME

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

A PilotCompute has state, can be queried and cancelled.

.. autoclass:: pilot.impl.pilotcompute_manager.PilotCompute
   :members:

PilotDataService
================
.. warning:: DOCUMENT_ME 

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

    DESCRIBE_ME

    :type: string

    .. note:: 'userkey' is only supported by ...

.. data:: access_key_id

    DESCRIBE_ME

    :type: string

    .. note:: 'access_key_id' is only supported by ...

.. data:: secret_access_key

    DESCRIBE_ME

    :type: string

    .. note:: 'secret_access_key' is only supported by ...

.. data:: service_url

    DESCRIBE_ME

    :type: string

    .. note:: 'service_url' is only supported by ...

PilotData
=========
.. warning:: DOCUMENT_ME 

.. autoclass:: pilot.impl.pilotdata_manager.PilotData
   :members:

ComputeDataService
==================
.. warning:: DOCUMENT_ME 

.. autoclass:: pilot.impl.pilot_manager.ComputeDataService
   :members:

Compute and Data Units
**********************

PilotComputeDescription
=======================
.. warning:: DOCUMENT_ME 

.. autoclass:: pilot.impl.pilotcompute_manager.PilotComputeDescription
   :members:

ComputeUnit
===========
.. warning:: DOCUMENT_ME 

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


