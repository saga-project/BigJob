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

Some propper discussion what compute and data services are and how they are supposed to
be used needs to go here. The 'aggregate' ComputeDataService needs to be explained here
as well! And examples! Examples are always good.


PilotComputeService
===================
.. warning:: DOCUMENT_ME 

.. autoclass:: pilot.impl.pilotcompute_manager.PilotComputeService
   :members:

PilotComputeDescription
=======================
.. warning:: DOCUMENT_ME

.. class:: PilotComputeDescription

.. data:: affinity_datacenter_label

   .. warning:: DOCUMENT_ME

   :type: string

.. data:: affinity_machine_label

   .. warning:: DOCUMENT_ME

   :type: string

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

   .. warning:: DOCUMENT_ME

   :type: string

.. data:: processes_per_host

   .. warning:: DOCUMENT_ME

   :type: string

.. data:: project

   .. warning:: DOCUMENT_ME

   :type: string

.. data:: queue

   .. warning:: DOCUMENT_ME

   :type: string

.. data:: wall_time_limit

   .. warning:: DOCUMENT_ME

   :type: string

.. data:: working_directory

   .. warning:: DOCUMENT_ME

   :type: string


PilotCompute
============
.. warning:: DOCUMENT_ME 

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


