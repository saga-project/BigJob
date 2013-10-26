#############################
Introduction to Pilot-Data
#############################

Suppose we are running an application that needs input data and generates output data. The Pilot-API provides a way to effectively manage the data flow between the application and its associated data (Compute Units [CUs] and Data Units [DUs]). This means you can stage data in and out of the application.

A CU can have both input and output dependencies to a set of DUs. For this purpose, the API declares two fields: :code:`input_data` and :code:`output_data` that can be populated with a reference to a DU. The runtime system will ensure that these dependencies are met when the CU is executed, i. e. either the DUs are moved to a Pilot that is close to the CU or the CU is executed in a Pilot close to the DU's pilot. The input data is made available in the working directory of the CU. As described, depending on the locality of the DUs/CUs, different costs can be associated with this operation. The runtime system relies on an affinity-aware scheduler that ensures that data movements are minimized and that if possible “affine” CUs and DUs in order to co-locate (affinity).

**What is Affinity?**

Affinity describes the relationship between compute and data units. It is used to try to co-locate data and compute resources as close to each other as possible. It is an optional parameter.

There are two ways of defining affinity using the Pilot API: :code:`affinity_datacenter_label` and :code:`affinity_machine_label`. As the names suggest, the affinity data center label is used to affine machines in the same data center (i.e. for Amazon S3, you can try to have the storage and compute occur in us-east-1) and the affinity machine label is used to affine units to a certain resource. A logical topology tree is used (similar to the tree spawned by a DNS topology). The further the distance between two resources, the smaller their affinity.

======================
Pilot Data Service
======================

The PilotDataService (PDS) is a factory for creating Pilot-Data objects, where the latter is an individual handle to the storage resource. The PDS takes the COORDINATION_URL as an argument. This is for the coordination of data units with the redis database. ::

	pilot_data_service = PilotDataService(COORDINATION_URL)

======================
Pilot Data Description
======================

The Pilot Data Description (PDS) simply defines the storage resource in which you will be moving the data from or to. 

The following are the resource specifications that need to be provided:

- :code:`service_url` - Specifies the file-adaptor and storage hostname
- :code:`size` - Specifies the storage space required (in MB) on the storage resource

The following affinity parameters are optional:

- :code:`affinity_datacenter_label` - The data center label used for affinity topology. Pilot stores sharing the same label are located in the same data center 
- :code:`affinity_machine_label` - The machine label used for affinity topology. Pilot stores sharing the same label are located on the same machine

Some backends require specific keys to be passed to the resource (i.e. Amazon S3 storage requires a access key and password). Parameters for passing these keys are provided in the `Library Documentation <../library/index.html>`_.
::

	pilot_data_description =    {
   								'service_url': "ssh://localhost/tmp/pilotstore/",
								}

======================
Data Unit Description
======================

The data unit description defines the different files to be moved around.

::

	data_unit_description = {
       					'file_urls': [file1, file2, file3]        
   	 			} 



======================
Staging
======================

The process of moving the data to the compute or from the compute elsewhere is called staging. There are two types of staging: input staging (e.g. copy the input data to where the compute is executing) or output staging (e.g. copy the output of an executable back to a central location). 

-----------------------
Input Staging
-----------------------

How do we stage the contents of each data unit to the input? The content of the Data-Unit referenced in the :code:`input_data` field will be moved to the working directory of the Compute Unit.

First, we define our Pilot-Data using a Pilot-Data Description as follows::

    pilot_data_description={
                                "service_url":"ssh://localhost/"+os.getenv("HOME")+"/pilotdata",                               
                           }

Next, we define the description for our input data unit (note that we use the affinity labels that match the Pilot-Data in this case)::

    input_data_unit_description = { 
									"file_urls": ["ssh://localhost" +os.path.join(os.getcwd(), "test.txt")],
									}

    input_du = compute_data_service.submit_data_unit(input_data_unit_description)

Finally, we modify our Compute Unit Description. The following stages the content of the input data unit into the working directory of the compute unit::

    compute_unit_description = {
            "executable": "/bin/cat",
            "arguments": ["test.txt"],
            "number_of_processes": 1,
            "output": "stdout.txt",
            "error": "stderr.txt",
            "input_data" : [input_du.get_url()]
    }

-----------------------
Output Staging
-----------------------

To stage the output of your executable somewhere, the following process applies:

#. Create a Pilot-Data at the location where you want to move the files to
#. Create an empty Data-Unit and bind it to your Pilot-Data. A Data-Unit is a logical container for a set of data; while a Pilot-Data is a physical store for a set of DUs. That means that you can simply create another DU in the Pilot-Data where your input DU resides.

There is no need to repeat the pilot data description, since in this example, we are going to stage the data back to the same location.

We must define an output data unit description. Note that we leave the field file_urls blank, since we are manipulated output data::

    output_data_unit_description = { "file_urls": [],
                                     "affinity_datacenter_label":input_du.data_unit_description['affinity_datacenter_label'],
                                     "affinity_machine_label":input_du.data_unit_description['affinity_machine_label']}

    output_du = compute_data_service.submit_data_unit(output_data_unit_description)

Next, we modify our Compute Unit Description for the :code:`output_data`. The following stages all output data beginning with std* (wildcards are accepted) back to the Pilot-Data described in the PDD (*ssh://localhost/"+os.getenv("HOME")+"/pilotdata*). This means the output and error files (defined as stdout.txt and stderr.txt) will be staged back to the pilot data location. ::

    compute_unit_description = {
            "executable": "/bin/cat",
            "arguments": ["test.txt"],
            "number_of_processes": 1,
            "output": "stdout.txt",
            "error": "stderr.txt",
            "output_data":[{ output_du.get_url(): ['std*']} ]
    }

========================
Putting it All Together
========================

The following script combines input and output staging. Input DUs and output DUs utilize the same Pilot-Data for storage. This can be modified in your own scripts by making two Pilot-Datas. ::

	import sys
	import os
	import time
	import logging
	from pilot import PilotComputeService, PilotDataService,ComputeDataService, State

	COORDINATION_URL = "redis://localhost"

	if __name__ == "__main__":

	    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)

	    # create pilot job service and initiate a pilot job
	    pilot_compute_description = {
        	                     "service_url": 'pbs://localhost',
                	             "number_of_processes": 12,
                        	     "working_directory": os.getenv("HOME")+"/agent",
                            	     "walltime":10,
                             	     'affinity_datacenter_label':"eu-de-south",
                             	     'affinity_machine_label': "mymachine-1"
                             		}

	    pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
	    # create pilot data service (factory for data pilots (physical,distributed storage))
	    # and pilot data
	    pilot_data_service = PilotDataService(coordination_url=COORDINATION_URL)
	    pilot_data_description={
	                                "service_url":"ssh://localhost/"+os.getenv("HOME")+"/pilotdata",
 	                               "size": 100,
        	                       "affinity_datacenter_label":"eu-de-south",
                                       "affinity_machine_label":"mymachine-1"
                       		   }
   
	    ps = pilot_data_service.create_pilot(pilot_data_description=pilot_data_description)

    	    compute_data_service = ComputeDataService()
    	    compute_data_service.add_pilot_compute_service(pilot_compute_service)
    	    compute_data_service.add_pilot_data_service(pilot_data_service)

	    input_data_unit_description = { "file_urls": ["ssh://localhost" +os.path.join(os.getcwd(), "test.txt")],
         	                           "affinity_datacenter_label":"eu-de-south",
                	                    "affinity_machine_label": "mymachine-1"}

 	   # submit pilot data to a pilot store
	    input_du = compute_data_service.submit_data_unit(input_data_unit_description)
	    input_du.wait()


	    ### Create Output DU to store the output files.
	    output_data_unit_description = { "file_urls": [],
        	                             "affinity_datacenter_label":input_du.data_unit_description['affinity_datacenter_label'],
                	                     "affinity_machine_label":input_du.data_unit_description['affinity_machine_label']}

 	   # submit pilot data to a pilot store
	    output_du = compute_data_service.submit_data_unit(output_data_unit_description)
	    output_du.wait()


	    # start compute unit
	    compute_unit_description = {
        	    "executable": "/bin/cat",
            	    "arguments": ["test.txt"],
            	    "number_of_processes": 1,
            	    "output": "stdout.txt",
            	    "error": "stderr.txt",
            	    "input_data" : [input_du.get_url()], # this stages the content of the data unit to the working directory of the compute unit
            	    "output_data":[{ output_du.get_url(): ['std*']} ]
    	    }

	    compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)

	    logging.debug("Finished setup of PSS and PDS. Waiting for scheduling of PD")
	    compute_data_service.wait()

	    ## export the output files to local directory.
	    output_du.export(os.getcwd())

	    logging.debug("Terminate Pilot Compute/Data Service")
	    compute_data_service.cancel()
	    pilot_data_service.cancel()
	    pilot_compute_service.cancel()

