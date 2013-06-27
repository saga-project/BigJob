###########################
Remote Pilot Data Example
###########################

This example illustrates how to move the files (contained in a DU) associated with your compute workload from one machine to another machine using the concept of "Pilot Data." This is achieved by creating two Pilot Data entities and moving the output DU from the local Pilot Data entity to the remote Pilot Data entity.

As an example, we are running a computational workload on Stampede, an XSEDE machine (see :code:`pilot_compute_description`). We want our input Data Unit to be available on Stampede, because our Compute Unit (which requires the file in the input Data Unit) runs on Stampede. The CU needs the input DU to be transferred to the compute node where the computation will be done. However, we want our output data to be available on india, a FutureGrid machine. Note that we have set up our ssh keys to allow for password-less login to :code:`india.futuregrid.org` from :code:`stampede.tacc.utexas.edu`.

Be sure to change the :code:`project` field to your allocation on the machine you are running on. You will also need to change the :code:`service_url` to your home directory on india. You may also wish to create a folder to contain the DUs such as "pilotdata". This is not a necessary step, but is a suggested step. Your output on india.futuregrid.org will be available in the directory you specify, in this case, :code:`/N/u/<your username>/pilotdata` in subdirectories that start with :code:`DU-<uid>` where uid is a unique identifier to your output data. 

.. code-block:: python

	import sys
	import os
	import time
	import logging
	from pilot import PilotComputeService, PilotDataService,ComputeDataService, State

	COORDINATION_URL = "redis://localhost:6379"

	if __name__ == "__main__":

	    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)

	    # create pilot job service and initiate a pilot job
	    pilot_compute_description = {
        	                     "service_url": 'slurm+ssh://stampede.tacc.utexas.edu',
                	             "number_of_processes": 32,
                        	     "working_directory": os.getenv("HOME")+"/agent",
                             	     "queue": "normal",
                       		     "project": 'YourAllocation',	#CHANGE ME	
                             	     "walltime":10
                                	}

	    pilotjob = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
	    # create pilot data service (factory for data pilots (physical,distributed storage))
	    # and pilot data
	    pilot_data_service = PilotDataService(coordination_url=COORDINATION_URL)

	   #Define our local Pilot Data entity
	    pilot_data_description_1={
        	                        "service_url":"ssh://localhost"+os.getenv("HOME")+"/pilotdata/",
                	                "size":100,
                        	    }

	    #Define our remote Pilot Data entity on india.futuregrid.org
    	     pilot_data_description_2={
                            		"service_url":"ssh://india.futuregrid.org/N/u/<your username>/pilotdata", # CHANGE ME
                            		"size":100,
    		}

	    ps1 = pilot_data_service.create_pilot(pilot_data_description=pilot_data_description_1)
	    ps2 = pilot_data_service.create_pilot(pilot_data_description=pilot_data_description_2)

	    compute_data_service = ComputeDataService()
	    compute_data_service.add_pilot_compute_service(pilot_compute_service)
	    compute_data_service.add_pilot_data_service(pilot_data_service)



	    input_data_unit_description = { "file_urls": ["ssh://localhost" +os.path.join(os.getcwd(), "test.txt")],
        	                          }

	   # submit pilot data to a pilot store
	   # Note that we are using the Local Pilot Data storage
	   input_du = ps1.submit_data_unit(input_data_unit_description)
    	   input_du.wait()


    	    ### Create Output DU to store the output files.
   	    output_data_unit_description = { "file_urls": [],
                                   	   }

	   # submit pilot data to a pilot store
	   # Note that we are using the Remote Data storage
	   output_du = ps2.submit_data_unit(output_data_unit_description)
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
	    # output_du.export(os.getcwd())

	   logging.debug("Terminate Pilot Compute/Data Service")
 	   compute_data_service.cancel()
	   pilot_data_service.cancel()
 	   pilot_compute_service.cancel()

