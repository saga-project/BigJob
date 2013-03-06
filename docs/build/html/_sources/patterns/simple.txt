###############
Simple Ensemble
###############

You might be wondering how to create your own BigJob script or how BigJob can be useful for your needs. 

The first example, below, submits N jobs using BigJob. This is very useful if you are running many jobs using the same executable. Rather than submit each job individually to the queuing system and then wait for every job to become active and complete, you submit just one 'Big' job that reserves the number of cores needed to run all of your jobs. When this BigJob becomes active, your jobs are pulled by BigJob from the Redis server and executed. 

The below examples demonstrates the mapping of a simple job (i.e. executable is /bin/echo) using all of the parameters of a Compute Unit Description. Specifically, it shows how to run 4 jobs on your local machine using fork::

	import os
	import time
	import sys
	from pilot import PilotComputeService, ComputeDataService, State
	
	### This is the number of jobs you want to run
	NUMBER_JOBS=4
	COORDINATION_URL = "redis://localhost"

	if __name__ == "__main__":

	    pilot_compute_service = PilotComputeService(COORDINATION_URL)

	    pilot_compute_description = { "service_url": "fork://localhost",
        	                          "number_of_processes": 1,
                	                  "working_directory": os.getenv("HOME")+"/agent",
                        	          "walltime":10
                                	}

    	    pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)

    	    compute_data_service = ComputeDataService()
    	    compute_data_service.add_pilot_compute_service(pilot_compute_service)

    	    print ("Finished Pilot-Job setup. Submitting compute units")

    	    # submit compute units
    	    for i in range(NUMBER_JOBS):
        	compute_unit_description = {
                	"executable": "/bin/echo",
                	"arguments": ["Hello","$ENV1","$ENV2"],
                	"environment": ['ENV1=env_arg1','ENV2=env_arg2'],
                	"number_of_processes": 1,            
                	"spmd_variation":"single",
                	"output": "stdout.txt",
                	"error": "stderr.txt",
                	}    
        	compute_data_service.submit_compute_unit(compute_unit_description)

    	    print ("Waiting for compute units to complete")
    	    compute_data_service.wait()

    	    print ("Terminate Pilot Jobs")
    	    compute_data_service.cancel()    
    	    pilot_compute_service.cancel()