#############################
XSEDE Simple Ensemble Example
#############################

**Note:** If you do not have an `XSEDE account <http://www.xsede.org/>`_, please skip this tutorial.

One of the features of BigJob is the ability for application-level programmability by users. Many of the parameters in each script are customizable and configurable. There are several parameters that must be added to the PilotComputeDescription in order to run on XSEDE. 

----------------------------
:code:`service_url`
----------------------------

The service URL communicates what type of queueing system or middleware you want to use and where it is. The following table shows the machine type and the adaptor to use for that machine.

+-----------------------------+-----------------------------------------------------------------------------+ 
| Machine	              | :code:`service_url`		          				    |
+=============================+=============================================================================+ 
| All machines                |* *fork://localhost*							    |
|			      |* *ssh://your.remote.cluster*						    |
+-----------------------------+-----------------------------------------------------------------------------+ 
| Stampede		      |* **Local:** *slurm://localhost*						    |
|			      |* **Remote:** *slurm+ssh://stampede.tacc.utexas.edu*			    |
+-----------------------------+-----------------------------------------------------------------------------+ 
| Lonestar and Ranger         |* **Local:** *sge://localhost*						    |	
|			      |* **Remote (over SSH):** *sge+ssh://lonestar.tacc.utexas.edu*		    |
|			      |* **Remote (GSISSH):** *sge+gsissh://ranger.tacc.utexas.edu*                 |
+-----------------------------+-----------------------------------------------------------------------------+ 
| Trestles		      |* **Local:** *pbs://localhost*						    |
|			      |* **Remote (over SSH):** *pbs+ssh://trestles.sdsc.edu*			    |
+-----------------------------+-----------------------------------------------------------------------------+
| Kraken		      |* **Local:** *xt5torque://localhost*					    |
|			      |* **Remote (GSISSH):** *xt5torque+gsissh://gsissh.kraken.nics.xsede.org*	    |	
+-----------------------------+-----------------------------------------------------------------------------+


----------------------------
:code:`project`
----------------------------

When running on XSEDE, the project parameter must be changed to your project's allocation number. 

----------------------------
:code:`number_of_processes`
----------------------------

This refers to the number of cores used. If your machine does not have 12 cores per node, you will have to change this parameter.

----------------------------
:code:`queue`
----------------------------

This refers to the name of the queue on the submission machine. For example, two queue names on Lonestar are 'normal' and 'development'. Please refer to the machine-specific documentation to find out the names of the queues on the machines.

-------------------------------
Example PilotComputeDescription
-------------------------------

::

	pilot_compute_description = {
                         "service_url": 'slurm+ssh://stampede.tacc.utexas.edu',
                         "number_of_processes": 32,                             
                         "queue":"normal", 
                         "project":"TG-MCBXXXXXX", # if None default allocation is used 
                         "walltime":10,
                         "working_directory": os.getcwd()
                        }


----------------------------------
Simple Ensembles Stampede Example
----------------------------------

Now that we have modified the Pilot Compute Description, we can put this together with our simple ensemble pattern to build a script that executes on Stampede. Note that the PCD is the only thing that changes in this example. ::

	import os
	import time
	import sys
	from pilot import PilotComputeService, ComputeDataService, State
	
	### This is the number of jobs you want to run
	NUMBER_JOBS=4
	COORDINATION_URL = "redis://localhost:6379"

	if __name__ == "__main__":

	    pilot_compute_service = PilotComputeService(COORDINATION_URL)

	    pilot_compute_description = { 
                         			"service_url": 'slurm+ssh://stampede.tacc.utexas.edu',
                         			"number_of_processes": 32,                             
                         			"queue":"normal", 
                         			"project":"TG-MCBXXXXXX", # if None default allocation is used 
                         			"walltime":10,
                         			"working_directory": os.getcwd()
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

