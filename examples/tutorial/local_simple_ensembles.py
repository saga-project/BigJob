import os
import time
import sys
from pilot import PilotComputeService, ComputeDataService, State

### This is the number of jobs you want to run
NUMBER_JOBS=4
COORDINATION_URL = "redis://localhost:6379"

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
                "arguments": ["Hello","$ENV1","I am CU number "+str(i)]
                "environment": ['ENV1=World'],
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
