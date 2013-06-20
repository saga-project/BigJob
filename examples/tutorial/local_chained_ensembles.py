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
                                  "walltime":10,
                                }

   pilot_compute_service.create_pilot(pilot_compute_description)

   compute_data_service = ComputeDataService()
   compute_data_service.add_pilot_compute_service(pilot_compute_service)

    print ("Finished Pilot-Job setup. Submit compute units")
    # submit Set A compute units
    all_A_cus = []
    for i in range(NUMBER_JOBS):
        compute_unit_description = { "executable": "/bin/echo",
                                     "arguments": ["Hello","$ENV1","$ENV2"],
                                     "environment": ['ENV1=env_arg1','ENV2=env_arg2'],
                                     "number_of_processes": 1,
                                     "output": "A_stdout.txt",
                                     "error": "A_stderr.txt"
                                   }
        compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)
        all_A_cus.append(compute_unit) # Store all the compute units.

    # Chaining tasks i.e submit a compute unit, when compute unit from A is successfully executed.

    while 1:
        for i in all_A_cus:
            if i.get_state() == "Done":
                compute_unit_description = { "executable": "/bin/echo",
                                             "arguments": ["$ENV1","$ENV2"],
                                             "environment": ['ENV1=task_B:','ENV2=after_task_A'+str(i)],
                                             "number_of_processes": 1,
                                             "output": "B_stdout.txt",
                                             "error": "B_stderr.txt"
                                           }
                compute_data_service.submit_compute_unit(compute_unit_description)
                all_A_cus.remove(i)

        if len(all_A_cus) == 0:
            break

    # Wait for set B jobs.
    compute_data_service.wait()

    print ("Terminate Pilot Jobs")
    compute_data_service.cancel()
    pilot_compute_service.cancel()
