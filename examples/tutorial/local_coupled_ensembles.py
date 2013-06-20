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

    pilot_compute_service.create_pilot(pilot_compute_description)

    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)

    print ("Finished Pilot-Job setup. Submitting compute units")

    # submit a set of CUs, call it A
    for i in range(NUMBER_JOBS):
        compute_unit_description = { "executable": "/bin/echo",
                		     "arguments": ["Hello","$ENV1","I am CU number "+str(i)],
                		     "environment": ['ENV1=World'],
				     "number_of_processes": 1,
                                     "spmd_variation":"single",
                                     "output": "A"+str(i)+"_stdout.txt",
                                     "error": "A"+str(i)+"_stderr.txt"
                                    }

        compute_data_service.submit_compute_unit(compute_unit_description)


# submit a set of CUs, call it B
for i in range(NUMBER_JOBS):
        compute_unit_description = { "executable": "/bin/date",
                                     "arguments": [],
                                     "environment": {},
                                     "spmd_variation":"single",
                                     "number_of_processes": 1,
                                     "output": "B"+str(i)+"_stdout.txt",
                                     "error": "B"+str(i)+"_stderr.txt",
                                   }
        compute_data_service.submit_compute_unit(compute_unit_description)

print ("Wait for CUs of task set A & B to complete")
compute_data_service.wait()

# submit a set of CUs, call it C
for i in range(NUMBER_JOBS):
        compute_unit_description = { "executable": "/bin/echo",
                                     "arguments": ["Hello","$ENV1","$ENV2"],
                                     "environment": ['ENV1=env_arg1','ENV2=env_arg2'],
                                     "number_of_processes": 1,
                                     "spmd_variation":"single",
                                     "output": "C"+str(i)+"_stdout.txt",
                                     "error": "C"+str(i)+"_stderr.txt",
                                    }
        compute_data_service.submit_compute_unit(compute_unit_description)

print ("Wait for CUs of task set C to complete")
compute_data_service.wait()

print ("Terminate Pilot Jobs")
compute_data_service.cancel()
pilot_compute_service.cancel()
