import os
import time
import sys

### COORDINATION_URL to point to advert server.  
COORDINATION_URL = "redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
from bigjob import bigjob, subjob, description

### EDIT based on the number of jobs you want to submit
NUMBER_JOBS=8

### BigJob main script#######
if __name__ == "__main__":
    starttime=time.time()

    ##########################################################################################
    # Edit parameters for BigJob
    queue = "workq" # provide queue information
    project = "loni_xxxxxx" # provide allocation information
    walltime = 10 # provide wall time in minutes
    processes_per_node = 4 # provide ppn
    required_processes = 8 # provide total processes required
    workingdirectory = "/home/username/agent" # provide working directory 
    userproxy = None # userproxy (not supported yet due to context issue w/ SAGA)
    lrms_url = "gram://username@eric1.loni.org/jobmanager-pbs" 

    ##########################################################################################
    # Start Pilot Job/BigJob

    bj = bigjob(COORDINATION_URL)
    bj.start_pilot_job( lrms_url,
                        None,
                        required_processes,
                        queue,
                        project,
                        workingdirectory,
                        userproxy,
                        walltime,
                        processes_per_node)

    print "Pilot Job/BigJob URL: " + bj.pilot_url + " State: " + str(bj.get_state())

    ##########################################################################################
    # Submit SubJob through BigJob
    for i in range(0, NUMBER_JOBS):
        jd = description()
        jd.executable = "/bin/echo"
        jd.number_of_processes = "4"
        jd.spmd_variation = "mpi" # single for serial jobs
        jd.environment=["NAME=user"]
        jd.arguments = ["hello","$NAME"]
        jd.output = "sj-stdout-"+str(i)+".txt"
        jd.error = "sj-stderr-"+str(i)+".txt"
        sj = subjob()
        sj.submit_job(bj.pilot_url, jd)

    ########################################################################################
    # wait for all jobs to complete
    bj.wait()

    runtime = time.time()-starttime
    print "Runtime: " + str(runtime) + " s; Runtime per Job: " + str(runtime/NUMBER_JOBS)


    ##########################################################################################
    # Cleanup - stop BigJob
    bj.cancel()
