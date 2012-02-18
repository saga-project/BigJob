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
    queue = "normal" # if None default queue is used
    project = "TG-MCB090174" # if None default allocation is used 
    walltime = 100
    tasks_per_node = 16
    total_cores = 32
    workingdirectory = "/share/home/01539/pmantha"
    userproxy = None # userproxy (not supported yet due to context issue w/ SAGA)
    lrms_url = "sge-ssh://login3.ranger.tacc.utexas.edu" 

    ##########################################################################################
    # Start Pilot Job/BigJob

    bj = bigjob(COORDINATION_URL)
    bj.start_pilot_job( lrms_url,
                        None,
                        total_cores,
                        queue,
                        project,
                        workingdirectory,
                        userproxy,
                        walltime,
                        tasks_per_node)

    print "Pilot Job/BigJob URL: " + bj.pilot_url + " State: " + str(bj.get_state())

    ##########################################################################################
    # Submit SubJob through BigJob
    for i in range(0, NUMBER_JOBS):
        jd = description()
        jd.executable = "/bin/date"
        jd.number_of_processes = "4"
        jd.spmd_variation = "mpi"
        jd.arguments = [""]
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
