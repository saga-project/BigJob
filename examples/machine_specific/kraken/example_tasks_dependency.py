import os
import time
import sys
import pdb

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
    queue = "small" # provide queue information
    project = "TG-XXXXXXXXXX" # provide allocation information 
    walltime = 10 # provide wall time in minutes
    size = 12 # provide total cores required
    #### Working directory should be in $SCRATCHDIR 
    workingdirectory = "/lustre/scratch/<username>/agent/"  
    userproxy = None # userproxy (not supported yet due to context issue w/ SAGA)
    #Provide resource url in the format sge-ssh://pmantha@target_machine_name
    lrms_url = "pbs-ssh://<username>@kraken-gsi.nics.teragrid.org"

    ##########################################################################################
    # Start Pilot Job/BigJob

    bj = bigjob(COORDINATION_URL)
    bj.start_pilot_job( lrms_url,
                        None,
                        size,
                        queue,
                        project,
                        workingdirectory,
                        userproxy,
                        walltime,
                        1)

    print "Pilot Job/BigJob URL: " + bj.pilot_url + " State: " + str(bj.get_state())

    ##########################################################################################
    ### Submit set of tasks T(A)
    ##########################################################################################
    for i in range(NUMBER_JOBS):
        jd = description()
        jd.executable = "/bin/echo"
        jd.number_of_processes = "4" 
        jd.spmd_variation = "mpi"
        jd.arguments = ["$INFRASTRUCTURE","$USERNAME"]
        jd.environment = ["INFRASTRUCTURE=kraken-mpi","USERNAME=test"]
        jd.output = "sj-stdout.txt"
        jd.error = "sj-stderr.txt"
        sj = subjob()
        sj.submit_job(bj.pilot_url, jd)

    ##########################################################################################
    ### Submit set of tasks T(B)
    ##########################################################################################
    for i in range(NUMBER_JOBS):
        jd = description()
        jd.executable = "/bin/echo"
        jd.number_of_processes = "1"
        jd.spmd_variation = "single"
        jd.arguments = ["$INFRASTRUCTURE","$USERNAME"]
        jd.environment = ["INFRASTRUCTURE=kraken-single","USERNAME=test"]
        jd.output = "sj-stdout.txt"
        jd.error = "sj-stderr.txt"
        sj = subjob()
        sj.submit_job(bj.pilot_url, jd)

    ########################################################################################
    # T(A) and T(B) execute parallely.....
    # wait for T(A) and T(B) to complete and then start T(C)
    bj.wait()
    runtime = time.time()-starttime
    print "Runtime: " + str(runtime) + " s; Runtime per Job: " + str(runtime/NUMBER_JOBS)

    ##########################################################################################
    ### Submit set of tasks T(C)
    ##########################################################################################
    starttime=time.time()
    for i in range(NUMBER_JOBS):
        jd = description()
        jd.executable = "/bin/date"
        jd.number_of_processes = "1"
        jd.spmd_variation = "single"
        jd.arguments = []
        jd.environment = []
        jd.output = "sj-stdout.txt"
        jd.error = "sj-stderr.txt"
        sj = subjob()
        sj.submit_job(bj.pilot_url, jd)

    ########################################################################################
    # wait for T(C) to complete
    bj.wait()
    runtime = time.time()-starttime
    print "Runtime: " + str(runtime) + " s; Runtime per Job: " + str(runtime/NUMBER_JOBS)
    ##########################################################################################

    ### The placement of bj.wait() provides dependencies between set of tasks implictly..

    # Cleanup - stop BigJob
    bj.cancel()
