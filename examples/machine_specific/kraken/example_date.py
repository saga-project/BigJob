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
    project = "TG-XXXXXXX" # provide allocation information 
    walltime = 10 # provide wall time in minutes
    size = 12 # provide total cores required
    #### Working directory should be in $SCRATCHDIR 
    workingdirectory = "/lustre/scratch/pmantha/agent/"  
    userproxy = None # userproxy (not supported yet due to context issue w/ SAGA)
    #Provide resource url in the format sge-ssh://username@target_machine_name
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
    # Submit SubJob through BigJob
    for i in range(NUMBER_JOBS):
        jd = description()
        jd.executable = "/bin/date"
        jd.number_of_processes = "1" # > 1 for MPI jobs
        jd.spmd_variation = "single"
        jd.arguments = [""]
        jd.output = "sj-stdout"+str(i)+".txt"
        jd.error = "sj-stderr"+str(i)+".txt"
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
