""" Example application demonstrating how to submit a N 
    /bin/echo Jobs with single BigJob. 

    if number_of_processes (BigJob size) = (number_of_processes/Job) * N. In this case all Jobs are executed     
    concurrently.
    if number_of_processes (BigJob size) < (number_of_processes/Job) * N. In this case, First set of Jobs 
    which satisfy the resource requirements are processed. Unprocessed Jobs are stored in a FIFO queue. 
    As resources become available, Jobs are processed..  
"""

import os
import time
import pdb
import sys

sys.path.insert(0, os.getcwd() + "/../")

COORDINATION_URL = "redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"

#For remote machines username should be provided in the resource manager url as mentiond as above.
RESOURCEMGR_URL = "pbs-ssh://username@Target_machines_host_name"
RESOURCEMGR_URL = "pbs-ssh://pmantha@localhost"

from bigjob import bigjob, subjob, description


### This is the number of SubJobs you want to run
NUMBER_JOBS=24

def has_finished(state):
        state = state.lower()
        if state=="done" or state=="failed" or state=="canceled":
            return True
        else:
            return False


if __name__ == "__main__":

    starttime=time.time()

    ##########################################################################################
    # make sure you are familiar with the queue structure on futuregrid,ppn, your project id
    # and the walltime limits on each queue. change accordingly
    # 
    queue="normal"          # Queue to which BigJob has to be submitted, if None, default queue is considered.
    project=None            # Allocation Information. if None, default information is considered
    walltime=60             # Time in minutes. There are limits on the time you can request

    processes_per_node=8    # ppn
    number_of_processes=24  # The total number of processes ( BigJob size), used to run Jobs 
    workingdirectory= os.path.join(os.getcwd(), "agent") # working directory for agent. 
    ##########################################################################################

    print "Start Pilot Job/BigJob at: " + RESOURCEMGR_URL
    bj = bigjob(COORDINATION_URL)
    bj.start_pilot_job( RESOURCEMGR_URL,
                        None,
                        number_of_processes,
                        queue,
                        project,
                        workingdirectory,
                        None,
                        walltime,
                        processes_per_node)

    print "Pilot Job/BigJob URL: " + bj.pilot_url + " State: " + str(bj.get_state())

    ##########################################################################################
    jobs = []
    job_start_times = {}
    job_states = {}

    # Submit Jobs through BigJob
    # Here you can add any arguments to each SubJob, change the ouput and error filenames and so on
    # change this to your heart's content, but be careful

    for i in range(0, NUMBER_JOBS):
        jd = description()
        jd.executable = "/bin/echo"
        jd.number_of_processes = "4"
        jd.spmd_variation = "mpi" # for serial codes jd.spmd_variation="single"
        jd.arguments = ["$INFRASTRUCTURE"]
        jd.environment = ["INFRASTRUCTURE=FutureGrid"]
        jd.output = "sj-stdout-"+str(i)+".txt"
        jd.error = "sj-stderr-"+str(i)+".txt"

        sj = subjob()
        sj.submit_job(bj.pilot_url, jd)
        jobs.append(sj)
        job_start_times[sj]=time.time()
        job_states[sj] = sj.get_state()

    # busy wait for completion
    while 1:
        finish_counter=0
        result_map = {}
        for i in range(0, NUMBER_JOBS):
            old_state = job_states[jobs[i]]
            state = jobs[i].get_state()
            if result_map.has_key(state)==False:
                result_map[state]=1
            else:
                result_map[state] = result_map[state]+1
            if old_state != state:
                print "Job " + str(jobs[i]) + " changed from: " + old_state + " to " + state
            if old_state != state and has_finished(state)==True:
                print "Job: " + str(jobs[i]) + " Runtime: " + str(time.time()-job_start_times[jobs[i]]) + " s."
            if has_finished(state)==True:
                finish_counter = finish_counter + 1
            job_states[jobs[i]]=state

        if finish_counter == NUMBER_JOBS:
            break
        time.sleep(2)

    runtime = time.time()-starttime
    print "Runtime: " + str(runtime) + " s; Runtime per Job: " + str(runtime/NUMBER_JOBS)

    ##########################################################################################
    # Cleanup - stop BigJob
    bj.cancel()
