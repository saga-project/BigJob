""" Example application demonstrating how to submit a simple 
    /bin/echo job with BigJob. 
        
"""

import os
import time
import pdb
import sys

sys.path.insert(0, os.getcwd() + "/../")

COORDINATION_URL = "redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
RESOURCEMGR_URL  = "pbs-ssh://localhost"

from bigjob import bigjob, subjob, description


### This is the number of jobs you want to run
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
    # make sure you are familiar with the queue structure on lonestar, your project id
    # and the walltime limits on each queue. change accordingly
    # 
    queue="normal"          # You can switch to development for very short runs
    project=None            # since FutureGrid is an experimental infrastrucutre. 
    walltime=60             # Time in minutes. There are limits on the time you can request. 

    processes_per_node=8   # if you want more memory per process you can reduce this to 6, 2  or 1
    number_of_processes=24  # The total number of processes you want running, depends on the value above

    # yye00: to keep things clean: create the directory agent where you intend to run the script
    # you can start by doing this in SCRATCH if you run out of space. This directory
    # will get big quickly so keep an eye out and do not put it in $HOME

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
    # Submit jobs through BigJob
    # Here you can add any arguments to each SubJob, change the ouput and error filenames and so on
    # change this to your heart's content, but be careful
    jobs = []
    job_start_times = {}
    job_states = {}
    for i in range(0, NUMBER_JOBS):
        jd = description()
        jd.executable = "/bin/echo"
        jd.number_of_processes = "4"
        jd.spmd_variation = "mpi"
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
            #print "Job " + str(jobs[i]) + " state: " + state
            if result_map.has_key(state)==False:
                result_map[state]=1
            else:
                result_map[state] = result_map[state]+1
            #pdb.set_trace()
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
