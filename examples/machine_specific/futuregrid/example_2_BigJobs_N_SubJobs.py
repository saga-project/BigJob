""" Example application demonstrating how to submit a N/2 
    /bin/echo jobs(SubJobs) each to two BigJobs. 
        
"""

import os
import time
import pdb
import sys
sys.path.insert(0, os.getcwd() + "/../")
COORDINATION_URL = "redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
from bigjob import bigjob, subjob, description


### This is the number of Jobs you want to run
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
    # Start 1st BigJob
    ##########################################################################################
    # make sure you are familiar with the queue structure on futuregrid,ppn, your project id
    # and the walltime limits on each queue. change accordingly
    # 

    RESOURCEMGR_URL = "pbs-ssh://username@Target_machines_host_name"
    queue"normal"          # Queue information to which BigJob has to be submitted. if None, default queue is considered.
    project=None            # Allocation information 
    walltime=30             # Time in minutes. There are limits on the time you can request. 
    processes_per_node=8    # ppn
    number_of_processes=12  # The total number of processes ( BigJob size), used to run SubJobs 
    workingdirectory= os.path.join(os.getcwd(), "agent") # working directory for agent. 
    ##########################################################################################

    print "Start Pilot Job/BigJob at: " + RESOURCEMGR_URL
    bj1 = bigjob(COORDINATION_URL)
    bj1.start_pilot_job( RESOURCEMGR_URL,
                        None,
                        number_of_processes,
                        queue,
                        project,
                        workingdirectory,
                        None,
                        walltime,
                        processes_per_node)

    print "Pilot Job/BigJob URL: " + bj1.pilot_url + " State: " + str(bj1.get_state())

    ##########################################################################################
    #Start 2nd BigJob
    ##########################################################################################
    # make sure you are familiar with the queue structure on futuregrid,ppn, your project id
    # and the walltime limits on each queue. change accordingly
    # 

    RESOURCEMGR_URL = "pbs-ssh://username@target_machine_host_name" 
    queue="normal"          # Queue information to which BigJob has to be submitted. if None, default queue is considered.
    project=None            # Allocation information. If None, default allocation information is considered. 
    walltime=30             # Time in minutes. There are limits on the time you can request. 
    processes_per_node=8    # ppn
    number_of_processes=12  # The total number of processes ( BigJob size), used to run jobs(SubJobs) 
    workingdirectory= os.path.join(os.getcwd(), "agent") # working directory for agent. 
    ##########################################################################################

    print "Start Pilot Job/BigJob at: " + RESOURCEMGR_URL
    bj2 = bigjob(COORDINATION_URL)
    bj2.start_pilot_job( RESOURCEMGR_URL,
                        None,
                        number_of_processes,
                        queue,
                        project,
                        workingdirectory,
                        None,
                        walltime,
                        processes_per_node)

    print "Pilot Job/BigJob URL: " + bj2.pilot_url + " State: " + str(bj2.get_state())

    jobs = []
    job_start_times = {}
    job_states = {}

    ##########################################################################################

    # Submit Jobs through 1st BigJob
    # Here you can add any arguments to each Job, change the ouput and error filenames and so on
    # change this to your heart's content, but be careful
    for i in range(0, NUMBER_JOBS/2):
        jd = description()
        jd.executable = "/bin/echo"
        jd.number_of_processes = "4"
        jd.spmd_variation = "mpi" # for serial codes jd.spmd_variation="single"
        jd.arguments = ["BigJob", "$BIGJOB"]
        jd.environment = ["BIGJOB=1stBigJob"]
        jd.output = "sj-stdout-"+str(i)+".txt"
        jd.error = "sj-stderr-"+str(i)+".txt"

        sj = subjob()
        sj.submit_job(bj1.pilot_url, jd)
        jobs.append(sj)
        job_start_times[sj]=time.time()
        job_states[sj] = sj.get_state()

    ##########################################################################################

    # Submit Jobs through 2nd BigJob
    # Here you can add any arguments to each Job, change the ouput and error filenames and so on
    # change this to your heart's content, but be careful

    for i in range(0, NUMBER_JOBS/2):
        jd = description()
        jd.executable = "/bin/cat"
        jd.number_of_processes = "1"
        jd.spmd_variation = "single" 
        jd.arguments = ["/etc/hosts"]
        jd.environment =[]
        jd.output = "sj-stdout-"+str(i)+".txt"
        jd.error = "sj-stderr-"+str(i)+".txt"

        sj = subjob()
        sj.submit_job(bj2.pilot_url, jd)
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
    bj1.cancel()
    bj2.cancel()
