""" Example application demonstrating job submission via bigjob 
"""

import saga
import os
import time
# BigJob implementation can be swapped here by importing another implementation,
# e.g. condor, cloud, azure
import sys
sys.path.insert(0, os.getcwd() + "/../")

# configuration
""" This variable defines the coordination system that is used by BigJob
    e.g. 
        advert://localhost (SAGA/Advert SQLITE)
        advert://advert.cct.lsu.edu:8080 (SAGA/Advert POSTGRESQL)
        advert://advert.cct.lsu.edu:5432 (SAGA/Advert POSTGRESQL)
        redis://localhost:6379 (Redis at localhost)
        tcp://localhost (ZMQ)
        tcp://* (ZMQ - listening to all interfaces)
"""

### EDIT COORDINATION_URL to point to advert server.  
COORDINATION_URL = "advert://advert.cct.lsu.edu:5432/"
#COORDINATION_URL = "advert://advert.cct.lsu.edu:8080/"

from bigjob.bigjob_manager import bigjob, subjob


### EDIT based on the number of jobs you want to submit
NUMBER_JOBS=8

def has_finished(state):
        state = state.lower()
        if state=="done" or state=="failed" or state=="canceled":
            return True
        else:
            return False


""" Test Job Submission via Advert """
if __name__ == "__main__":

    starttime=time.time()

    ##########################################################################################
    # Edit parameters for BigJob
    queue=None # if None default queue is used
    project=None # if None default allocation is used 
    walltime=10
    processes_per_node=4
    number_of_processes = 8
    workingdirectory=os.getcwd() +"/agent"  # working directory for agent
    userproxy = None # userproxy (not supported yet due to context issue w/ SAGA)

    #lrms_url = "fork://localhost" # resource url to run the jobs on localhost
    lrms_url = "gram://eric1.loni.org/jobmanager-pbs" # globus resource url used when globus is used. (LONI)
    #lrms_url = "PBSPro://localhost" # pbspro resource url used when pbspro scheduling system is used.(Futuregrid or LSU Machines)
    #lrms_url = "xt5torque://localhost" # torque resource url 

    ##########################################################################################

    print "Start Pilot Job/BigJob at: " + lrms_url
    bj = bigjob(COORDINATION_URL)
    bj.start_pilot_job( lrms_url,
                        None,
                        number_of_processes,
                        queue,
                        project,
                        workingdirectory,
                        userproxy,
                        walltime,
                        processes_per_node)

    print "Pilot Job/BigJob URL: " + bj.pilot_url + " State: " + str(bj.get_state())

    ##########################################################################################
    # Submit SubJob through BigJob
    jobs = []
    job_start_times = {}
    job_states = {}
    for i in range(0, NUMBER_JOBS):
        jd = saga.job.description()
        jd.executable = "/bin/date"
        jd.number_of_processes = "1"
        jd.spmd_variation = "single"
        jd.arguments = [""]
        jd.working_directory = os.getcwd() 
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
    #time.sleep(30)
