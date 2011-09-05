""" Example application demonstrating job submission via bigjob 
    advert_job implementation of BigJob is used
"""

import saga
import os
import time
import pdb

# BigJob implementation can be swapped here by importing another implementation,
# e.g. condor, cloud, azure
import sys



# configurationg
advert_host = "localhost"

if os.getenv("BIGJOB_HOME")!=None:
    BIGJOB_HOME= os.getenv("BIGJOB_HOME") 
else:
    BIGJOB_HOME= os.getcwd() + "/../"
    os.environ["BIGJOB_HOME"]=BIGJOB_HOME

sys.path.insert(0, BIGJOB_HOME)
#from bigjob import bigjob, subjob
from bigjob_generic import bigjob, subjob

NUMBER_JOBS=128

def has_finished(state):
        state = state.lower()
        if state=="done" or state=="failed" or state=="canceled":
            return True
        else:
            return False


""" Test Job Submission via Advert """
if __name__ == "__main__":

    print "BigJob load test with " + str(NUMBER_JOBS) + " jobs."
    starttime=time.time()
    ##########################################################################################
    # Start BigJob
    # Parameter for BigJob
    #bigjob_agent = BIGJOB_HOME + "/bigjob_agent_launcher.sh" # path to agent
    bigjob_agent = os.getcwd() + "/bigjob_agent_launcher.sh"
    #bigjob_agent = "/bin/echo"
    nodes = 1 # number nodes for agent
    lrms_url = "fork://localhost" # resource url
    workingdirectory=os.getcwd() +"/agent"  # working directory for agent
    userproxy = None # userproxy (not supported yet due to context issue w/ SAGA)

    # start pilot job (bigjob_agent)
    print "Start Pilot Job/BigJob: " + bigjob_agent + " at: " + lrms_url
    bj = bigjob(advert_host)
    bj.start_pilot_job(lrms_url,
                            bigjob_agent,
                            nodes,
                            None,
                            None,
                            workingdirectory, 
                            userproxy,
                            None)
        
    
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

        sj = subjob(advert_host)
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
