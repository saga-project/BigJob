""" Example application demonstrating job submission via bigjob 

    DON'T EDIT THIS FILE (UNLESS THERE IS A BUG)
    
    THIS FILE SHOULD NOT BE COMMITTED TO SVN WITH USER-SPECIFIC PATHS!
"""

import os
import time
import pdb
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
COORDINATION_URL = "advert://localhost/?dbtype=sqlite3"
#COORDINATION_URL = "redis://localhost"
#COORDINATION_URL = "advert://advert.cct.lsu.edu:8080/"

from bigjob import bigjob, subjob, description


### EDIT based on the number of jobs you want to submit
NUMBER_JOBS=1

def has_finished(state):
        state = state.lower()
        if state=="done" or state=="failed" or state=="canceled":
            return True
        else:
            return False


""" Test Job Submission via Advert """
if __name__ == "__main__":

    starttime=time.time()
    
    if len(sys.argv)==2:
        reconnect_url=sys.argv[1]
    else:
        print "Usage: " + sys.executable + " " + __file__ + " <BigJob URL to Reconnect to>"
        sys.exit(-1)

    print "Reconnect to Pilot Job/BigJob at: " + reconnect_url
    bj = bigjob(pilot_url=reconnect_url)
    
    print "Pilot Job/BigJob URL: " + bj.pilot_url + " State: " + str(bj.get_state() 
           + " Number of SJs: " + str(len(bj.list_subjobs())) )

    ##########################################################################################
    # Submit some more subjobs
    if bj.get_state()!="Done":
        jobs = []
        job_start_times = {}
        job_states = {}
        for i in range(0, NUMBER_JOBS):
            jd = description()
            jd.executable = "/bin/date"
            jd.number_of_processes = "1"
            jd.spmd_variation = "single"
            jd.arguments = [""]
            jd.output = "sj-stdout-"+str(i)+".txt"
            jd.error = "sj-stderr-"+str(i)+".txt"
    
            sj = subjob()
            sj.submit_job(bj.pilot_url, jd)
            jobs.append(sj)
            job_start_times[sj]=time.time()
            job_states[sj] = sj.get_state()
    
        print "Pilot Job/BigJob URL: " + bj.pilot_url + " State: " + str(bj.get_state() 
               + " Number of SJs: " + str(len(bj.list_subjobs())) )
        print "Wait for completion..."
        bj.wait()
    
        runtime = time.time()-starttime
        print "Runtime: " + str(runtime) + " s; Runtime per Job: " + str(runtime/NUMBER_JOBS)
        
        bj.cancel()
    
