""" Example application demonstrating job submission via bigjob 
"""

import saga
import os
import time
import pdb
import sys

# BigJob implementation can be swapped here by importing another implementation,
# e.g. condor, cloud, azure


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

#COORDINATION_URL = "advert://localhost/"
COORDINATION_URL = "advert://advert.cct.lsu.edu:8080/"
#COORDINATION_URL = "tcp://*"
#COORDINATION_URL = "redis://localhost:6379"

# for running BJ from local dir
sys.path.insert(0, os.getcwd() + "/../")

from bigjob.bigjob_manager import bigjob, subjob

def main():
    # Start BigJob

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
    #lrms_url = "gram://oliver1.loni.org/jobmanager-pbs" # globus resource url used when globus is used. (LONI)
    #lrms_url = "pbspro://louie1.loni.org" # pbspro resource url used when pbspro scheduling system is used.(Futuregrid or LSU Machines)
    #lrms_url = "ssh://louie1.loni.org" # ssh resource url which launches jobs on target machine. Jobs not submitted to scheduling system.
    lrms_url = "pbs-ssh://louie1.loni.org" # Submit jobs to scheduling system of remote machine.
    #lrms_url = "xt5torque://localhost" # torque resource url.
 
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
    jd = saga.job.description()
    jd.executable = "/bin/date"
    jd.number_of_processes = "1"
    jd.spmd_variation = "single"
    jd.arguments = [""]
    jd.working_directory = os.getcwd() 
    jd.output = "stdout.txt"
    jd.error = "stderr.txt"
    sj = subjob()
    sj.submit_job(bj.pilot_url, jd)
    
    #########################################
    # busy wait for completion
    while 1:
        state = str(sj.get_state())
        print "state: " + state
        if(state=="Failed" or state=="Done"):
            break
        time.sleep(2)

    ##########################################################################################
    # Cleanup - stop BigJob
    bj.cancel()
    #time.sleep(30)


""" Test Job Submission via Advert """
if __name__ == "__main__":
    main()
    
