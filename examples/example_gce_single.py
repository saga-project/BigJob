""" Example application demonstrating job submission via bigjob 

    DON'T EDIT THIS FILE (UNLESS THERE IS A BUG)
    
    THIS FILE SHOULD NOT BE COMMITTED TO SVN WITH USER-SPECIFIC PATHS!
"""
import os
import time
import pdb
import sys

# configuration
""" This variable defines the coordination system that is used by BigJob
    e.g. 
        advert://localhost (SAGA/Advert SQLITE)
        advert://advert.cct.lsu.edu:8080 (SAGA/Advert POSTGRESQL)
        redis://localhost:6379 (Redis at localhost)
        tcp://localhost (ZMQ)
        tcp://* (ZMQ - listening to all interfaces)
"""

COORDINATION_URL = "redis://localhost:6379"

# for running BJ from local dir
sys.path.insert(0, os.getcwd() + "/../")

from bigjob import bigjob, subjob, description


def main():
    # Start BigJob

    ##########################################################################################
    # Edit parameters for BigJob
    queue=None # if None default queue is used
    project=None # if None default allocation is used 
    walltime=10
    processes_per_node=1
    number_of_processes = 1
    workingdirectory="." # working directory for agent
    userproxy = None # userproxy (not supported yet due to context issue w/ SAGA)

        
    #lrms_url = "ec2+ssh://localhost" # resource url to run on GCE
    lrms_url = "gce+ssh://locahost"
    ##########################################################################################

    print "Start Pilot Job/BigJob at: " + lrms_url
    bj = bigjob(COORDINATION_URL)
    bj.start_pilot_job( lrms_url,
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
    jd = description()
    jd.executable = "/bin/echo"
    #jd.executable = "$HOME/hello.sh"
    jd.number_of_processes = "1"
    jd.arguments = ["$HELLOWORLD"]
    jd.environment = ['HELLOWORLD=hello_world']
    jd.input_data = ["hi", "ho"]
    
    # specify an optinal working directory if sub-job should be executed outside of bigjob sandbox
    #jd.working_directory = "/tmp" 
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
    
