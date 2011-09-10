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

from bigjob.bigjob_manager import bigjob, subjob

def main():
    ##########################################################################################
    # Start BigJob
    # Parameter for BigJob
    nodes = 1 # number nodes for agent
    lrms_url = "fork://localhost" # resource url
    workingdirectory=os.getcwd() +"/agent"  # working directory for agent
    userproxy = None # userproxy (not supported yet due to context issue w/ SAGA)

    # start pilot job (bigjob_agent)
    print "Start Pilot Job/BigJob: " + bigjob_agent + " at: " + lrms_url
    bj = bigjob(advert_host)
    bj.start_pilot_job(lrms_url,
                            None,
                            nodes,
                            None,
                            None,
                            workingdirectory, 
                            userproxy,
                            None)
        
    
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
    sj = subjob(advert_host)
    sj.submit_job(bj.pilot_url, jd)
    
    # busy wait for completion
    while 1:
        state = str(sj.get_state())
        print "state: " + state
        if(state=="Failed" or state=="Done"):
            break
        time.sleep(2)

    #time.sleep(30)
    ##########################################################################################
    # Cleanup - stop BigJob
    bj.cancel()
    #time.sleep(30)


""" Test Job Submission via Advert """
if __name__ == "__main__":
    main()
    