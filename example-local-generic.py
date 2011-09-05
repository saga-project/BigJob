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
    BIGJOB_HOME= os.getcwd() + "."
    os.environ["BIGJOB_HOME"]=BIGJOB_HOME

sys.path.insert(0, BIGJOB_HOME)
sys.path.append("..") # for api.base

from bigjob_generic import bigjob, subjob


""" Test Job Submission via Advert """
if __name__ == "__main__":

    ##########################################################################################
    # Start BigJob
    # Parameter for BigJob
    bigjob_agent = os.getcwd() + "/bigjob_agent_launcher.sh" # path to agent
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
