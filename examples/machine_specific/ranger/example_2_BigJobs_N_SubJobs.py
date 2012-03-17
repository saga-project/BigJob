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
bjs=[]

if __name__ == "__main__":

    starttime=time.time()

    resource_list = []
    ## append machine parameters as below for each machine to be used.
    
    resource_list.append( {"lrms_url" : "sge-ssh://username@login#.ranger.tacc.utexas.edu",
                           "total_cores" : "32",
                           "tasks_per_node":"16",
                           "project" : "TG-XXXXXX",
                           "queue" : "normal",  
                           "workingdirectory": "/share/home/xxxxx/xxxxxx/agent",
                           "walltime": 10} )

    resource_list.append( {"lrms_url" : "sge-ssh://username@login#.ls4.tacc.utexas.edu",
                           "total_cores" : "24",
                           "tasks_per_node":"12",
                           "project" : "TG-MCB090174",
                           "queue" : "normal",  
                           "workingdirectory": "/home1/xxxxxx/xxxxxxx/agent",
                           "walltime": 10} )

    ##########################################################################################
    # Start bigjobs 
 
    for resource in resource_list:
        print "Start BigJob at: " + resource['lrms_url']
        bj=bigjob(COORDINATION_URL)
        bjs.append(bj)
        bj.start_pilot_job( resource['lrms_url'],
                             None,
                             resource['total_cores'],
                             resource['queue'],
                             resource['project'],
                             resource['workingdirectory'],
                             None,
                             resource['walltime'],
                             resource['tasks_per_node'])

    print "Pilot Job/BigJob URL: " + bj.pilot_url + " State: " + str(bj.get_state())

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
        sj.submit_job(bjs[0].pilot_url, jd)  # bjs[0] represents the 1st bigjob

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
        sj.submit_job(bjs[1].pilot_url, jd) # bjs[1] represents the 2nd bigjob

    ##########################################################################################
    # waiting until all bigjobs complete
    ##########################################################################################

    for bj in bjs:
        print "waiting started..\n"
        bj.wait()
        print "waiting done..\n"

    runtime = time.time()-starttime
    print "Runtime: " + str(runtime) + " s; Runtime per Job: " + str(runtime/NUMBER_JOBS)

    ##########################################################################################
    # Cleanup - stop BigJob
    for bj in bjs:
        bj.cancel()
