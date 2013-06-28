#!/usr/bin/env python

"""
Dynamic BigJob (ManyJob) Example

This Module is used to launch a set of bigjobs.

DON'T EDIT THIS FILE (UNLESS THERE IS A BUG)
    
THIS FILE SHOULD NOT BE COMMITTED TO SVN WITH USE-SPECIFIC PATHS!

"""

import sys
import getopt
import time
import pdb
import os
import traceback
import logging


# BigJob implementation can be swapped here by importing another implementation,
# e.g. condor, cloud, azure
import sys

sys.path.insert(0, os.getcwd() + "/../")

from bigjob import bigjob, subjob, description
from bigjob_dynamic.many_job import *


""" This variable defines the coordination system that is used by BigJob
    e.g. 
        advert://localhost (SAGA/Advert SQLITE)
        advert://advert.cct.lsu.edu:8080 (SAGA/Advert POSTGRESQL)
        redis://localhost:6379 (Redis at localhost)
        tcp://localhost (ZMQ)
"""
COORDINATION_URL = "advert://localhost/?dbtype=sqlite3"
#COORDINATION_URL = "advert://advert.cct.lsu.edu:8080"
    
NUMBER_JOBS=8

def has_finished(state):
        state = state.lower()
        if state=="done" or state=="failed" or state=="canceled":
            return True
        else:
            return False

def main():
    try:
        print "ManyJob load test with " + str(NUMBER_JOBS) + " jobs."
        starttime=time.time()

        """ submit via mj abstraction
         
         resource_list.append( {"resource_url" : "gram://eric1.loni.org/jobmanager-pbs", "processes_per_node":"4",
                               "number_of_processes" : "4", "allocation" : None, "queue" : "workq", 
                               "working_directory": (os.getcwd() + "/agent"), "walltime":10 })

        """
        resource_list = []
        resource_dictionary = {"resource_url" : "fork://localhost/", "number_of_processes" : "32", 
                               "processes_per_node":"1", "allocation" : None, "queue" : None, 
                               "working_directory": (os.getcwd() + "/agent"), "walltime":3600 }
        resource_list.append(resource_dictionary)
        
        
        #Flags for controlling dynamic BigJob
        add_additional_resources=True
        remove_additional_resources=False
        

        print "Create Dynamic BigJob Service "
        mjs = many_job_service(resource_list, COORDINATION_URL)
        
        jobs = []
        job_start_times = {}
        job_states = {}
        cwd = os.getcwd()
        for i in range(0, NUMBER_JOBS):
            # create job description
            jd = description()
            jd.executable = "/bin/date"
            jd.number_of_processes = "1"
            jd.spmd_variation = "single"
            jd.arguments = [""]
            jd.working_directory = os.getcwd();
            jd.output =  "stdout-" + str(i) + ".txt"
            jd.error = "stderr-" + str(i) + ".txt"
            subjob = mjs.create_job(jd)
            subjob.run()
            print "Submited sub-job " + "%d"%i + "."
            jobs.append(subjob)
            job_start_times[subjob]=time.time()
            job_states[subjob] = subjob.get_state()
        print "************************ All Jobs submitted ************************"
        while 1: 
            finish_counter=0
            result_map = {}
            for i in range(0, NUMBER_JOBS):
                old_state = job_states[jobs[i]]
                state = jobs[i].get_state()
                if result_map.has_key(state) == False:
                    result_map[state]=0
                result_map[state] = result_map[state]+1
                #print "counter: " + str(i) + " job: " + str(jobs[i]) + " state: " + state
                if old_state != state:
                    print "Job " + str(jobs[i]) + " changed from: " + old_state + " to " + state
                if old_state != state and has_finished(state)==True:
                    print "Job: " + str(jobs[i]) + " Runtime: " + str(time.time()-job_start_times[jobs[i]]) + " s."
                if has_finished(state)==True:
                    finish_counter = finish_counter + 1
                job_states[jobs[i]]=state
                
            # Dynamic BigJob add resources at runtime
            # if more than 30 s - add additional resource
            if time.time()-starttime > 10 and add_additional_resources==True:
                print "***add additional resources***"
                mjs.add_resource(resource_dictionary)
                add_additional_resources=False  
                
            # remove resources from dynamic bigjob
            if (time.time()-starttime > 15 and remove_additional_resources==True):
                bj_list = mjs.get_resources()
                if len(bj_list)>0:
                    print "***remove resources: " + str(bj_list[0])
                    mjs.remove_resource(bj_list[0])
                remove_additional_resources=False
                
            print "Current states: " + str(result_map) 
            time.sleep(5)
            if finish_counter == NUMBER_JOBS:
                break

        mjs.cancel()
        runtime = time.time()-starttime
        print "Runtime: " + str(runtime) + " s; Runtime per Job: " + str(runtime/NUMBER_JOBS)
    except:
        traceback.print_exc(file=sys.stdout)
        try:
            mjs.cancel()
        except:
            pass

""" Test Job Submission via ManyJob abstraction """
if __name__ == "__main__":
    main()
