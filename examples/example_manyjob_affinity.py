#!/usr/bin/env python

"""many_job example with affinity.

This Module is used to launch a set of bigjobs.

"""
import getopt
import time
import pdb
import os
import traceback
import logging
import sys

sys.path.insert(0, os.getcwd() + "/../")

# Big Job Imports
from bigjob import bigjob, subjob, description
from bigjob_dynamic.many_job_affinity import *

""" This variable defines the coordination system that is used by BigJob
    e.g. 
        advert://localhost (SAGA/Advert SQLITE)
        advert://advert.cct.lsu.edu:8080 (SAGA/Advert POSTGRESQL)
        redis://localhost:6379 (Redis at localhost)
        tcp://localhost (ZMQ)
"""
COORDINATION_URL = "advert://localhost/?dbtype=sqlite3"

NUMBER_JOBS=8


def has_finished(state):
        state = state.lower()
        if state=="done" or state=="failed" or state=="canceled":
            return True
        else:
            return False

""" Test Job Submission via ManyJob abstraction """
if __name__ == "__main__":
    try:
        print "ManyJob load test with " + str(NUMBER_JOBS) + " jobs."
        starttime=time.time()

        # submit via mj abstraction
        resource_list = []
        resource_list.append( {"resource_url" : "fork://localhost/", "number_of_processes" : "2", "allocation" : "myAllocation", 
                               "queue" : "workq",  
                               "working_directory": (os.getcwd() + "/agent"), 
                               "walltime": 10, "affinity" : "affinity1"})

        #resource_list.append( {"resource_url" : "gram://oliver1.loni.org/jobmanager-pbs", "number_nodes" : "4", "allocation" : "<your allocation>", 
        #                       "queue" : "workq", "bigjob_agent": (BIGJOB_HOME+"/bigjob_agent_launcher.sh"), 
        #                       "working_directory": (os.getcwd() + "/agent"), "walltime":10, "affinity" : "affinity1"})

        print "Create manyjob service "
        mjs = many_job_affinity_service(resource_list, COORDINATION_URL)
        
        jobs = []
        job_start_times = {}
        job_states = {}
        cwd = os.getcwd()
        for i in range(0, NUMBER_JOBS):
            # create job description
            jd = description()
            jd.executable = "/bin/echo"
            jd.number_of_processes = "1"
            jd.spmd_variation = "single"
            jd.arguments = ["Hello","$VAR"]
            jd.output =  "stdout-" + str(i) + ".txt"
            jd.error =  "stderr-" + str(i) + ".txt"
            jd.environment = ["affinity=affinity1","VAR=USER"]
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

