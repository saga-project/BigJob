#!/usr/bin/env python
"""Dynamic BigJob (ManyJob) with affinity aware scheduler.
"""

import os
import sys
sys.path.append(os.path.dirname( __file__ ))
from many_job import *

class many_job_affinity_service(many_job_service):
    
    def __init__(self, bigjob_list, advert_host):
        """ accepts resource list as key/value pair:
            ( {"resource_url" : "gram://qb1.loni.org/jobmanager-pbs", "number_cores" : "32", "allocation" : "<allocaton>", "queue" : "workq", 
               "re_agent":  os.getcwd() + "/bigjob_agent_launcher.sh"  "walltime":1000, "affinity": "affinity1"},
              {"resource_url" : "gram://qb1.loni.org/jobmanager-pbs", "number_cores" : "32", "allocation" : "<allocation", "queue" : "workq", 
               "re_agent": "os.getcwd() + "/bigjob_agent_launcher.sh", "walltime":1000, "affinity": "affinity1"})
        """   
        super(many_job_affinity_service, self).__init__(bigjob_list, advert_host)
        
    def __schedule_subjob (self, subjob):
        """ find resource (bigjob) with the same affinity for subjob 
            returns bigjob object """
        for i in self.bigjob_list:
            bigjob = i["bigjob"]
            lock = i["lock"]
            affinity = i["affinity"]
            lock.acquire()
            free_cores = i["free_cores"]
            bigjob_url = bigjob.pilot_url
            state = bigjob.get_state_detail()
            env  = subjob.job_description.environment[0]
            sj_affinity=None
            if env.find("affinity") >=0:
                sj_affinity = env.split("=")[1]            
            logging.debug("Subjob Env: " + str(sj_affinity))
            logging.debug("Big Job: " + bigjob_url + " Cores: " + "%s"%free_cores + "/" + i["number_cores"] + " Affinity: " 
                          + affinity + " SJ affinity: " + sj_affinity + " State: " + state)
            if state.lower() == "running" and free_cores >= int(subjob.job_description.number_of_processes) and affinity == sj_affinity:
                free_cores = i["free_cores"]
                free_cores = free_cores - int(subjob.job_description.number_of_processes)
                i["free_cores"]=free_cores
                lock.release()
                return i 

            lock.release()

        # no resource found
        self.subjob_queue.put(subjob)
        logging.debug("found no active resource for sub-job => (re-) queue it")
        return None        
    
    