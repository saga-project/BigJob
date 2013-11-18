#!/usr/bin/env python

"""Dynamic BigJob (ManyJob): Manages multiple BigJob (on different resources). 
Sub-jobs are distributed across the set of BJs managed by the dynamic BJ.
"""
import pdb
import sys
import os
sys.path.append(os.path.dirname( __file__ ))
import getopt
import time
import uuid
import socket
import traceback

import Queue
import threading
import logging
import time
import math
import operator
import copy


import bigjob.bigjob_manager 

# Log everything, and send it to stderr.
#logging.basicConfig(level=logging.DEBUG)

COORDINATION_URL="advert://advert.cct.lsu.edu:8080"

class many_job_service(object):

    def __init__(self, bigjob_list, coordination_url):
        """ accepts resource list as key/value pair:
            ( {"resource_url" : "gram://qb1.loni.org/jobmanager-pbs", "number_nodes" : "32", "allocation" : "loni_stopgap2", "queue" : "workq", "bigjob_agent": "$(HOME)/src/REMDgManager/bigjob/advert_launcher.sh",  "walltime":1000},
              {"resource_url" : "gram://qb1.loni.org/jobmanager-pbs", "number_nodes" : "32", "allocation" : "loni_stopgap2", "queue" : "workq", "bigjob_agent": "$(HOME)/src/REMDgManager/bigjob/advert_launcher.sh", "walltime":1000})
        """        
        self.uuid = uuid.uuid1()
        
        if coordination_url==None:
            self.advert_host=COORDINATION_URL   
        else:
            self.advert_host=coordination_url

        # list of resource dicts (1 dict per resource) 
        # will also store state of bigjob
        self.bigjob_list=copy.deepcopy(bigjob_list) 
       
        # state variable storing state of sub-jobs 
        self.active_subjob_list = []
        self.subjob_bigjob_dict = {}

        # queue contains unscheduled subjobs        
        self.subjob_queue = Queue.Queue()
        
        # submit bigjobs to resources
        self.__init_bigjobs()
        
        # thread which tries to resubmit jobs
        self.stop=threading.Event()
        self.rescheduler_thread=threading.Thread(target=self.__reschedule_subjobs_thread)
        self.rescheduler_thread.start()
        
        # last queue Size
        self.last_queue_size = 0
        self.submisssion_times=[]

    def __init_bigjobs(self):
        """ start on specified resources a bigjob """
        self.bigjob_list = self.__schedule_bigjobs()
        for i in self.bigjob_list:
            self.__start_bigjob(i)


    def __start_bigjob(self, bj_dict):
        """ private method - starts a bigjob on the defined resource """
        gram_url = bj_dict["resource_url"]
        logging.debug("start bigjob at: " + gram_url)
        bj = bigjob.bigjob_manager.bigjob(self.advert_host)

        if("processes_per_node" not in bj_dict or bj_dict["processes_per_node"] == 'None'):
            ppn="1"
	    bj_dict["processes_per_node"] = ppn
        else:
            ppn=bj_dict["processes_per_node"]

        walltime = 3600
        if ("walltime" in bj_dict):
            walltime=bj_dict["walltime"]

        working_directory = (os.getcwd()+"/agent")
        if ("working_directory" in bj_dict):
            working_directory=bj_dict["working_directory"]
            
        bj_filetransfer = None
        if ("file_transfer" in bj_dict):
            bj_filetransfer = bj_dict["file_transfer"]

        bj.start_pilot_job(gram_url,
                           None,
                           bj_dict["number_of_processes"],
                           bj_dict["queue"],
                           bj_dict["allocation"],
                           working_directory, 
                           None,
                           walltime,
                           ppn,
                           filetransfers=bj_filetransfer)
        bj_dict["bigjob"]=bj # store bigjob for later reference in dict
        bj_dict["free_cores"]=int(bj_dict["number_of_processes"])
        bj_dict["to_be_terminated"]=False
        # lock for modifying the number of free nodes
        bj_dict["lock"] = threading.Lock()

    def add_resource(self, resource_dictionary):
        """ adds bigjob described in resource_dictionary to resources """
        bj_dict = copy.deepcopy(resource_dictionary)
        
        self.__start_bigjob(bj_dict)
        self.bigjob_list.append(bj_dict)
        return bj_dict["bigjob"] 
        
    
    def remove_resource(self, bigjob):
        """ remove bigjob from resource list of manyjob """
        # mark bigjob for termination (after all sub-jobs in bj are 
        # finished        
        bigjob["to_be_terminated"]=True
        
 
    def __cleanup_resources(self):
        """ called periodically from scheduling thread
            terminates big-jobs which are marked and don't have 
            any running sub-jobs
        """
        # iterate over copy of list, but remove from orig list
        for i in self.bigjob_list[:]:
            if i["to_be_terminated"]==True:
                bj = i["bigjob"]
                total_cores = int(i["number_of_processes"])
                if  i["free_cores"]==total_cores and not i.has_key("bj_stopped"):
                    logging.debug("***Stop BigJob: " + str(bj.pilot_url))
                    # release resources of pilot job
                    bj.stop_pilot_job()
                    i["bj_stopped"]=True
                    #self.bigjob_list.remove(i)

    
    def get_resources(self):
        """ returns list with bigjob dictionaries
            for each managed bigjob 1 dictionary exists 
        """
        return self.bigjob_list


    def list_bigjobs(self):
        """ returns a list of bigjob objects """
        return [i["bigjob"]  for i in self.bigjob_list]

    def __schedule_bigjobs(self):
        """ prioritizes bigjob_list (bigjob with shortest expected delay will have index 0) """
        # no scheduling for now (start bigjob in the user specified order)
        return self.bigjob_list

    def create_job (self, job_description):
        subjob = sub_job(self, job_description, self.advert_host)        
        return subjob

    def __run_subjob(self, subjob):
        # select appropriate bigjob
        st = time.time()
        bigjob_info = self.__schedule_subjob(subjob)
        job = subjob.job
        if bigjob_info == None:
            return job

        # create subjob on bigjob
        bj = bigjob_info["bigjob"]
        
        job.submit_job(bj.pilot_url, subjob.job_description)
        self.submisssion_times.append(time.time()-st)

        # store reference of subjob for further bookkeeping    
        self.active_subjob_list.append(subjob)
        self.subjob_bigjob_dict[subjob] = bigjob_info
        logging.debug("Subjob submission time: " + str(time.time()-st) + " sec.")
        return job

    def queue_subjob(self, subjob):
        subjob.job = bigjob.bigjob_manager.subjob(self.advert_host) 
        self.subjob_queue.put(subjob)
        return subjob.job

    def __schedule_subjob (self, subjob):
        """ find resource (bigjob) for subjob
            returns bigjob object """
        for i in self.bigjob_list:
            bigjob = i["bigjob"]
            lock = i["lock"]
            lock.acquire()
            free_cores = i["free_cores"]
            bigjob_url = bigjob.pilot_url
            state = bigjob.get_state_detail()
            logging.debug("Big Job: " + bigjob_url + " Cores: " + "%s"%free_cores + "/" 
                          + str(int(i["number_of_processes"])) 
                          + " State: " + str(state) + " Terminated: " + str(i["to_be_terminated"])
                          + " #Required Cores: " + subjob.job_description.number_of_processes
                          )
            if (state.lower() == "running" and free_cores >= int(subjob.job_description.number_of_processes) 
                and i["to_be_terminated"]==False):
                logging.debug("FOUND match - dispatch to BigJob: " + bigjob_url)
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

    def __check_subjobs_states(self):    
        """iterate through all sub-jobs and check state"""
        for i in self.active_subjob_list:            
            try:
                #logging.debug("get job state")
                state = i.job.get_state()
                #logging.debug("check job state")
                if self.__has_finished(state) == True:
                    #logging.debug("free resources")
                    self.__free_resources(i)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback,
                                          limit=2, file=sys.stderr)
        
    def __free_resources(self, subjob):
        """free resources taken by subjob"""
        if(self.subjob_bigjob_dict.has_key(subjob)):
            logging.debug("job: " + str(subjob) + " done - free resources")
            bigjob = self.subjob_bigjob_dict[subjob]
            lock = bigjob["lock"]
            lock.acquire()
            free_cores = bigjob["free_cores"]
            free_cores = free_cores + int(subjob.job_description.number_of_processes)
            bigjob["free_cores"]=free_cores
            del(self.subjob_bigjob_dict[subjob])
            lock.release()
            print "Freed resource - new state: Big Job: " +  bigjob["bigjob"].pilot_url + " Cores: " + "%s"%free_cores + "/" + str(int(bigjob["number_of_processes"])) 
    
    def __reschedule_subjobs_thread(self):
        """ periodically checks subjob_queue for unscheduled subjobs
            if a unscheduled job exists it is scheduled
        """

        while True and self.stop.isSet()==False:
            logging.debug("Reschedule Thread")
            # check sub-job state
            self.__check_subjobs_states()
            # remove unneeded big-jobs
            self.__cleanup_resources()
            subjob = self.subjob_queue.get()  
            # check whether this is a real subjob object  
            if isinstance(subjob, sub_job):
                self.__run_subjob(subjob)
                if self.last_queue_size == self.subjob_queue.qsize() or self.__get_total_free_cores()==0:
                    time.sleep(2) # sleep 30 s        

        logging.debug("Re-Scheduler terminated")


    def __get_free_cores(self, bigjob):
        """ return number of free cores if bigjob is active """
        #pdb.set_trace()
        if (bigjob["bigjob"].get_state_detail().lower()=="running" 
            and bigjob["to_be_terminated"]==False):
            return bigjob["free_cores"]

        return 0            

    def __get_total_free_cores(self):
        """ get's the total number of free cores from all active  bigjobs """
        free_cores = map(self.__get_free_cores, self.bigjob_list)
        #print "Free cores: " + str(free_cores)
        if len(free_cores)>0:
            total_free_cores = reduce(lambda x, y: x + y, free_cores)
            logging.debug("free_cores: " + str(free_cores) + " total_free_cores: " + str(total_free_cores))
            return total_free_cores
        return 0

    def cancel(self):
        logging.debug("Cancel re-scheduler thread")
        self.stop.set()
        # put object in queue to unlock the get() operation
        self.subjob_queue.put("dummy")
        self.rescheduler_thread.join()        
        logging.debug("Cancel many-job: kill all bigjobs")
        for i in self.bigjob_list:
            bigjob = i["bigjob"]
            bigjob.cancel()
        self.print_stats(self.submisssion_times, "Submission Times")

    def print_stats(self, times, description):
        try:
            n = len(times)
            sum = reduce(operator.add, times)
            mean = sum/n
            variance=0
            if n > 1:
                for i in times:
                    variance += (i - mean)**2
                variance /= (n-1)
                variance = math.sqrt(variance)
            print description + " Average: " + str(mean) + " Stdev: " + str(variance)
        except:
            pass
        
    def __has_finished(self, state):
        state = state.lower()
        if state=="done" or state=="failed" or state=="canceled":
            return True
        else:
            return False

    def __repr__(self):
        return str(self.uuid)

    def __del__(self):
        self.cancel()
        
        
                    

class sub_job(object):
    """ Class for controlling individual sub-jobs """
    
    def __init__(self, manyjob, job_description, advert_host):
        # store bigjob for reference
        self.manyjob=manyjob
        
        # init sub-job via advert
        self.job_description = job_description
        self.job = None

    def run(self):
        # select appropriate bigjob
        self.job = self.manyjob.queue_subjob(self)

    def get_state(self):     
        try:
            state = self.job.get_state()
            return state
        except:
            #traceback.print_stack()
            pass
        return "Unknown"

    def get_arguments(self):
        try:
            arguments = self.job.get_arguments()
            return arguments
        except:
            traceback.print_exc(file=sys.stdout)
            pass
        return ""

    def get_exe(self):
        try:
            exe  = self.job.get_exe()
            return exe
        except:
            traceback.print_exc(file=sys.stdout)
            pass
        return "Unknown"


    def cancel(self):
        return self.job.cancel()

    def wait(self):
        while 1:
            try:
                state = self.get_state()
                logging.debug("wait: state: " + state)
                if self.__has_finished(state) == True:
                    break
                time.sleep(2)
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                pass
   
    def __del__(self):
        pass

    def __repr__(self):        
        return str(self.job)

""" Test Job Submission via ManyJob abstraction """
if __name__ == "__main__":
    try:
        print "Test ManyJob"
        # create job description
        jd = SAGAJobDescription()
        jd.executable = "/bin/date"
        jd.number_of_processes = "1"
        jd.spmd_variation = "single"
        jd.arguments = [""]
        jd.working_directory = "/home/luckow"
        jd.output = "output.txt"
        jd.error = "error.txt"
        # submit via mj abstraction
        #resource_list =  ( {"resource_url" : "gram://qb1.loni.org/jobmanager-pbs", "number_nodes" : "128", "allocation" : "<your allocation>", "queue" : "workq", "bigjob_agent": "$(HOME)/src/REMDgManager/bigjob/advert_launcher.sh"},
        #                   {"resource_url" : "gram://qb1.loni.org/jobmanager-pbs", "number_nodes" : "64", "allocation" : "<your allocation>", "queue" : "workq", "bigjob_agent": "$(HOME)/src/REMDgManager/bigjob/advert_launcher.sh"})
        resource_list = []
        resource_list.append({"resource_url" : "gram://qb1.loni.org/jobmanager-pbs", "number_nodes" : "16", "allocation" : "<your allocation>", "queue" : "workq", "bigjob_agent": os.getcwd() + "/bigjob_agent_launcher.sh"})
        print "Create manyjob service " 
        mjs = many_job_service(resource_list, None)
        print "Create sub-job using manyjob " + str(mjs) 
        subjob = mjs.create_job(jd)
        print "Run sub-job"
        subjob.run()
        print "Wait for termination"
        subjob.wait()
        print "Sub-job state: " + str(subjob.get_state())
        mjs.cancel()
    except:
        try:
            if mjs != None:
                mjs.cancel()
        except:
            pass
