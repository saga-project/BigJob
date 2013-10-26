#!/usr/bin/env python
"""bigjob_agent: bigjob agent adapted for Condor resources
"""
import sys
import os
import bigjob.state
import socket
import threading
import time
import pdb
import traceback
import ConfigParser
import types
import logging
logging.basicConfig(level=logging.DEBUG)

try:
    import saga
except:
    logging.warning("SAGA could not be found. Not all functionalities working")

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../ext/threadpool-1.2.7/src/")
logging.debug(str(sys.path))
from threadpool import *

if sys.version_info < (2, 5):
    sys.path.append(os.path.dirname( __file__ ) + "/../../ext/uuid-1.30/")
    sys.stderr.write("Warning: Using unsupported Python version\n")
if sys.version_info < (2, 4):
    sys.path.append(os.path.dirname( __file__ ) + "/../../ext/subprocess-2.6.4/")
    sys.stderr.write("Warning: Using unsupported Python version\n")
if sys.version_info < (2, 3):
    sys.stderr.write("Warning: Python versions <2.3 not supported\n")
    sys.exit(-1)

import subprocess

""" Config parameters (will move to config file in future) """
CONFIG_FILE="bigjob_agent.conf"
THREAD_POOL_SIZE=4
APPLICATION_NAME="bigjob"

class bigjob_agent:
    
    """BigJob Agent:
       - reads new job information from communication and coordination subsystem (Redis)
       - starts new jobs
       - monitors running jobs """
   
    """Constructor"""
    def __init__(self, args):
        
        self.coordination_url = args[1]
        # objects to store running jobs and processes
        self.jobs = []
        self.processes = {}
        self.freenodes = []
        self.busynodes = []
        self.restarted = {}

        # read config file
        # conf_file = os.path.dirname(args[0]) + "/" + CONFIG_FILE
        # conf_file = os.path.dirname(os.path.abspath( __file__ )) + "/" + CONFIG_FILE
        conf_file = os.path.dirname(os.path.abspath( __file__ )) + "/../" + CONFIG_FILE
        config = ConfigParser.ConfigParser()
        logging.debug ("read configfile: " + conf_file)
        config.read(conf_file)
        default_dict = config.defaults()        
        self.CPR = default_dict["cpr"]
        self.SHELL=default_dict["shell"]
        self.MPIRUN=default_dict["mpirun"]
        logging.debug("cpr: " + self.CPR + " mpi: " + self.MPIRUN + " shell: " + self.SHELL)
        
        # init rms (SGE/PBS)
        self.init_rms()

        self.failed_polls = 0
        
        ##############################################################################
        # initialization of coordination and communication subsystem
        # Redis initialization
        self.base_url = args[2]
        logging.debug("BigJob Agent arguments: " + str(args))
        logging.debug("Initialize C&C subsystem to pilot-url: " + self.base_url)
        
        
        if(self.coordination_url.startswith("advert://")):
            try:
                from coordination.bigjob_coordination_advert import bigjob_coordination
                logging.debug("Utilizing ADVERT Backend: " + self.coordination_url)
            except:
                logging.error("Advert Backend could not be loaded")
        elif (self.coordination_url.startswith("redis://")):
            try:
                from coordination.bigjob_coordination_redis import bigjob_coordination      
                logging.debug("Utilizing Redis Backend: " + self.coordination_url + ".") 
            except:
                logger.error("Error loading pyredis. Please verify Redis is configured properly.")
        elif (self.coordination_url.startswith("tcp://")):
            try:
                from coordination.bigjob_coordination_zmq import bigjob_coordination
                logging.debug("Utilizing ZMQ Backend")
            except:
                logging.error("ZMQ Backend not found. Please install ZeroMQ (http://www.zeromq.org/intro:get-the-software) and " 
                      +"PYZMQ (http://zeromq.github.com/pyzmq/)")

        self.coordination = bigjob_coordination(server_connect_url=self.coordination_url)
    
        # update state of pilot job to running
        self.coordination.set_pilot_state(self.base_url, str(bigjob.state.Running), False)

        
        ##############################################################################
        # start background thread for polling new jobs and monitoring current jobs
        self.resource_lock=threading.RLock()
        self.threadpool = ThreadPool(THREAD_POOL_SIZE)
        
        self.launcher_thread=threading.Thread(target=self.dequeue_new_jobs)
        self.launcher_thread.start()
        
        self.monitoring_thread=threading.Thread(target=self.start_background_thread)
        self.monitoring_thread.start()
        

    
    def init_rms(self):
        if(os.environ.get("PBS_NODEFILE")!=None):
            return self.init_pbs()
        elif(os.environ.get("PE_HOSTFILE")!=None):
            return self.init_sge()
        else:
            return self.init_local()
        return None

    def init_local(self):
        """ initialize free nodes list with dummy (for fork jobs)"""
        try:
            num_cpus = self.get_num_cpus()
            for i in range(0, num_cpus): 
                self.freenodes.append("localhost\n")
        except IOError:
            self.freenodes=["localhost\n"]

    def init_sge(self):
        """ initialize free nodes list from SGE environment """
        sge_node_file = os.environ.get("PE_HOSTFILE")    
        if sge_node_file == None:
                return
        f = open(sge_node_file)
        sgenodes = f.readlines()
        f.close()
        for i in sgenodes:    
        
            columns = i.split()                
            try:
                for j in range(0, int(columns[1])):
                    logging.debug("add host: " + columns[0].strip())
                    self.freenodes.append(columns[0]+"\n")
            except:
                    pass
        return self.freenodes            

    def init_pbs(self):
        """ initialize free nodes list from PBS environment """
        pbs_node_file = os.environ.get("PBS_NODEFILE")    
        if pbs_node_file == None:
            return
        f = open(pbs_node_file)
        self.freenodes = f.readlines()
        f.close()

        # check whether pbs node file contains the correct number of nodes
        num_cpus = self.get_num_cpus()
        node_dict={}
        for i in set(self.freenodes):
            node_dict[i] = self.freenodes.count(i)
            if node_dict[i] < num_cpus:
                node_dict[i] = num_cpus
    
        self.freenodes=[]
        for i in node_dict.keys():
            logging.debug("host: " + i + " nodes: " + str(node_dict[i]))
            for j in range(0, node_dict[i]):
                logging.debug("add host: " + i.strip())
                self.freenodes.append(i)

    def get_num_cpus(self):
        cpuinfo = open("/proc/cpuinfo", "r")
        cpus = cpuinfo.readlines()
        cpuinfo.close()
        num = 0
        for i in cpus:
                if i.startswith("processor"):
                        num = num+1
        return num
        
     
    def execute_job(self, job_url, job_dict):
        """ obtain job attributes from c&c and execute process """
        state=str(job_dict["state"])
       
        #try:
        #   state = self.coordination.get_job_state(job_url)
        #except:
        #    logging.error("Could not access job state... skip execution attempt and requeuing job")
        #    result = self.coordination.queue_job(self.base_url, job_url)
        #    if result == False:
        #        self.coordination.set_job_state(job_url, str(saga.job.Failed))
        
        if(state==str(bigjob.state.Unknown) or
            state==str(bigjob.state.New)):
            try:
                #job_dict["state"]=str(saga.job.New)                
                logging.debug("Start job: " + str(job_dict))        
                numberofprocesses = "1"
                if (job_dict.has_key("NumberOfProcesses") == True):
                    numberofprocesses = job_dict["NumberOfProcesses"]
                
                spmdvariation="single"
                if (job_dict.has_key("SPMDVariation") == True):
                    spmdvariation = job_dict["SPMDVariation"]
                
                arguments = ""
                if (job_dict.has_key("Arguments") == True):
                    arguments_raw = job_dict['Arguments'];
                    if type(arguments_raw) == types.ListType:
                        arguments_list = arguments_raw
                    else:
                        arguments_list = eval(job_dict["Arguments"])                    
                    for i in arguments_list:
                        arguments = arguments + " " + i
                        
                workingdirectory = os.getcwd()
                if (job_dict.has_key("WorkingDirectory") == True):
                        workingdirectory =  job_dict["WorkingDirectory"]
                
                environment = os.environ
                if (job_dict.has_key("Environment") == True):
                    for i in job_dict["Environment"]:
                        env = i.split("=")
                        environment[env[0]]=env[1] + ":" +  environment[env[0]]                        
                        
                environment["PATH"]= workingdirectory + ":"+environment["PATH"]   
                print "environment[PATH]", environment["PATH"]
                executable = job_dict["Executable"]
                
                
                output="stdout"
                if (job_dict.has_key("Output") == True):
                    output = job_dict["Output"]
                        
                error="stderr"
                if (job_dict.has_key("Error") == True):
                    error = job_dict["Error"]
               
                # append job to job list
                self.jobs.append(job_url)
                
                # create stdout/stderr file descriptors
                output_file = os.path.join(workingdirectory, output)
                error_file = os.path.join(workingdirectory, error)
                logging.debug("stdout: " + output_file + " stderr: " + error_file + " env: " + str(environment))
                stdout = open(output_file, "w")
                stderr = open(error_file, "w")
                #if not "/" in executable:
                #         command = workingdirectory +"/" +executable + " " + arguments
                command = executable + " " + arguments
                #pdb.set_trace()
                # special setup for MPI NAMD jobs
                machinefile = self.allocate_nodes(job_dict)
                host = "localhost"
                try:
                    machine_file_handler = open(machinefile, "r")
                    node= machine_file_handler.readlines()
                    machine_file_handler.close()
                    host = node[0].strip()
                except:
                    pass


                if(machinefile==None):
                    logging.debug("Not enough resources to run: " + job_url)
                    self.coordination.queue_job(self.base_url, job_url)
                    return # job cannot be run at the moment

                # start application process
                if (spmdvariation.lower( )=="mpi"):
                    command = "cd " + workingdirectory + "; " + self.MPIRUN + " -np " + numberofprocesses + " -machinefile " + machinefile + " " + command
                    #if (host != socket.gethostname()):
                    #    command ="ssh  " + host + " \"cd " + workingdirectory + "; " + command +"\""     
                else:
                    command ="chmod +x " + executable +";export PATH=$PATH:" + workingdirectory + ";" +command
                shell = self.SHELL 
                logging.debug("execute: " + command + " in " + workingdirectory + " from: " + str(socket.gethostname()) + " (Shell: " + shell +")")
                # bash works fine for launching on QB but fails for Abe :-(
                p = subprocess.Popen(args=command, executable=shell, stderr=stderr,
                                     stdout=stdout, cwd=workingdirectory, 
                                     env=environment, shell=True)
                logging.debug("started " + command)
                
                #for condor debugging 
                dirlist = os.listdir(workingdirectory)
                print dirlist
                os.system("ls;pwd")
                
                self.processes[job_url] = p
                self.coordination.set_job_state(job_url, str(bigjob.state.Running))
            except:
                traceback.print_exc(file=sys.stderr)
    
            
    def allocate_nodes(self, job_dict):
        """ allocate nodes
            allocated nodes will be written to machinefile advert-launcher-machines-<jobid>
        """
        self.resource_lock.acquire()
        number_nodes = int(job_dict["NumberOfProcesses"])
        nodes = []
        machine_file_name = None
        if (len(self.freenodes)>=number_nodes):
            unique_nodes=set(self.freenodes)
            for i in unique_nodes:
                number = self.freenodes.count(i)
                logging.debug("allocate: " + i + " number nodes: " + str(number) 
                              + " current busy nodes: " + str(self.busynodes) 
                              + " free nodes: " + str(self.freenodes))
                for j in range(0, number):
                    if(number_nodes > 0):
                        nodes.append(i)
                        self.freenodes.remove(i)                
                        self.busynodes.append(i)
                        number_nodes = number_nodes - 1
                    else:
                        break

            machine_file_name = self.get_machine_file_name(job_dict)
            machine_file = open(machine_file_name, "w")
            #machine_file.writelines(self.freenodes[:number_nodes])
            machine_file.writelines(nodes)
            machine_file.close() 
            logging.debug("wrote machinefile: " + machine_file_name + " Nodes: " + str(nodes))
            # update node structures
            #self.busynodes.extend(self.freenodes[:number_nodes])
            #del(self.freenodes[:number_nodes])            
        
        self.resource_lock.release()
        return machine_file_name
        
    
    
    def setup_charmpp_nodefile(self, allocated_nodes):
        """ Setup charm++ nodefile to use for executing NAMD  
            HACK!! Method violates layering principle
            File $HOME/machinefile in charm++ nodefileformat is written to first node in list
        """
        # Nodelist format:
        # 
        # host tp-x001 ++cpus 2 ++shell ssh 
        # host tp-x002 ++cpus 2 ++shell ssh
        
        nodefile_string=""
        for i in allocated_nodes:
            if i.has_key("private_hostname"):
                nodefile_string=nodefile_string + "host "+ i["private_hostname"] + " ++cpus " + str(i["cpu_count"]) + " ++shell ssh\n"
            else:
                nodefile_string=nodefile_string + "host "+ i["hostname"] + " ++cpus " + str(i["cpu_count"]) + " ++shell ssh\n"
            
        # copy nodefile to rank 0 node
        jd = saga.job.description()
        jd.executable = "echo"
        jd.number_of_processes = "1"
        jd.spmd_variation = "single"
        # ssh root@tp-x001.ci.uchicago.edu "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys"
        jd.arguments = ["\""+nodefile_string+"\"", ">", "machinefile"]
        jd.output = "stdout.txt"
        jd.error = "stderr.txt"
        
        job_service_url = saga.url("ssh://root@"+allocated_nodes[0]["hostname"])
        job_service = saga.job.service(self.session, job_service_url)
        job = job_service.create_job(jd)
        job.run()
        job.wait()
    
    def print_machine_file(self, filename):
        fh = open(filename, "r")
        lines = fh.readlines()
        fh.close
        logging.debug("Machinefile: " + filename + " Hosts: " + str(lines))
         
    def free_nodes(self, job_url):
        job_dict = self.coordination.get_job(job_url)
        self.resource_lock.acquire()
        number_nodes = int(job_dict["NumberOfProcesses"])
        machine_file_name = self.get_machine_file_name(job_dict)
        logging.debug("Machine file: " + machine_file_name)
        allocated_nodes = ["localhost\n"]
        try:
            machine_file = open(machine_file_name, "r")
            allocated_nodes = machine_file.readlines()
            machine_file.close()
        except:	
            traceback.print_exc(file=sys.stderr)

        logging.debug("Free nodes: " + str(allocated_nodes))         

        for i in allocated_nodes:
            logging.debug("free node: " + str(i) + " current busy nodes: " + str(self.busynodes) 
                          + " free nodes: " + str(self.freenodes))       
            self.busynodes.remove(i)
            self.freenodes.append(i)
        logging.debug("Delete " + machine_file_name)
        if os.path.exists(machine_file_name):
            os.remove(machine_file_name)
        self.resource_lock.release()
               
            
    def get_machine_file_name(self, job_dict):
        """create machinefile based on jobid"""
        job_id = job_dict["job-id"]                
        homedir = os.path.expanduser('~')
        return homedir  + "/advert-launcher-machines-"+ job_id
        
    def dequeue_new_jobs(self):	    
        """Subscribe to new jobs from Redis. """ 
        job_counter = 0               
        while self.is_stopped(self.base_url)==False:     
            if len(self.freenodes)==0:
                time.sleep(3)
                continue
            logging.debug("Dequeue sub-job from: " + self.base_url)       
            job_url=self.coordination.dequeue_job(self.base_url)
            if job_url==None:
                time.sleep(3)
                continue
            if job_url=="STOP":
                break
            
            job_counter = job_counter + 1            
            if (job_counter % (THREAD_POOL_SIZE))==0: # ensure that threadpool is not too overloaded
                self.threadpool.wait()
            
            request = WorkRequest(self.start_new_job_in_thread, [job_url])
            self.threadpool.putRequest(request)
            
        # wait for termination of Worker Threads
        self.threadpool.wait()   
        logging.debug("Terminating Agent - Dequeue Sub-Jobs Thread")   
       
    #def poll_jobs(self):       
    #    self.threadpool.wait()
    #    new_jobs=self.redis.keys(self.base_url+":*")   
    #    logging.debug("All jobs:" + str(new_jobs))
    #    for i in new_jobs:            
    #        request = WorkRequest(self.start_new_job_in_thread, [str(i)])
    #        logging.debug("WorkRequest: " + str(request))
    #        self.threadpool.putRequest(request)
        
    def start_new_job_in_thread(self, job_url):
        """evaluates job dir, sanity checks, executes job """
        #pdb.set_trace()
        if job_url != None:
            failed = False;
            try:
                job_dict = self.coordination.get_job(job_url)
            except:
                failed=True
                
            if job_dict==None or failed==True:
                self.coordination.queue_job(self.pilot_url, job_url)
                
            logging.debug("start job: " + job_url + " data: " + str(job_dict))
            if(job_dict["state"]==str(bigjob.state.Unknown)):
                job_dict["state"]=str(bigjob.state.New)
                self.coordination.set_job_state(job_url, str(bigjob.state.New))
            self.execute_job(job_url, job_dict)
            #print "Execute: " + str(job_dict)
    
    def monitor_jobs(self):
        """Monitor running processes. """   
        #pdb.set_trace()
        logging.debug("Monitor jobs - # current jobs: %d"%len(self.jobs))
        for i in self.jobs:
            if self.processes.has_key(i): # only if job has already been starteds
                p = self.processes[i]
                p_state = p.poll()
                logging.debug(self.print_job(i) + " state: " + str(p_state) + " return code: " + str(p.returncode))
                if (p_state != None and (p_state==0 or p_state==255)):
                    logging.debug("Job successful: " + self.print_job(i))
                    self.coordination.set_job_state(i, str(bigjob.state.Done))
                    #i.set_attribute("state", str(saga.job.Done))
                    self.free_nodes(i)
                    del self.processes[i]
                elif p_state!=0 and p_state!=255 and p_state != None:
                    logging.debug(self.print_job(i) + " failed.  ")
                    # do not free nodes => very likely the job will fail on these nodes
                    # self.free_nodes(i)                    
                    #if self.restarted.has_key(i)==False:
                    #    logging.debug("Try to restart job " + self.print_job(i))
                    #    self.restarted[i]=True
                    #    self.execute_job(i)                        
                    #else:
                    logging.debug("Job failed " + self.print_job(i))                    
                    self.coordination.set_job_state(i, str(bigjob.state.Failed))
                    self.free_nodes(i)
                    del self.processes[i]
    
    def print_job(self, job_url):
        job_dict = self.coordination.get_job(job_url)
        return  ("Job: " + job_url 
                 + " Executable: " + job_dict["Executable"])
                                
                            
    def start_background_thread(self):        
        self.stop=False                
        logging.debug("##################################### New POLL/MONITOR cycle ##################################")
        logging.debug("Free nodes: " + str(len(self.freenodes)) + " Busy Nodes: " + str(len(self.busynodes)))
        while True and self.stop==False:
            if self.is_stopped(self.base_url)==True:
                logging.debug("Pilot job entry deleted - terminate agent")
                break
            else:
                logging.debug("Pilot job entry: " + str(self.base_url) + " exists. Pilot job not in state stopped.")
            try:
                #self.poll_jobs()                
                self.monitor_jobs()            
                time.sleep(5)
                self.failed_polls=0
            except:
                traceback.print_exc(file=sys.stdout)
                self.failed_polls=self.failed_polls+1
                if self.failed_polls>3: # after 3 failed attempts exit
                    break
        logging.debug("Terminating Agent - Background Thread")
        
    
    def is_stopped(self, base_url):
        state = None
        try:
            state = self.coordination.get_pilot_state(base_url)
        except:
            pass
        logging.debug("Pilot State: " + str(state))
        if state==None or state.has_key("stopped")==False or state["stopped"]==True:
            return True
        else:
            return False
        

    def stop_background_thread(self):        
        self.stop=True
    
  
#########################################################
#  main                                                 #
#########################################################
if __name__ == "__main__" :
    args = sys.argv
    num_args = len(args)
    if (num_args!=3):
        print "Usage: \n " + args[0] + " <coordination host url> <coordination namespace url>"
        sys.exit(1)
    
    bigjob_agent = bigjob_agent(args)    
    
