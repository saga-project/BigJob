#!/usr/bin/env python

"""Module bigjob_manager.

This Module is used to launch jobs via a central distributed coordination service (e.g. an Redis or Advert instance). 

Background: This approach avoids queueing delays since only the BigJob-Agent must be started via saga.job. 
All shortrunning task will be started using the protocol implemented by subjob() and bigjob_agent.py
"""

import sys
import time
import os
import traceback
import logging
import textwrap
import urlparse
import types
import subprocess
import pdb
 
# the one and only saga
import saga
from saga.job import Description
from saga     import Url         as SAGAUrl
from saga.job import Description as SAGAJobDescription
from saga.job import Service     as SAGAJobService
from saga     import Session     as SAGASession
from saga     import Context     as SAGAContext

from radical.utils.object_cache import ObjectCache

from bigjob.state import Running, New, Failed, Done, Unknown
from bigjob import logger

# Optional Job Plugins
try:
    from job_plugin.gcessh import Service as GCEService
except:
    pass 

try:
    from job_plugin.ec2ssh import Service as EC2Service
except:
    pass  

try:
    from job_plugin.mesos.mesos_job import Service as MesosService
except:
    pass 

try:
    from job_plugin.yarn.yarn import Service as YarnService
except:
    pass 



# import other BigJob packages
# import API
import api.base
sys.path.append(os.path.dirname(__file__))

# Some python version detection
if sys.version_info < (2, 5):
    sys.path.append(os.path.dirname( __file__ ) + "/ext/uuid-1.30/")
    sys.stderr.write("Warning: Using unsupported Python version\n")

if sys.version_info < (2, 4):
    sys.stderr.write("Error: Python versions <2.4 not supported\n")
    sys.exit(-1)

import uuid

def get_uuid():
    wd_uuid=""
    wd_uuid += str(uuid.uuid1())
    return wd_uuid


""" Config parameters (will move to config file in future) """
_CLEANUP=True

#for legacy purposes and support for old BJ API
_pilot_url_dict={} # stores a mapping of pilot_url to bigjob

class BigJobError(Exception):
    def __init__(self, value):
        self.value = value
    
    def __str__(self):
        return repr(self.value)
    

class bigjob(api.base.bigjob):
    
    ''' BigJob: Class for managing pilot jobs:
    
        Example:        
        
        bj = bigjob("redis://localhost")
        bj.start_pilot_job("fork://localhost")
                        
        ..
        
        bj.cancel()                        
    '''
    
    __APPLICATION_NAME="bigjob" 
    
    def __init__(self, 
                 coordination_url="advert://localhost/?dbtype=sqlite3", 
                 pilot_url=None):    
        """ Initializes BigJob's coordination system
            advert://localhost (SAGA/Advert SQLITE)
            advert://advert.cct.lsu.edu:8080 (SAGA/Advert POSTGRESQL)
            redis://localhost:6379 (Redis at localhost)
            tcp://localhost (ZMQ)
            
            The following formats for pilot_url are supported:
            
            
            1.) Including root path at distributed coordination service:
            redis://localhost/bigjob:bj-1c3816f0-ad5f-11e1-b326-109addae22a3:localhost
            
            This path is returned when call bigjob.get_url()
            
            2.) BigJob unique ID:
            bigjob:bj-1c3816f0-ad5f-11e1-b326-109addae22a3:localhost
            
            
        """  
        
        self.coordination_url = coordination_url
        if self.coordination_url==None:
            logger.error("Coordination URL not set. Exiting BigJob.")
        #self.launch_method=""
        self.__filemanager=None
        self._ocache = ObjectCache ()
        
        # restore existing BJ or initialize new BJ
        if pilot_url!=None:
            logger.debug("Reconnect to BJ: %s"%pilot_url)
            if pilot_url.startswith("bigjob:"):
                self.pilot_url=pilot_url
            else:
                self.coordination_url, self.pilot_url = self.__parse_pilot_url(pilot_url)
                
            self.uuid = self.__get_bj_id(pilot_url)
            self.app_url = self.__APPLICATION_NAME +":" + str(self.uuid)
            self.job = None
            self.working_directory = None
            # Coordination subsystem must be initialized before get_state_detail
            self.coordination = self.__init_coordination(self.coordination_url)
            self.state=self.get_state_detail()
            _pilot_url_dict[self.pilot_url]=self
        else:
            self.coordination = self.__init_coordination(self.coordination_url)
            self.uuid = "bj-" + str(get_uuid())        
            logger.debug("init BigJob w/: " + coordination_url)
            self.app_url =self. __APPLICATION_NAME +":" + str(self.uuid) 
            self.state=Unknown
            self.pilot_url=""
            self.job = None
            self.working_directory = None
            logger.debug("initialized BigJob: " + self.app_url)
        

    def start_pilot_job(self, 
                 lrms_url, 
                 number_nodes=1,
                 queue=None,
                 project=None,
                 working_directory=None,
                 userproxy=None,
                 walltime=None,
                 processes_per_node=1,
                 filetransfers=None,
                 spmd_variation=None,
                 external_queue="",
                 pilot_compute_description=None):
        """ Start a batch job (using SAGA Job API) at resource manager. Currently, the following resource manager are supported:
            fork://localhost/ (Default Job Adaptor
            gram://qb1.loni.org/jobmanager-pbs (Globus Adaptor)
            pbspro://localhost (PBS Pro Adaptor)
        
        """         
        if self.job != None:
            raise BigJobError("One BigJob already active. Please stop BigJob first.") 
            return

        ##############################################################################
        # initialization of coordination and communication subsystem
        # Communication & Coordination initialization
        lrms_saga_url = SAGAUrl(lrms_url)
        self.url = lrms_saga_url
        self.pilot_url = self.app_url + ":" + lrms_saga_url.host
        self.number_nodes=int(number_nodes)*int(processes_per_node)  
        
        # Store references to BJ in global dict
        _pilot_url_dict[self.pilot_url]=self
        _pilot_url_dict[external_queue]=self
        
        logger.debug("create pilot job entry on backend server: " + self.pilot_url)
        self.coordination.set_pilot_state(self.pilot_url, str(Unknown), False)
        if pilot_compute_description==None:
            pilot_compute_description={"service_url": lrms_url, 
                                       "number_of_processes": number_nodes, 
                                       "processes_per_node": processes_per_node,
                                       "working_directory": working_directory}
        
        # put agent startup information into pilot description
        pilot_compute_description["coordination_host"]=self.coordination.get_address()
        pilot_compute_description["pilot_url"]=self.pilot_url
        pilot_compute_description["external_queue"]=external_queue
                 
        self.coordination.set_pilot_description(self.pilot_url, pilot_compute_description)
        
            
        logger.debug("set pilot state to: " + str(Unknown))

        # Create Job Service (Default: SAGA Job Service, alternative Job Services supported)
        self.js =None
        if lrms_saga_url.scheme=="gce+ssh":
            self.js = GCEService(lrms_saga_url, pilot_compute_description) 
        elif lrms_saga_url.scheme=="ec2+ssh" or lrms_saga_url.scheme=="euca+ssh" \
            or lrms_saga_url.scheme=="nova+ssh":
            self.js = EC2Service(lrms_saga_url, pilot_compute_description)    
        elif lrms_saga_url.scheme=="mesos":
            self.js = MesosService(lrms_saga_url, pilot_compute_description)          
        elif lrms_saga_url.scheme=="yarn":
            self.js = YarnService(lrms_saga_url, pilot_compute_description)          
        
        
        #elif lrms_saga_url.scheme=="slurm+ssh":
        #    self.js = SlurmService(lrms_saga_url, pilot_compute_description)          
        else:
            self.js = self._ocache.get_obj (lrms_saga_url, lambda : SAGAJobService (lrms_saga_url))
        ##############################################################################
        # create job description
        jd = SAGAJobDescription()
        
        #  Attempt to create working directory (e.g. in local scenario)
        if working_directory != None and working_directory != "":
            if not os.path.isdir(working_directory) \
                and (lrms_saga_url.scheme.startswith("fork") or lrms_saga_url.scheme.startswith("condor")) \
                and working_directory.startswith("go:")==False:
                os.mkdir(working_directory)
            self.working_directory = working_directory
        else:
            # if no working dir is set assume use home directory
            # will fail if home directory is not the same on remote machine
            # but this is just a guess to avoid failing
            self.working_directory = "~"
            #self.working_directory = "" 
        
        if queue != None:
            jd.queue = queue
        if spmd_variation != None:
            jd.spmd_variation = spmd_variation
        if project !=None:
            jd.project=project       
        if walltime!=None:
            logger.debug("setting walltime to: " + str(walltime))
            jd.wall_time_limit=int(walltime)

    
        
        ##############################################################################
        # File Management and Stage-In            
        # Determine whether target machine use gsissh or ssh to logon.
        # logger.debug("Detect launch method for: " + lrms_saga_url.host)        
        # self.launch_method = self.__get_launch_method(lrms_saga_url.host,lrms_saga_url.username)
        self.bigjob_working_directory_url=""
        if lrms_saga_url.scheme.startswith("gce") or lrms_saga_url.scheme.startswith("ec2")\
            or lrms_saga_url.scheme.startswith("euca") or lrms_saga_url.scheme.startswith("nova"):
            logger.debug("File Staging for Cloud Instances currently not supported.")
        elif lrms_saga_url.scheme.startswith("condor") == True:
            logger.debug("Using Condor file staging")
        else:           
            # build target url for working directory
            # this will also create the remote directory for the BJ
            # Fallback if working directory is not a valid URL
            if not (self.working_directory.startswith("go:") or self.working_directory.startswith("ssh://")):            
                if lrms_saga_url.username!=None and lrms_saga_url.username!="":
                    self.bigjob_working_directory_url = "ssh://" + lrms_saga_url.username + "@" + lrms_saga_url.host + "/" + self.__get_bigjob_working_dir()
                else:
                    self.bigjob_working_directory_url = "ssh://" + lrms_saga_url.host + "/" + self.__get_bigjob_working_dir()
            elif self.working_directory.startswith("go:"):
                    self.bigjob_working_directory_url=os.path.join(self.working_directory, self.uuid)
            else:
                # working directory is a valid file staging URL
                self.bigjob_working_directory_url=self.working_directory            
                
            # initialize file manager that takes care of file movement and directory creation
            if self.__filemanager==None:
                self.__initialize_pilot_data(self.bigjob_working_directory_url) # determines the url
            
            if self.__filemanager != None and not self.working_directory.startswith("/"):
                self.working_directory = self.__filemanager.get_path(self.bigjob_working_directory_url)
            
            # determine working directory of bigjob 
            # if a remote sandbox can be created via ssh => create a own dir for each bj job id
            # otherwise use specified working directory
            logger.debug("BigJob working directory: %s"%self.bigjob_working_directory_url)
            if self.__filemanager!=None and self.__filemanager.create_remote_directory(self.bigjob_working_directory_url)==True:
                self.working_directory = self.__get_bigjob_working_dir()
                self.__stage_files(filetransfers, self.bigjob_working_directory_url)
            else:        
                logger.warn("No file staging adaptor found.")
            
            logger.debug("BJ Working Directory: %s", self.working_directory)      
        
        if lrms_saga_url.scheme.startswith("condor")==False:
            jd.working_directory = self.working_directory
        else:
            jd.working_directory=""
        
        
        
        ##############################################################################
        # Create and process BJ bootstrap script
        bootstrap_script = self.__generate_bootstrap_script(
                                                          self.coordination.get_address(), 
                                                          self.pilot_url, # Queue 1 used by this BJ object 
                                                          external_queue  # Queue 2 used by Pilot Compute Service 
                                                                          # or another external scheduler
                                                          )
        logger.debug("Adaptor specific modifications: "  + str(lrms_saga_url.scheme))
        bootstrap_script = self.__escape(bootstrap_script)
        logger.debug(bootstrap_script)
 

        # Define Agent Executable in Job description
        # in Condor case bootstrap script is staged 
        # (Python app cannot be passed inline in Condor job description)
        if lrms_saga_url.scheme.startswith("condor")==True:

            bootstrap_script = self.__generate_bootstrap_script_from_binary(
                                                          self.coordination.get_address(), 
                                                          self.pilot_url, # Queue 1 used by this BJ object 
                                                          external_queue  # Queue 2 used by Pilot Compute Service 
                                                                          # or another external scheduler
                                                          )
     
            condor_bootstrap_filename = os.path.join("/tmp", "bootstrap-"+str(self.uuid))
            condor_bootstrap_file = open(condor_bootstrap_filename, "w")
            condor_bootstrap_file.write(bootstrap_script)
            condor_bootstrap_file.close()
            logger.debug("Using Condor - bootstrap file: " + condor_bootstrap_filename)
           
            jd.executable = "/usr/bin/env"
            jd.arguments = ["python",  os.path.basename(condor_bootstrap_filename)]                
            if pilot_compute_description.has_key("candidate_hosts"):
                jd.candidate_hosts = pilot_compute_description["candidate_hosts"]
            bj_file_transfers = []
            file_transfer_spec = condor_bootstrap_filename + " > " + os.path.basename(condor_bootstrap_filename)
            bj_file_transfers.append(file_transfer_spec)
            output_file_name = "output-" + str(self.uuid) + ".tar.gz"
            #output_file_transfer_spec = os.path.join(self.working_directory, output_file_name) +" < " + output_file_name
            output_file_transfer_spec = output_file_name +" < " + output_file_name
            #output_file_transfer_spec = os.path.join(self.working_directory, "output.tar.gz") +" < output.tar.gz"
            #logger.debug("Output transfer: " + output_file_transfer_spec)
            #bj_file_transfers.append(output_file_transfer_spec)
            if filetransfers != None:
                for t in filetransfers:
                    bj_file_transfers.append(t)
            logger.debug("Condor file transfers: " + str(bj_file_transfers))
            jd.file_transfer = bj_file_transfers
        else:
            jd.total_cpu_count=int(number_nodes)                   
            jd.spmd_variation = "single"
            if pilot_compute_description!=None and pilot_compute_description.has_key("spmd_variation"):
                jd.spmd_variation=pilot_compute_description["spmd_variation"]
            jd.arguments = ["python", "-c", bootstrap_script]
            jd.executable = "/usr/bin/env"           
      
        logger.debug("Working directory: " + jd.working_directory + " Job Description: " + str(jd))
        
        jd.output = os.path.join(self.working_directory, "stdout-" + self.uuid + "-agent.txt")
        jd.error = os.path.join(self.working_directory, "stderr-" + self.uuid + "-agent.txt")
      
        ##############################################################################
        # Create and submit pilot job to job service
        logger.debug("Creating pilot job with description: %s" % str(jd))
        self.job = self.js.create_job(jd)
        logger.debug("Trying to submit pilot job to: " + str(lrms_saga_url))
        self.job.run()

        if self.job.get_state() == saga.job.FAILED:
            logger.debug("SUBMISSION FAILED. Exiting... ")
            sys.exit(-1)
        else:
            logger.debug("Submission succeeded. Job ID: %s " % self.job.id)

        return self.pilot_url
    
    def get_nodes(self):        
        while True:
            nodeStr = self.coordination.get_nodes(self.pilot_url)
            if nodeStr != None:
                break
            time.sleep(2)        
        nodeList = eval(nodeStr)
        nodeList = [i.replace('\n','') for i in nodeList]
        return nodeList
        
    
    def list_subjobs(self):
        sj_list = self.coordination.get_jobs_of_pilot(self.pilot_url)
        logger.debug(str(sj_list))
        subjobs = []
        for i in sj_list:
            url = i 
            #if url.find("/")>0:
            #    url = url[url.find("bigjob"):]
            #    url =  url.replace("/", ":")    
            #sj = subjob(coordination_url=self.coordination_url, subjob_url=url)
            sj = subjob(subjob_url=url)
            subjobs.append(sj.get_url())
        return subjobs                  

     
    def get_state(self):        
        """ duck typing for get_state of saga.job.job  
            state of saga job that is used to spawn the pilot agent
        """
        return self.get_state_detail()
        
    
    def get_state_detail(self): 
        """ internal state of BigJob agent """ 
        try:
            return self.coordination.get_pilot_state(self.pilot_url)["state"]
        except:
            return None
        
        
    def get_url(self):
        """ Get unique URL of big-job. This URL can be used to reconnect to BJ later, e.g.:
        
            redis://localhost/bigjob:bj-1c3816f0-ad5f-11e1-b326-109addae22a3:localhost             
        
        """
        url = os.path.join(self.coordination.address, 
                           self.pilot_url)        
        if self.coordination.dbtype!="" and self.coordination.dbtype!=None:
            url = os.path.join(url, "?" + self.coordination.dbtype)            
        return url    
     
    
    def get_free_nodes(self):
        """ Returns number of free nodes of subjob 
            
            Attention: Only evaluates jobs directly submitted to BigJob. 
            This method cannot be used if Pilot is managed by a ComputeDataService.
        """
        jobs = self.coordination.get_jobs_of_pilot(self.pilot_url)
        number_used_nodes=0
        for i in jobs:
            job_detail = self.coordination.get_job(i)            
            if job_detail != None and job_detail.has_key("state") == True\
                and job_detail["state"]==str(Running):
                job_np = "1"
                if (job_detail["NumberOfProcesses"] == True):
                    job_np = job_detail["NumberOfProcesses"]
                number_used_nodes=number_used_nodes + int(job_np)
        return (self.number_nodes - number_used_nodes)


    def get_details(self):
        """ Return details with respect to PilotCompute (BigJob) Instance """
        details_dict={}
        try:
            details_dict = self.coordination.get_pilot_state(self.pilot_url)
        except:
            pass
        details_dict["bigjob_id"] = self.pilot_url
        return details_dict
    

    ###############################################################################################################    
    def cancel(self):        
        """ duck typing for cancel of saga.cpr.job and saga.job.job  """
        logger.debug("Cancel Pilot Job")
        try:
            self.job.cancel()
        except:
            pass
            #traceback.print_stack()

        logger.debug("Cancel Job Service")
        try:
            if  not self._ocache.rem_obj (self.js) :
                logger.debug("Cancel Job Service")
                del (self.js)
            else :
                logger.debug("Cancel Job Service done")

            self.js = None
        except:
            pass
            #traceback.print_stack()

        try:            
            self._stop_pilot_job()
            logger.debug("delete pilot job: " + str(self.pilot_url))                      
            if _CLEANUP:
                self.coordination.delete_pilot(self.pilot_url)                    
            #os.remove(os.path.join("/tmp", "bootstrap-"+str(self.uuid)))            
        except:            
            pass
            #traceback.print_stack()
        logger.debug("Cancel Pilot Job finished")
        

    def wait(self):
        """ Waits for completion of all sub-jobs """        
        while 1:
            if self.get_state()=="Done" or self.get_state()=="Failed":
                logger.debug("BigJob terminated. Exit Wait")
                break
            
            jobs = self.coordination.get_jobs_of_pilot(self.pilot_url)
            finish_counter=0
            result_map = {}
            for i in jobs:
                # parse job id out of sj url
                surl = SAGAUrl(i)
                sj_id = surl.path
                if sj_id.startswith("/"): sj_id = sj_id[1:]
                state = str(self.coordination.get_job_state(sj_id))
                #logger.debug("SJ: %s : State: %s"%(sj_id, str(state)))   
                #state = job_detail["state"]                
                if result_map.has_key(state)==False:
                    result_map[state]=1
                else:
                    result_map[state] = result_map[state]+1
                if self.__has_finished(state)==True:
                    finish_counter = finish_counter + 1                   
            logger.debug("Total Jobs: %s States: %s"%(len(jobs), str(result_map)))
            if finish_counter == len(jobs):
                break
            time.sleep(2)
    
        self.coordination.cancel()

    ###########################################################################
    # internal and protected methods
    def _stop_pilot_job(self):
        """ mark in database entry of pilot-job as stopped """
        try:
            logger.debug("stop pilot job: " + self.pilot_url)
            self.coordination.set_pilot_state(self.pilot_url, str(Done), True)            
            self.job=None
        except:
            pass
        self.coordination.cancel()
        
        
    def _delete_subjob(self, job_url):
        self.coordination.delete_job(job_url) 
    
    def _get_subjob_state(self, job_url):
        logger.debug("Get subjob state: " + str(job_url))
        return self.coordination.get_job_state(job_url) 
    
    def _get_subjob_details(self, job_url):
        return self.coordination.get_job(job_url) 
    
    def _add_subjob(self, queue_url, jd, job_url, job_id):
        logger.debug("add subjob to queue of PJ: " + str(queue_url))        
        for i in range(0,3):
            try:
                logger.debug("create dictionary for job description. Job-URL: " + job_url)
                # put job description attributes to Coordination Service
                job_dict = {}
                # to accomendate current bug in saga (Number of processes is not returned from list attributes)
                job_dict["NumberOfProcesses"] = "1" 
                attributes = jd.list_attributes()   
                logger.debug("SJ Attributes: " + str(jd))             
                for i in attributes:          
                        if jd.attribute_is_vector(i):
                            vector_attr = []
                            for j in jd.get_vector_attribute(i):
                                vector_attr.append(j)
                            job_dict[i]=vector_attr
                        else:
                            #logger.debug("Add attribute: " + str(i) + " Value: " + jd.get_attribute(i))
                            job_dict[i] = jd.get_attribute(i)
                
                # Other pilot state information
                job_dict["state"] = str(Unknown)
                job_dict["job-id"] = str(job_id)
                logger.debug("job dict: " + str(job_dict))
                
                
                if job_dict.has_key("FileTransfer"):
                    files = job_dict["FileTransfer"]
                    sj_work_dir = self.__get_subjob_working_dir(job_id)
                    self.__stage_files(files, sj_work_dir)
                
                #logger.debug("update job description at communication & coordination sub-system")
                self.coordination.set_job(job_url, job_dict)                                                
                self.coordination.queue_job(queue_url, job_url)
                break
            except:
                self.__print_traceback()
                time.sleep(2)
                
    
    def _get_subjob_url(self, subjob_url):
        """ Get unique URL for a sub-job. This URL can be used to reconnect to SJ later, e.g.:
        
            redis://localhost/bigjob:bj-9a9ba4d8-b162-11e1-9c42-109addae22a3:localhost:jobs:sj-6f44da6e-b178-11e1-bc99-109addae22a3
        """
        url = subjob_url
        if subjob_url.find("bigjob")==0:
            url = os.path.join(self.coordination.address, 
                               subjob_url)        
            if self.coordination.dbtype!="" and self.coordination.dbtype!=None:
                url = os.path.join(url, "?" + self.coordination.dbtype)            
        return url


    def __generate_bootstrap_script(self, coordination_host, coordination_namespace, external_coordination_namespace="", bigjob_home=None):
        script = textwrap.dedent("""import sys
import os
import urllib
import sys
import time
start_time = time.time()
home = os.environ.get("HOME")
#print "Home: " + home
if home==None: home = os.getcwd()
BIGJOB_AGENT_DIR= os.path.join(home, ".bigjob")
if not os.path.exists(BIGJOB_AGENT_DIR): os.mkdir (BIGJOB_AGENT_DIR)
BIGJOB_PYTHON_DIR=BIGJOB_AGENT_DIR+"/python/"
if not os.path.exists(BIGJOB_PYTHON_DIR): os.mkdir(BIGJOB_PYTHON_DIR)
BOOTSTRAP_URL="https://raw.github.com/saga-project/BigJob/master/bootstrap/bigjob-bootstrap.py"
BOOTSTRAP_FILE=BIGJOB_AGENT_DIR+"/bigjob-bootstrap.py"
#ensure that BJ in .bigjob is upfront in sys.path
sys.path.insert(0, os.getcwd() + "/../")
p = list()
for i in sys.path:
    if i.find(\".bigjob/python\")>1:
          p.insert(0, i)
for i in p: sys.path.insert(0, i)
print "Python path: " + str(sys.path)
print "Python version: " + str(sys.version_info)
try: import saga
except: print "SAGA not found.";
try: import bigjob.bigjob_agent
except: 
    print "BigJob not installed. Attempt to install it."; 
    try:
        opener = urllib.FancyURLopener({}); 
        opener.retrieve(BOOTSTRAP_URL, BOOTSTRAP_FILE);
    except Exception, ex:
        print "Unable to download bootstrap script: " + str(ex) + ". Please install BigJob manually."
    print "Execute: " + "python " + BOOTSTRAP_FILE + " " + BIGJOB_PYTHON_DIR
    os.system("/usr/bin/env")
    try:
        os.system("python " + BOOTSTRAP_FILE + " " + BIGJOB_PYTHON_DIR); 
        activate_this = os.path.join(BIGJOB_PYTHON_DIR, "bin/activate_this.py"); 
        execfile(activate_this, dict(__file__=activate_this))
    except:
        print "BJ installation failed. Trying system-level python (/usr/bin/python)";
        os.system("/usr/bin/python " + BOOTSTRAP_FILE + " " + BIGJOB_PYTHON_DIR); 
        activate_this = os.path.join(BIGJOB_PYTHON_DIR, "bin/activate_this.py"); 
        execfile(activate_this, dict(__file__=activate_this))
#try to import BJ once again
try:
    import bigjob.bigjob_agent
except Exception, ex:
        print "Unable install BigJob: " + str(ex) + ". Please install BigJob manually."   
# execute bj agent
args = list()
args.append("bigjob_agent.py")
args.append(\"%s\")
args.append(\"%s\")
args.append(\"%s\")
print "Bootstrap time: " + str(time.time()-start_time)
print "Starting BigJob Agents with following args: " + str(args)
bigjob_agent = bigjob.bigjob_agent.bigjob_agent(args)
""" % (coordination_host, coordination_namespace, external_coordination_namespace))
        return script
    
    
    def __generate_bootstrap_script_from_binary(self, coordination_host, coordination_namespace, 
                                                external_coordination_namespace="", 
                                                bigjob_home=None):
        script = textwrap.dedent("""import sys
import os
import urllib
import sys
import time
start_time = time.time()
home = os.environ.get("HOME")
#print "Home: " + home
if home==None: home = os.getcwd()
BIGJOB_AGENT_DIR= os.path.join(home, ".bigjob")
if not os.path.exists(BIGJOB_AGENT_DIR): os.mkdir (BIGJOB_AGENT_DIR)
BIGJOB_PYTHON_DIR=BIGJOB_AGENT_DIR+"/python/"
if not os.path.exists(BIGJOB_PYTHON_DIR): os.mkdir(BIGJOB_PYTHON_DIR)
BOOTSTRAP_URL="http://s3.amazonaws.com/bigjob/bigjob-Linux-x86_64.tar.gz"
BOOTSTRAP_FILE="bigjob-Linux-x86_64.tar.gz"
#ensure that BJ in .bigjob is upfront in sys.path
sys.path.insert(0, os.getcwd() + "/../")
#sys.path.insert(0, /User/luckow/.bigjob/python/lib")
#sys.path.insert(0, os.getcwd() + "/../../")
p = list()
for i in sys.path:
    if i.find(\".bigjob/python\")>1:
          p.insert(0, i)
for i in p: sys.path.insert(0, i)
print "Python path: " + str(sys.path)
print "Python version: " + str(sys.version_info)
try: import saga
except: print "SAGA and SAGA Python Bindings not found.";
try: import bigjob.bigjob_agent
except: 
    print "BigJob not installed. Attempt to install it."; 
    #opener = urllib.FancyURLopener({}); 
    #ret = opener.retrieve(BOOTSTRAP_URL, BOOTSTRAP_FILE); 
    print "Download via wget"
    os.system("wget " + BOOTSTRAP_URL)
    os.system("rm -rf .bigjob")
    print "Execute: " + "tar -xzf " + BOOTSTRAP_FILE
    os.system("ls -lta")
    try:
        os.system("tar -xzf " + BOOTSTRAP_FILE); 
        os.system("ls -lta")
        os.system(".bigjob/python/bin/python -c 'import bigjob; import bigjob.bigjob_agent; print bigjob.version; bigjob.bigjob_agent.bigjob_agent([\\"bigjob_agent.py\\", \\"%s\\", \\"%s\\", \\"%s\\"])'")
        
    except:
        print "BJ installation failed!";
""" % (coordination_host, coordination_namespace, external_coordination_namespace))
        return script


    def __escape(self, bootstrap_script):
        logger.debug("Escape bootstrap script")
        bootstrap_script = "\'" + bootstrap_script+ "\'"
        return bootstrap_script
    
    
    def __escape_rsl(self, bootstrap_script):
        logger.debug("Escape RSL")
        bootstrap_script = bootstrap_script.replace("\"", "\"\"")        
        return bootstrap_script
    
    
    def __escape_ssh(self, bootstrap_script):
        logger.debug("Escape SSH")
        bootstrap_script = bootstrap_script.replace("\"", "\\\"")
        bootstrap_script = bootstrap_script.replace("\'", "\\\"")
        bootstrap_script = "\"" + bootstrap_script+ "\""
        return bootstrap_script
    
    def __escape_saga(self, bootstrap_script):
        logger.debug("Escape SAGA")
        #bootstrap_script = bootstrap_script.replace("\'", "\"")
        #bootstrap_script = "\'" + bootstrap_script+ "\'"
        bootstrap_script = bootstrap_script.replace('"','\\"')
        bootstrap_script = '"' + bootstrap_script+ '"'
        return bootstrap_script
                
  
    def __parse_pilot_url(self, pilot_url):
        #pdb.set_trace()        
        pilot_saga_url = SAGAUrl(pilot_url)
        dbtype = pilot_saga_url.query
        coordination = pilot_url[:pilot_url.index("bigjob")]   
        if dbtype!=None:
            coordination = os.path.join(coordination, "?"+dbtype)
        pilot_url = pilot_saga_url.path[1:]
        
        logger.debug("Parsed URL - Coordination: %s Pilot: %s"%(coordination, pilot_url))    
        return coordination, pilot_url
    
    
    def __has_finished(self, state):
        state = state.lower()
        if state=="done" or state=="failed" or state=="canceled":
            return True
        else:
            return False
    
    def __parse_url(self, url):
        try:
            surl = SAGAUrl(url)
            host = surl.host
            port = surl.port
            username = surl.username
            password = surl.password
            query = surl.query
            if query!=None and query.endswith("/"):
                query = query[:-1]
            scheme = "%s://"%surl.scheme
        except:
            """ Fallback URL parser based on Python urlparse library """
            logger.error("URL %s could not be parsed"%(url))
            traceback.print_exc(file=sys.stderr)
            result = urlparse.urlparse(url)
            logger.debug("Result: " + str(result))
            host = result.hostname
            #host = None
            port = result.port
            username = result.username
            password = result.password
            scheme = "%s://"%result.scheme 
            if host==None:
                logger.debug("Python 2.6 fallback")
                if url.find("/", len(scheme)) > 0:
                    host = url[len(scheme):url.find("/", len(scheme))]
                else:
                    host = url[len(scheme):]
                if host.find(":")>1:
                    logger.debug(host)
                    comp = host.split(":")
                    host = comp[0]
                    port = int(comp[1])
                    
            if url.find("?")>0:
                query = url[url.find("?")+1:]
            else:
                query = None
            
        
        logger.debug("%s %s %s"%(scheme, host, port))
        return scheme, username, password, host, port, query     
            
    def __get_bj_id(self, pilot_url):
        start = pilot_url.index("bj-")
        end =pilot_url.index(":", start)
        return pilot_url[start:end]
    
     
    def __init_coordination(self, coordination_url):        
        
        bigjob_coordination = None
        if(coordination_url.startswith("advert://") or coordination_url.startswith("sqlasyncadvert://")):
            try:
                from coordination.bigjob_coordination_advert import bigjob_coordination
                logger.debug("Utilizing ADVERT Backend")
            except:
                logger.error("Advert Backend could not be loaded")
        elif (coordination_url.startswith("redis://")):
            try:
                from coordination.bigjob_coordination_redis import bigjob_coordination      
                logger.debug("Utilizing Redis Backend")
            except:
                logger.error("Error loading pyredis.")
                self.__print_traceback()
        elif (coordination_url.startswith("tcp://")):
            try:
                from coordination.bigjob_coordination_zmq import bigjob_coordination
                logger.debug("Utilizing ZMQ Backend")
            except:
                logger.error("ZMQ Backend not found. Please install ZeroMQ (http://www.zeromq.org/intro:get-the-software) and " 
                      +"PYZMQ (http://zeromq.github.com/pyzmq/)")
        else:
            logger.error("No suitable coordination backend found.")
        
        # check whether coordination subsystem could be initialized
        if bigjob_coordination==None:
            raise BigJobError("Could not initialize coordination subsystem (Redis)")
        
        
        logger.debug("Parsing URL: " + coordination_url)
        scheme, username, password, host, port, dbtype  = self.__parse_url(coordination_url) 
        
        if port == -1:
            port = None
        coordination = bigjob_coordination(server=host, server_port=port, username=username, 
                                           password=password, dbtype=dbtype, url_prefix=scheme)
        return coordination
    
    
    def __get_bigjob_working_dir(self):
        self.working_directory = os.path.abspath(os.path.expanduser(self.working_directory))        
        if self.working_directory.find(self.uuid)!=-1: # working directory already contains BJ id
            return self.working_directory
        else:
            return os.path.join(self.working_directory, self.uuid)
    
    
    def __get_subjob_working_dir(self, sj_id):
        base_url = self.bigjob_working_directory_url 
        url  = os.path.join(base_url, sj_id)        
        return url
    

    ###########################################################################
    # File Management
    
    def __initialize_pilot_data(self, service_url):
        # initialize file adaptor
        # Pilot Data API for File Management
        if service_url.startswith("ssh:"):
            logger.debug("Use SSH backend for PilotData")
            try:
                from pilot.filemanagement.ssh_adaptor import SSHFileAdaptor
                self.__filemanager = SSHFileAdaptor(service_url) 
            except:
                logger.debug("SSH package not found.")            
                self.__print_traceback()
        elif service_url.startswith("http:"):
            logger.debug("Use WebHDFS backend")
            try:
                from pilot.filemanagement.webhdfs_adaptor import WebHDFSFileAdaptor
                self.__filemanager = WebHDFSFileAdaptor(service_url)
            except:
                logger.debug("WebHDFS package not found.")        
        elif service_url.startswith("go:"):
            logger.debug("Use Globus Online backend")
            try:
                from pilot.filemanagement.globusonline_adaptor import GlobusOnlineFileAdaptor
                self.__filemanager = GlobusOnlineFileAdaptor(service_url)
            except:
                logger.debug("Globus Online package not found.") 
                self.__print_traceback()
                
            
                  

    def __stage_files(self, filetransfers, target_url):
        logger.debug("Stage: %s to %s"%(filetransfers, target_url))
        if filetransfers==None:
            return
        if self.__filemanager:
            self.__filemanager.create_remote_directory(target_url)
        for i in filetransfers:
            source_file=i
            if i.find(">")>0:
                source_file = i[:i.find(">")].strip()
            if source_file.startswith("ssh://")==False and source_file.startswith("go://")==False:
                logger.error("Staging of file: %s not supported. Please use URL in form ssh://<filename>"%source_file)
                continue
            target_url_full = os.path.join(target_url, os.path.basename(source_file))
            logger.debug("Stage: %s to %s"%(source_file, target_url_full))
            #self.__third_party_transfer(source_file, target_url_full)
            if self.__filemanager:
                self.__filemanager.transfer(source_file, target_url_full)
           
       

    def __get_launch_method(self, hostname, user=None):
        """ returns desired execution method: ssh, aprun """
        if user == None: user = self.__discover_ssh_user(hostname)
        host = ""
        if user!=None and user!="": 
            logger.debug("discovered user: " + user)
            host = user + "@" + hostname
        else:
            host = hostname 
        gsissh_available = False
        try:
            cmd = "gsissh " + host + " /bin/date"
            logger.debug("Execute: " + cmd)
            gsissh_available = (subprocess.call(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)==0)
        except:
            pass
    
        ssh_available = False
        try:
            cmd = "ssh " + host + " /bin/date"
            logger.debug("Execute: " + cmd)
            ssh_available = (subprocess.call(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)==0)
        except:
            pass
    
        launch_method = "ssh"
        if ssh_available == False and gsissh_available == True:
            launch_method="gsissh"
        else:
            launch_method="ssh"
        logger.info("SSH: %r GSISSH: %r Use: %s"%(ssh_available, gsissh_available, launch_method))
        return launch_method
    
    
    def __discover_ssh_user(self, hostname):
        # discover username
        user = None
        ssh_config = os.path.join(os.path.expanduser("~"), ".ssh/config")
        ssh_config_file = open(ssh_config, "r")
        lines = ssh_config_file.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            if line.find(hostname)>0:
                for k in range(i + 1, len(lines)):
                    sub_line = lines[k]
                    if sub_line.startswith(" ")==True and sub_line.startswith("\t")==True:
                        break # configuration for next host
                    elif sub_line.find("User")!=-1:
                        stripped_sub_line = sub_line.strip()
                        user = stripped_sub_line.split()[1]
                        break
        ssh_config_file.close() 
        return user
    
    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logger.debug("*** print_exception:",
                     exc_info=(exc_type, exc_value, exc_traceback))
        #traceback.print_exception(exc_type, exc_value, exc_traceback,
        #                      limit=2, file=sys.stdout)
        
    def __repr__(self):
        return self.pilot_url 

                    
                    
class subjob(api.base.subjob):
    
    def __init__(self, coordination_url=None, subjob_url=None):
        """Constructor"""
        
        self.coordination_url = coordination_url
        if subjob_url!=None:
            self.job_url = subjob_url[subjob_url.index("bigjob"):]            
            if self.coordination_url==None:
                self.coordination_url, self.job_url=self.__parse_subjob_url(subjob_url)
            self.uuid = self.__get_sj_id(subjob_url)
            self.pilot_url = self.__get_pilot_url(subjob_url)
            if self.pilot_url.startswith("bigjob"):
                self.pilot_url=os.path.join(self.coordination_url, self.pilot_url)
            
            self.bj = bigjob(pilot_url=self.pilot_url)
            logger.debug("Reconnect SJ: %s Pilot %s"%(self.job_url, self.pilot_url))
        else:
            self.uuid = "sj-" + str(get_uuid())
            self.job_url = None
            self.pilot_url = None
            self.bj = None
    
    
    def get_url(self):
        if self.job_url==None:
            self.job_url=self.__get_subjob_url(self.pilot_url)
        return self.bj._get_subjob_url(self.job_url)
    

    def submit_job(self, pilot_url, jd):
        """ submit subjob to referenced bigjob """
        if self.job_url==None:
            self.job_url=self.__get_subjob_url(pilot_url)            
        
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=_pilot_url_dict[pilot_url]    
        self.bj._add_subjob(pilot_url, jd, self.job_url, self.uuid)


    def get_state(self, pilot_url=None):        
        """ duck typing for saga.job  """
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=_pilot_url_dict[pilot_url]                
        return self.bj._get_subjob_state(self.job_url)
    
    
    def cancel(self, pilot_url=None):
        logger.debug("delete job: " + self.job_url)
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=_pilot_url_dict[pilot_url]  
        if str(self.bj.get_state())=="Running":
            self.bj._delete_subjob(self.job_url)        
        
        
    def get_exe(self, pilot_url=None):
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=_pilot_url_dict[pilot_url]  
        sj = self.bj._get_subjob_details(self.job_url)
        return sj["Executable"]
    
    
    def get_details(self, pilot_url=None):
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=_pilot_url_dict[pilot_url]  
        sj = self.bj._get_subjob_details(self.job_url)
        return sj
   
   
    def get_arguments(self, pilot_url=None):
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=_pilot_url_dict[pilot_url]  
        sj = self.bj.get_subjob_details(self.job_url)  
        #logger.debug("Subjob details: " + str(sj))              
        arguments=""
        for  i in  sj["Arguments"]:
            arguments = arguments + " " + i
        return arguments
    
      
    def __repr__(self):        
        if(self.job_url==None):
            return "None"
        else:
            return self.job_url
    
    ###########################################################################
    # Internal and protected methods
        
    def __get_sj_id(self, job_url):
        start = job_url.index("sj-")        
        return job_url[start:]
    
    
    def __get_pilot_url(self, job_url):
        end =job_url.index(":jobs")
        
        # Make sure that DB type is appended
        surl = SAGAUrl(job_url)
        query = surl.query
        pilot_url=job_url[:end]
        if query!=None and query !="":
            pilot_url = pilot_url + "?"+query
        return pilot_url
    
                    
    def __get_subjob_url(self, pilot_url):
        if pilot_url.find("bigjob")>1:
            pilot_url = pilot_url[pilot_url.find("bigjob"):]
        if pilot_url.endswith("/"):
            pilot_url = pilot_url[:-1]
        self.job_url = pilot_url + ":jobs:" + str(self.uuid)
        return self.job_url
    
    def __parse_subjob_url(self, subjob_url):
        #pdb.set_trace()    
        subjob_saga_url = SAGAUrl(subjob_url)    
        dbtype = subjob_saga_url.query
        coordination = subjob_url[:subjob_url.index("bigjob")]   
        if dbtype!=None:
            coordination = os.path.join(coordination, "?"+dbtype)
        sj_url = subjob_saga_url.path[1:]
             
#        sj_url = subjob_url[subjob_url.find("bigjob"):]
#        if sj_url.find("/") > 0 or dbtype!=None or dbtype!="":
#            comp = sj_url.split("/")
#            sj_url = comp[0]
#            if comp[1].find("dbtype")>0:
#                dbtype=comp[1][comp[1].find("dbtype"):]
        
        logger.debug("Parsed URL - Coordination: %s Pilot: %s"%(coordination, sj_url))    
        return coordination, sj_url
        
        
###############################################################################
## Properties for description class
#

def input_data():
    doc = "List of input data units."
    def fget(self):
        return self._input_data
    def fset(self, val):
        self._input_data = val
    def fdel(self, val):
        self._input_data = None
    return locals()

def output_data():
    doc = "List of output data units."
    def fget(self):
        return self._output_data
    def fset(self, val):
        self._output_data = val
    def fdel(self, val):
        self._output_data = None
    return locals()

     
class description(SAGAJobDescription):
    """ Sub-job description """
    ##input_data  = property(**input_data())
    ##output_data = property(**output_data())   
    ##environment = {}
    
    # --------------------------------------------------------------------------
    #
    def __init__(self):
        saga.job.Description.__init__(self)
        #self.attributes_extensible_ (True)

        # Extend description class by Pilot-Data relevant attributes
        self._output_data = None
        self._input_data = None

        import saga.attributes as sa

        self._attributes_extensible  (True)
        self._attributes_camelcasing (True)


        self._attributes_register   ("InputData",  None, sa.ANY, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register   ("OutputData", None, sa.ANY, sa.VECTOR, sa.WRITEABLE)

        self._attributes_set_getter ("InputData",  self._get_input_data )
        self._attributes_set_getter ("OutputData", self._get_output_data)

    # --------------------------------------------------------------------------
    #
    def _get_input_data (self) :
        logger.debug("_get_input_data called. returning: %s" % self.input_data)
        return self.input_data

    # --------------------------------------------------------------------------
    #
    def _get_output_data (self) :
        return self.output_data



