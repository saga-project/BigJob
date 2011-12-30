#!/usr/bin/env python

"""Module bigjob_manager.

This Module is used to launch jobs via the advert service. 

It assumes that an bigjob_agent.py is available on the remote machine.
bigjob_agent.py will poll the advert service for new jobs and run these jobs on the respective
machine .

Background: This approach avoids queueing delays since the igjob_agent_launcher.py must be just started via saga.job or saga.cpr
once. All shortrunning task will be started using the protocol implemented by subjob() and bigjob_agent.py

Installation:
Set environment variable BIGJOB_HOME to installation directory
"""

import sys
from bigjob import logger
import time
import os
import traceback
import logging
import textwrap
import urlparse
import paramiko
import pdb

from bigjob import SAGA_BLISS 
from bigjob.state import *

if SAGA_BLISS == False:
    try:
        import saga
        logger.debug("Using SAGA C++/Python.")
        is_bliss=False
    except:
        logger.error("SAGA C++ and Python bindings not found. Using Bliss.")
        import bliss.sagacompat as saga
        is_bliss=True
else:
    logger.debug("Using SAGA Bliss.")
    import bliss.sagacompat as saga
    is_bliss=True 


# import other BigJob packages
# import API
import api.base
sys.path.append(os.path.dirname(__file__))

from pbsssh import pbsssh

if sys.version_info < (2, 5):
    sys.path.append(os.path.dirname( __file__ ) + "/ext/uuid-1.30/")
    sys.stderr.write("Warning: Using unsupported Python version\n")
if sys.version_info < (2, 4):
    sys.path.append(os.path.dirname( __file__ ) + "/ext/subprocess-2.6.4/")
    sys.stderr.write("Warning: Using unsupported Python version\n")
if sys.version_info < (2, 3):
    sys.stderr.write("Error: Python versions <2.3 not supported\n")
    sys.exit(-1)

import uuid

def get_uuid():
    wd_uuid=""
    wd_uuid += str(uuid.uuid1())
    return wd_uuid


""" Config parameters (will move to config file in future) """
CLEANUP=True

#for legacy purposes and support for old BJ API
pilot_url_dict={} # stores a mapping of pilot_url to bigjob



class BigJobError(Exception):
    def __init__(self, value):
        self.value = value
    
    def __str__(self):
        return repr(self.value)
    

class bigjob(api.base.bigjob):
    
    def __init__(self, coordination_url="advert://localhost/"):    
        """ Initializes BigJob's coordination system
            e.g.:
            advert://localhost (SAGA/Advert SQLITE)
            advert://advert.cct.lsu.edu:8080 (SAGA/Advert POSTGRESQL)
            redis://localhost:6379 (Redis at localhost)
            tcp://localhost (ZMQ)
        """  
        
        self.uuid = "bj-" + str(get_uuid())
        
        logger.debug("init BigJob w/: " + coordination_url)
        self.coordination_url = coordination_url
        self.coordination = self.__init_coordination(coordination_url)
        __APPLICATION_NAME="bigjob"        
        self.app_url = __APPLICATION_NAME +":" + str(self.uuid) 
        
        self.state=Unknown
        self.pilot_url=""
        self.job = None
        self.working_directory = None
        logger.debug("initialized BigJob: " + self.app_url)
        
        
    def __init_coordination(self, coordination_url):        
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
        elif (coordination_url.startswith("tcp://")):
            try:
                from coordination.bigjob_coordination_zmq import bigjob_coordination
                logger.debug("Utilizing ZMQ Backend")
            except:
                logger.error("ZMQ Backend not found. Please install ZeroMQ (http://www.zeromq.org/intro:get-the-software) and " 
                      +"PYZMQ (http://zeromq.github.com/pyzmq/)")
        else:
            logger.error("No suitable coordination backend found.")
        
        logger.debug("Parsing URL: " + coordination_url)
        scheme, username, password, host, port, dbtype  = self.__parse_url(coordination_url) 
        
        if port == -1:
            port = None
        coordination = bigjob_coordination(server=host, server_port=port, username=username, 
                                           password=password, dbtype=dbtype, url_prefix=scheme)
        return coordination
    
   
            
    def start_pilot_job(self, 
                 lrms_url, 
                 bigjob_agent_executable=None,
                 number_nodes=1,
                 queue=None,
                 project=None,
                 working_directory=None,
                 userproxy=None,
                 walltime=None,
                 processes_per_node=1,
                 filetransfers=None):
        """ Start a batch job (using SAGA Job API) at resource manager. Currently, the following resource manager are supported:
            fork://localhost/ (Default Job Adaptor
            gram://qb1.loni.org/jobmanager-pbs (Globus Adaptor)
            pbspro://localhost (PBS Prop Adaptor)
        
        """
         
        if self.job != None:
            raise BigJobError("One BigJob already active. Please stop BigJob first.") 
            return

        ##############################################################################
        # initialization of coordination and communication subsystem
        # Communication & Coordination initialization
        lrms_saga_url = saga.url(lrms_url)
        self.pilot_url = self.app_url + ":" + lrms_saga_url.host
        pilot_url_dict[self.pilot_url]=self
        
        logger.debug("create pilot job entry on backend server: " + self.pilot_url)
        self.coordination.set_pilot_state(self.pilot_url, str(Unknown), False)
                
        logger.debug("set pilot state to: " + str(Unknown))
        ##############################################################################
        
        self.number_nodes=int(number_nodes)        
        
        # create job description
        jd = saga.job.description()
        
        
        logger.debug("Adaptor specific modifications: "  + str(lrms_saga_url.scheme))
        if lrms_saga_url.scheme == "condorg":
            jd.arguments = [ "-a", self.coordination.get_address(), "-b",self.pilot_url]
            logger.debug("\n\n-a", self.coordination.get_address(),"-b", self.pilot_url)
            agent_exe = os.path.abspath(os.path.join(os.getcwd(),"..","bootstrap","bigjob-condor-bootstrap.py"))
            logger.debug(agent_exe) 
            jd.executable = agent_exe
            
        else:
            bootstrap_script = self.generate_bootstrap_script(self.coordination.get_address(), self.pilot_url)
            if lrms_saga_url.scheme == "gram":
                bootstrap_script = self.escape_rsl(bootstrap_script)
            elif lrms_saga_url.scheme == "pbspro":                
                bootstrap_script = self.escape_pbs(bootstrap_script)
            elif lrms_saga_url.scheme == "ssh":
                bootstrap_script = self.escape_ssh(bootstrap_script)
            ############ submit pbs script which launches bigjob agent using ssh adaptors########## 
            elif lrms_saga_url.scheme == "pbs-ssh":
                # change the url scheme ssh to use ssh adaptors to launch job
                bootstrap_script = self.escape_ssh(bootstrap_script)
                ### convert walltime in minutes to PBS representation of time ###
                hrs=walltime/60 
                minu=walltime%60 
                walltimepbs=""+str(hrs)+":"+str(minu)+":00"
                if number_nodes%processes_per_node == 0:
                    number_nodes = number_nodes/processes_per_node
                else:
                    number_nodes = ( number_nodes/processes_per_node) + 1
                pbssshj = pbsssh(bootstrap_script,lrms_saga_url, walltimepbs,number_nodes,processes_per_node,userproxy,working_directory)
                self.job = pbssshj
                self.job.run()
                return
            elif is_bliss:
                bootstrap_script = self.escape_bliss(bootstrap_script)

            #logger.debug(bootstrap_script)
            if is_bliss==False:
                jd.number_of_processes = str(number_nodes)
                jd.processes_per_host=str(processes_per_node)
            else:
                jd.TotalCPUCount=str(int(number_nodes)*int(processes_per_node))
                
            jd.spmd_variation = "single"
            #jd.arguments = [bigjob_agent_executable, self.coordination.get_address(), self.pilot_url]
            jd.arguments = ["-c", bootstrap_script]
            jd.executable = "python"
            if queue != None:
                jd.queue = queue
            if project !=None:
                jd.job_project = [project]
            if walltime!=None:
                jd.wall_time_limit=str(walltime)
        
            # XXX Isn't the working directory about the remote site?
            if working_directory != None:
                if not os.path.isdir(working_directory) and lrms_saga_url.scheme=="fork":
                    os.mkdir(working_directory)
                self.working_directory = working_directory
            else:
                self.working_directory = os.path.expanduser("~")
    
            jd.working_directory = self.working_directory
    
            logger.debug("Working directory: " + jd.working_directory)
            jd.output = os.path.join(self.__get_bigjob_working_dir(), "stdout-bigjob_agent.txt")
            jd.error = os.path.join(self.__get_bigjob_working_dir(),"stderr-bigjob_agent.txt")
         
        # Stage BJ Input files
        # build target url
        bigjob_working_directory_url = "ssh://" + lrms_saga_url.host + self.__get_bigjob_working_dir()
        self.__stage_files(filetransfers, bigjob_working_directory_url)
           
        # Submit job
        js = None    
        if userproxy != None and userproxy != '':
            s = saga.session()
            os.environ["X509_USER_PROXY"]=userproxy
            ctx = saga.context("x509")
            ctx.set_attribute ("UserProxy", userproxy)
            s.add_context(ctx)
            logger.debug("use proxy: " + userproxy)
            js = saga.job.service(s, lrms_saga_url)
        else:
            logger.debug("use standard proxy")
            js = saga.job.service(lrms_saga_url)

        self.job = js.create_job(jd)
        logger.debug("Submit pilot job to: " + str(lrms_saga_url))
        self.job.run()
        #return self.job
        
    def generate_bootstrap_script(self, coordination_host, coordination_namespace):
        script = textwrap.dedent("""import sys
import os
import urllib
import sys
import time

start_time = time.time()

home = os.environ["HOME"]

BIGJOB_AGENT_DIR= os.path.join(home, ".bigjob")
if not os.path.exists(BIGJOB_AGENT_DIR): os.mkdir (BIGJOB_AGENT_DIR)
BIGJOB_PYTHON_DIR=BIGJOB_AGENT_DIR+"/python/"
BOOTSTRAP_URL="https://svn.cct.lsu.edu/repos/saga-projects/applications/bigjob/trunk/generic/bootstrap/bigjob-bootstrap.py"
BOOTSTRAP_FILE=BIGJOB_AGENT_DIR+"/bigjob-bootstrap.py"

try: import saga
except: print "SAGA and SAGA Python Bindings not found: BigJob only work w/ non-SAGA backends e.g. Redis, ZMQ.";print "Python version: ",  os.system("python -V");print "Python path: " + str(sys.path)

sys.path.insert(0, os.getcwd() + "/../")
sys.path.insert(0, os.getcwd() + "/../../")

try: import bigjob.bigjob_agent
except: print "BigJob not installed. Attempting to install it."; opener = urllib.FancyURLopener({}); opener.retrieve(BOOTSTRAP_URL, BOOTSTRAP_FILE); os.system("python " + BOOTSTRAP_FILE + " " + BIGJOB_PYTHON_DIR); activate_this = BIGJOB_PYTHON_DIR+'bin/activate_this.py'; execfile(activate_this, dict(__file__=activate_this))

#try to import BJ once again
import bigjob.bigjob_agent

# execute bj agent
args = ["bigjob_agent.py", \"%s\", \"%s\"]
print "Bootstrap time: " + str(time.time()-start_time)
print "Starting BigJob Agents with following args: " + str(args)
bigjob_agent = bigjob.bigjob_agent.bigjob_agent(args)
""" % (coordination_host, coordination_namespace))
        return script
    
    def escape_rsl(self, bootstrap_script):
        logger.debug("Escape RSL")
        bootstrap_script = bootstrap_script.replace("\"", "\"\"")
        return bootstrap_script
    
    
    def escape_pbs(self, bootstrap_script):
        logger.debug("Escape PBS")
        bootstrap_script = "\'" + bootstrap_script+ "\'"
        return bootstrap_script
    
    
    def escape_ssh(self, bootstrap_script):
        logger.debug("Escape SSH")
        bootstrap_script = bootstrap_script.replace("\"", "\\\"")
        bootstrap_script = bootstrap_script.replace("\'", "\\\"")
        bootstrap_script = "\"" + bootstrap_script+ "\""
        return bootstrap_script
    
    def escape_bliss(self, bootstrap_script):
        logger.debug("Escape fork")
        #bootstrap_script = bootstrap_script.replace("\"", "\\\"")
        bootstrap_script = bootstrap_script.replace("\'", "\"")
        bootstrap_script = "\'" + bootstrap_script+ "\'"
        return bootstrap_script
     
     
    def add_subjob(self, jd, job_url, job_id):
        logger.debug("Stage input files for sub-job")
        if jd.attribute_exists ("filetransfer"):
            self.__stage_files(jd.filetransfer, self.__get_subjob_working_dir(job_id))
        logger.debug("add subjob to queue of PJ: " + str(self.pilot_url))        
        for i in range(0,3):
            try:
                logger.debug("create dictionary for job description. Job-URL: " + job_url)
                # put job description attributes to Redis
                job_dict = {}
                #to accomendate current bug in bliss (Number of processes is not returned from list attributes)
                job_dict["NumberOfProcesses"] = "1" 
                attributes = jd.list_attributes()   
                logger.debug("SJ Attributes: " + str(attributes))             
                for i in attributes:          
                        if jd.attribute_is_vector(i):
                            #logger.debug("Add attribute: " + str(i) + " Value: " + str(jd.get_vector_attribute(i)))
                            vector_attr = []
                            for j in jd.get_vector_attribute(i):
                                vector_attr.append(j)
                            job_dict[i]=vector_attr
                        else:
                            #logger.debug("Add attribute: " + str(i) + " Value: " + jd.get_attribute(i))
                            job_dict[i] = jd.get_attribute(i)
                
                job_dict["state"] = str(Unknown)
                job_dict["job-id"] = str(job_id)
                
                #logger.debug("update job description at communication & coordination sub-system")
                self.coordination.set_job(job_url, job_dict)                                                
                self.coordination.queue_job(self.pilot_url, job_url)
                break
            except:
                traceback.print_exc(file=sys.stdout)
                time.sleep(2)
                #raise Exception("Unable to submit job")
                     
    def delete_subjob(self, job_url):
        self.coordination.delete_job(job_url) 
    
    def get_subjob_state(self, job_url):
        return self.coordination.get_job_state(job_url) 
    
    def get_subjob_details(self, job_url):
        return self.coordination.get_job(job_url) 
     
    def get_state(self):        
        """ duck typing for get_state of saga.job.job  
            state of saga job that is used to spawn the pilot agent
        """
        try:
            return self.job.get_state()
        except:
            return None
    
    def get_state_detail(self): 
        """ internal state of BigJob agent """ 
        try:
            return self.coordination.get_pilot_state(self.pilot_url)["state"]
        except:
            return None
    
    def get_free_nodes(self):
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

    
    def stop_pilot_job(self):
        """ mark in advert directory of pilot-job as stopped """
        try:
            logger.debug("stop pilot job: " + self.pilot_url)
            self.coordination.set_pilot_state(self.pilot_url, str(Done), True)            
            self.job=None
        except:
            pass
    
    def cancel(self):        
        """ duck typing for cancel of saga.cpr.job and saga.job.job  """
        logger.debug("Cancel Pilot Job")
        try:
            self.job.cancel()
        except:
            pass
            #traceback.print_stack()
        try:            
            self.stop_pilot_job()
            logger.debug("delete pilot job: " + str(self.pilot_url))                      
            if CLEANUP:
                self.coordination.delete_pilot(self.pilot_url)                
        except:
            pass
            #traceback.print_stack()


    ###########################################################################
    # internal methods
    
    def __parse_url(self, url):
        try:
            surl = saga.url(url)
            host = surl.host
            port = surl.port
            username = surl.username
            password = surl.password
            query = surl.query
            scheme = "%s://"%surl.scheme
        except:
            """ Fallback URL parser based on Python urlparse library """
            logger.error("URL %s could not be parsed")
            traceback.print_exc(file=sys.stderr)
            result = urlparse.urlparse(url)
            host = result.hostname
            port = result.port
            username = result.username
            password = result.password
            if url.find("?")>0:
                query = url[url.find("?")+1:]
            else:
                query = None
            scheme = "%s://"%result.scheme
            
        return scheme, username, password, host, port, query     
            
    
    def __get_bigjob_working_dir(self):
        return os.path.join(self.working_directory, self.uuid)
    
    
    def __get_subjob_working_dir(self, sj_id):
        return os.path.join(self.__get_bigjob_working_dir(), sj_id)
    

    def __stage_files(self, filetransfers, target_url):
        logger.debug("Stage: %s to %s"%(filetransfers, target_url))
        self.__create_remote_directory(target_url)
        if filetransfers==None:
            return
        for i in filetransfers:
            source_file=i
            if i.find(">")>0:
                source_file = i[:i.find(">")].strip()
            target_url_full = os.path.join(target_url, os.path.basename(source_file))
            logger.debug("Stage: %s to %s"%(source_file, target_url_full))
            self.__third_party_transfer(source_file, target_url_full)
           
        
    def __third_party_transfer(self, source_url, target_url):
        """
            Transfers from source URL to machine of PS (target path)
        """
        result = urlparse.urlparse(source_url)
        source_host = result.netloc
        source_path = result.path
        
        result = urlparse.urlparse(target_url)
        target_host = result.netloc
        target_path = result.path
          
        python_script= """import sys
import os
import urllib
import sys
import time
import paramiko

client = paramiko.SSHClient()
client.load_system_host_keys()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect("%s")
sftp = client.open_sftp()
sftp.put("%s", "%s")
"""%(target_host, source_path, target_path)

        logging.debug("Execute: \n%s"%python_script)
        source_client = paramiko.SSHClient()
        source_client.load_system_host_keys()
        source_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        source_client.connect(source_host)
        stdin, stdout, stderr = source_client.exec_command("python -c \'%s\'"%python_script)
        stdin.close()
        logging.debug("************************************************")
        logging.debug("Stdout: %s\nStderr:%s", stdout.read(), stderr.read())
        logging.debug("************************************************")
      
    
    def __create_remote_directory(self, target_url):
        result = urlparse.urlparse(target_url)
        target_host = result.netloc
        target_path = result.path
        try:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(target_host)
            sftp = client.open_sftp()            
            sftp.mkdir(target_path)
            sftp.close()
            client.close()
        except:
            logger.warn("Error creating directory: " + str(target_path) 
                         + " at: " + str(target_host) + " Already exists?" )
            #self.__print_traceback()  
    
    
    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)
        
    def __repr__(self):
        return self.pilot_url 

    def __del__(self):
        self.cancel()


                    
                    
class subjob(api.base.subjob):
    
    def __init__(self, coordination_url=None):
        """Constructor"""
        self.coordination_url = coordination_url
        self.job_url=None
        self.uuid = "sj-" + str(get_uuid())
        self.job_url = None
        self.pilot_url = None
        self.bj = None
        
    def get_job_url(self, pilot_url):
        self.job_url = pilot_url + ":jobs:" + str(self.uuid)
        return self.job_url
    

    def submit_job(self, pilot_url, jd):
        """ submit subjob to referenced bigjob """
        if self.job_url==None:
            self.job_url=self.get_job_url(pilot_url)            
        
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]    
        self.bj.add_subjob(jd, self.job_url, self.uuid)


    def get_state(self, pilot_url=None):        
        """ duck typing for saga.job  """
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]                
        return self.bj.get_subjob_state(self.job_url)
    
    
    def cancel(self, pilot_url=None):
        logger.debug("delete job: " + self.job_url)
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]  
        if str(self.bj.get_state())=="Running":
            self.bj.delete_subjob(self.job_url)        
        
    def get_exe(self, pilot_url=None):
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]  
        sj = self.bj.get_subjob_details(self.job_url)
        return sj["Executable"]
   
    def get_arguments(self, pilot_url=None):
        if self.pilot_url==None:
            self.pilot_url = pilot_url
            self.bj=pilot_url_dict[pilot_url]  
        sj = self.bj.get_subjob_details(self.job_url)  
        #logger.debug("Subjob details: " + str(sj))              
        arguments=""
        for  i in  sj["Arguments"]:
            arguments = arguments + " " + i
        return arguments

    def __del__(self):
        self.cancel()
    
    def __repr__(self):        
        if(self.job_url==None):
            return "None"
        else:
            return self.job_url
        
        
class description(saga.job.description):
    pass