#!/usr/bin/env python

import textwrap
import re
import pdb

from bigjob import logger
import bigjob

try:
    import saga
except:
    logger.warn("pbs-ssh://<hostname> plugin not compatible with SAGA Bliss. Use pbs+ssh://<hostname>")

import os

class pbsssh:
    """Constructor"""
    def __init__(self, bootstrap_script, launch_method, queue, project, lrms_saga_url, walltime, number_nodes, processes_per_node, userproxy, working_directory=None, bj_working_directory=None):
        self.job_id = ""
        self.lrms_saga_url = lrms_saga_url
        if launch_method == "ssh":
            self.lrms_saga_url.scheme="ssh"
        else:
            logger.debug("Use GSISSH")
            self.lrms_saga_url.scheme="gsissh"
        self.userproxy = userproxy
        self.working_directory = ""
        if working_directory == None:
            self.working_directory = ""
        else:
            self.working_directory = working_directory
        if bj_working_directory==None:
            bj_working_directory=self.working_directory
        ### convert walltime in minutes to PBS representation of time ###
        walltime_pbs="1:00:00"
        if walltime!=None and walltime!="":    
            hrs=int(walltime)/60 
            minu=int(walltime)%60 
            walltime_pbs=""+str(hrs)+":"+str(minu)+":00"

        if int(number_nodes)%int(processes_per_node) == 0:
            nodes = int(number_nodes)/int(processes_per_node)
        else:
            nodes = (int(number_nodes)/int(processes_per_node)) + 1    
        
        ppn = processes_per_node
        self.bootstrap_script = textwrap.dedent("""import sys
import os
import urllib
import sys
import time
import textwrap

qsub_file_name="bigjob_pbs_ssh"

qsub_file = open(qsub_file_name, "w")
if ( %s == 1 ):
    qsub_file.write("#PBS -l size=%s")
else:
    qsub_file.write("#PBS -l nodes=%s:ppn=%s")
qsub_file.write("\\n")
if ( "%s" != "None" ):
    qsub_file.write("#PBS -q %s")
    qsub_file.write("\\n")
if (  "%s" != "None" ):
    qsub_file.write("#PBS -A %s")
    qsub_file.write("\\n")    
qsub_file.write("#PBS -l walltime=%s")
qsub_file.write("\\n")
qsub_file.write("#PBS -o %s/stdout-bigjob_agent.txt")
qsub_file.write("\\n")
qsub_file.write("#PBS -e %s/stderr-bigjob_agent.txt")
qsub_file.write("\\n")
qsub_file.write("cd %s")
qsub_file.write("\\n")
qsub_file.write("python -c XX" + textwrap.dedent(\"\"%s\"\") + "XX")
qsub_file.close()

cmd = "qsub  " + qsub_file_name
if os.path.exists(os.path.expanduser("~/.bashrc")):
    cmd = ". ~/.bashrc; " + cmd 
os.system(cmd)
""") % (str(ppn),str(nodes),str(nodes),str(ppn),str(queue),str(queue),str(project),str(project),str(walltime_pbs), bj_working_directory, bj_working_directory, str(self.working_directory), bootstrap_script)
        ### escaping characters
        self.bootstrap_script = self.bootstrap_script.replace("\"","\\\"")
        self.bootstrap_script = self.bootstrap_script.replace("\\\\","\\\\\\\\\\")
        self.bootstrap_script = self.bootstrap_script.replace("XX","\\\\\\\"")
        self.bootstrap_script = "\"" + self.bootstrap_script+ "\""
        logger.debug(self.bootstrap_script)


    def run(self):
        jd = saga.job.description()
        jd.arguments = ["-c", self.bootstrap_script]
        jd.executable = "python"
        jd.working_directory =  self.working_directory
        jd.set_attribute("Interactive", "True")
        # Submit job
        js = None
        if self.userproxy != None and self.userproxy != '':
            s = saga.session()
            os.environ["X509_USER_PROXY"]=self.userproxy
            ctx = saga.context("x509")
            ctx.set_attribute ("UserProxy", self.userproxy)
            s.add_context(ctx)
            print "use proxy: " + self.userproxy
            js = saga.job.service(s, self.lrms_saga_url)
        else:
            print "use standard proxy"
            js = saga.job.service(self.lrms_saga_url)
        pbssshjob = js.create_job(jd)
        print "Submit pilot job to: " + str(self.lrms_saga_url)
        pbssshjob.run()
        pbssshjob.wait()
        outstr = pbssshjob.get_stdout().read()
        errstr = pbssshjob.get_stderr().read()
        self.job_id=(outstr).split(".")[0]
        logger.debug("PBS JobID: " + str(self.job_id))
        if self.job_id==None or self.job_id=="":
            logger.error("Unable to submit to: " + str(self.lrms_saga_url))
            raise Exception("BigJob submission via pbs-ssh:// failed: %s %s" % (outstr,errstr))


    def get_state(self):
        jd = saga.job.description()
        jd.set_attribute("Executable", "qstat")
        jd.set_attribute("Interactive", "True")
        jd.set_vector_attribute("Arguments", [self.job_id])
        # connect to the local job service
        js = saga.job.service(self.lrms_saga_url);
        # submit the job
        job = js.create_job(jd)
        job.run()
        # wait for the job to complete
        job.wait(-1)
        # print the job's output
        output = job.get_stdout()
        output.readline()
        output.readline()
        state=output.readline()
        state=(re.sub(r'\s+',' ',state)).split(' ')[4]
        if state == "R":
            state = "Running"
        elif state == "C" or state == "E":
            state = "Done"
        else:
            state = "Unknown"
        return state 

    def cancel(self):
        jd = saga.job.description()
        jd.set_attribute("Executable", "qdel")
        jd.set_attribute("Interactive", "True")
        jd.set_vector_attribute("Arguments", [self.job_id])
        # connect to the local job service
        js = saga.job.service(self.lrms_saga_url);
        # submit the job
        job = js.create_job(jd)
        job.run()
        # wait for the job to complete
        job.wait(-1)
