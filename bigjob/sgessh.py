#!/usr/bin/env python

import textwrap
import re

from bigjob import logger
import bigjob

try:
    import saga
except:
    logger.warn("sge-ssh://<hostname> plugin not compatible with SAGA Bliss.")

import os

class sgessh:
    """Constructor"""
    def __init__(self, bootstrap_script, lrms_saga_url, walltime, number_nodes, processes_per_node, userproxy, project, queue, working_directory=None, bj_working_directory=None):
        self.job_id = ""
        self.lrms_saga_url = lrms_saga_url
        self.lrms_saga_url.scheme="ssh"
        self.userproxy = userproxy
        self.working_directory = ""
        if working_directory == None:
            self.working_directory = ""
        else:
            self.working_directory = working_directory
        if bj_working_directory==None:
            bj_working_directory=self.working_directory
        ### convert walltime in minutes to PBS representation of time ###
        walltime_sge="1:00:00"
        if walltime!=None and walltime!="":    
            hrs=int(walltime)/60 
            minu=int(walltime)%60 
            walltime_sge=""+str(hrs)+":"+str(minu)+":00"

        self.bootstrap_script = textwrap.dedent("""import sys
import os
import urllib
import sys
import time
import textwrap

qsub_file_name="bigjob_sge_ssh"

qsub_file = open(qsub_file_name, "w")
qsub_file.write("#$ -pe %sway %s")
qsub_file.write("\\n")
qsub_file.write("#$ -V")
qsub_file.write("\\n")
qsub_file.write("#$ -l h_rt=%s")
qsub_file.write("\\n")
qsub_file.write("#$ -q %s")
qsub_file.write("\\n")
qsub_file.write("#$ -A %s")
qsub_file.write("\\n")
qsub_file.write("#$ -o %s/stdout-bigjob_agent.txt")
qsub_file.write("\\n")
qsub_file.write("#$ -e %s/stderr-bigjob_agent.txt")
qsub_file.write("\\n")
qsub_file.write("cd %s")
qsub_file.write("\\n")
qsub_file.write("python -c XX" + textwrap.dedent(\"\"%s\"\") + "XX")
qsub_file.close()
os.system( "qsub  -terse " + qsub_file_name)
""") % (str(processes_per_node),str(number_nodes),str(walltime_sge), str(queue), str(project), bj_working_directory,bj_working_directory, 
str(self.working_directory), bootstrap_script)
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
        sgesshjob = js.create_job(jd)
        print "Submit pilot job to: " + str(self.lrms_saga_url)
        sgesshjob.run()
        sgesshjob.wait()
        outstr = (sgesshjob.get_stdout().read()).rstrip()
        errstr = sgesshjob.get_stderr().read()
        print "Output - \n" + str(outstr)
        self.job_id=(outstr).split("\n")[-1]
        print "SGE JobID: " + str(self.job_id)
        if self.job_id==None or self.job_id=="":
            raise Exception("BigJob submission via sge-ssh:// failed: %s %s" % (outstr,errstr))


    def get_state(self):
        jd = saga.job.description()
        jd.set_attribute("Executable", "qstat")
        jd.set_attribute("Interactive", "True")
        # connect to the local job service
        js = saga.job.service(self.lrms_saga_url);
        # submit the job
        job = js.create_job(jd)
        job.run()
        # wait for the job to complete
        job.wait(-1)
        # print the job's output
        output = ((job.get_stdout()).read()).rstrip()
        jobs=output.split("\n")
        for k in jobs:
            jinfo = k.split(" ")            
            jinfo = filter(lambda x: x!='',jinfo)
            if jinfo[0] == self.job_id:
                state=jinfo[4]
                break
        if state.upper() == "R":
            state = "Running"
        elif state.upper() == "C" or state.upper() == "E":
            state = "Done"
        elif state.upper() == "QW":
            state = "Queue"
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
