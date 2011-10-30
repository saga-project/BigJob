#!/usr/bin/env python

import textwrap
import re
import saga
import os

class pbsssh:
    """Constructor"""
    def __init__(self,bootstrap_script,lrms_saga_url,walltime,nodes,ppn,userproxy,working_directory=None):
        self.job_id = ""
        self.lrms_saga_url = lrms_saga_url
        self.lrms_saga_url.scheme="ssh"
        self.userproxy = userproxy
        self.working_directory = ""
        if working_directory == None:
            self.working_directory = ""
        else:
            self.working_directory = working_directory

        self.bootstrap_script = textwrap.dedent("""import sys
import os
import urllib
import sys
import time
import textwrap

qsub_file_name="bigjob_pbs_ssh"

qsub_file = open(qsub_file_name, "w")
qsub_file.write("#PBS -l nodes=%s:ppn=%s")
qsub_file.write("\\n")
qsub_file.write("#PBS -l walltime=%s")
qsub_file.write("\\n")
qsub_file.write("cd %s")
qsub_file.write("\\n")
qsub_file.write("python -c XX" + textwrap.dedent(\"\"%s\"\") + "XX")
qsub_file.close()
os.system( "qsub  " + qsub_file_name)
""") % (str(nodes),str(ppn),str(walltime),str(self.working_directory),bootstrap_script)
        ### escaping characters
        self.bootstrap_script = self.bootstrap_script.replace("\"","\\\"")
        self.bootstrap_script = self.bootstrap_script.replace("\\\\","\\\\\\\\\\")
        self.bootstrap_script = self.bootstrap_script.replace("XX","\\\\\\\"")
        self.bootstrap_script = "\"" + self.bootstrap_script+ "\""


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
        joboutput= pbssshjob.get_stdout()
        self.job_id=(joboutput.read()).split(".")[0]

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
