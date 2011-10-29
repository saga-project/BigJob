#!/usr/bin/env python

import textwrap
import re
import saga


class pbsssh:
    """Constructor"""
    def __init__(self,bootstrap_script,walltime,nodes,ppn,working_directory=None):
        self.job_id = ""
        self.lrms_saga_url = ""
        if working_directory == None:
            working_directory = ""
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
""") % (str(nodes),str(ppn),str(walltime),str(working_directory),bootstrap_script)
        ### escaping characters
        self.bootstrap_script = self.bootstrap_script.replace("\"","\\\"")
        self.bootstrap_script = self.bootstrap_script.replace("\\\\","\\\\\\\\\\")
        self.bootstrap_script = self.bootstrap_script.replace("XX","\\\\\\\"")
        self.bootstrap_script = "\"" + self.bootstrap_script+ "\""

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
        # print the job's output
        output = job.get_stdout()
