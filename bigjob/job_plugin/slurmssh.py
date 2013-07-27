#!/usr/bin/env python

import textwrap
import re
import os
import pdb

from bigjob import logger
import bigjob

import saga
    
class Service(object):
    """ Plugin for SlURM    """ 

    def __init__(self, resource_url, pilot_compute_description=None):
        """Constructor"""
        self.resource_url = resource_url
        self.pilot_compute_description = pilot_compute_description
            
    
    def create_job(self, job_description):
        j = Job(job_description, self.resource_url, self.pilot_compute_description)
        return j
            
    
    def __del__(self):
        pass

class Job(object):
    """Constructor"""
    def __init__(self, job_description, resource_url, pilot_compute_description=None):
        self.job_description=job_description
        self.bootstrap_script = self.job_description.arguments[2]
        self.job_id = ""
        self.resource_url = resource_url
        self.resource_url.scheme="ssh"
        logger.debug("BigJob/SLURM: Parsing job description")        
        if pilot_compute_description == None:
            pilot_compute_description={}
        pilot_compute_description['queue'] = job_description.queue
        pilot_compute_description['project'] = job_description.project
        pilot_compute_description['working_directory'] = job_description.working_directory
        pilot_compute_description['walltime'] = job_description.wall_time_limit
        pilot_compute_description['number_of_processes'] =  job_description.total_cpu_count

        self.working_directory = pilot_compute_description["working_directory"]
        ### convert walltime in minutes to SLURM representation of time ###
        walltime_slurm="1:00:00"
        if pilot_compute_description.has_key("walltime"):    
            hrs=int(pilot_compute_description["walltime"])/60 
            minu=int(pilot_compute_description["walltime"])%60 
            walltime_slurm=""+str(hrs)+":"+str(minu)+":00"

        logger.debug("BigJob/SLURM: generate bootstrap script")
        self.bootstrap_script = textwrap.dedent("""import sys
import os
import urllib
import sys
import time
import textwrap

sbatch_file_name="bigjob_slurm_ssh"

sbatch_file = open(sbatch_file_name, "w")
sbatch_file.write("#!/bin/bash")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -n %s")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -J bigjob_slurm")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -t %s")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -A %s")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -o %s/stdout-bigjob_agent.txt")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -e %s/stderr-bigjob_agent.txt")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -p %s")
sbatch_file.write("\\n")

sbatch_file.write("cd %s")
sbatch_file.write("\\n")
sbatch_file.write("python -c XX" + textwrap.dedent(\"\"%s\"\") + "XX")
sbatch_file.close()
os.system( "sbatch  " + sbatch_file_name)
""") % (str(pilot_compute_description["number_of_processes"]),str(walltime_slurm), str(pilot_compute_description["project"]), pilot_compute_description["working_directory"], pilot_compute_description["working_directory"], pilot_compute_description["queue"], pilot_compute_description["working_directory"], self.bootstrap_script)
        ### escaping characters
        self.bootstrap_script = self.bootstrap_script.replace("\"","\\\"")
        self.bootstrap_script = self.bootstrap_script.replace("\\\\","\\\\\\\\\\")
        self.bootstrap_script = self.bootstrap_script.replace("XX","\\\\\\\"")
        self.bootstrap_script = "\"" + self.bootstrap_script+ "\""
        logger.debug(self.bootstrap_script)


    def run(self):
        jd = saga.job.Description()
        jd.arguments = ["-c", self.bootstrap_script]
        jd.executable = "python"
        jd.working_directory =  self.working_directory
        jd.output = "saga_job_submission.out"
        jd.error = "saga_job_submission.err"
        # Submit job
        js = None
        js = saga.job.Service(self.resource_url)
        slurmsshjob = js.create_job(jd)
        logger.debug("Submit pilot job to: " + str(self.resource_url))
        slurmsshjob.run()
        slurmsshjob.wait()
        logger.debug("BigJob/SLURM: SSH run job finished")
        
        saga_surl = saga.Url(self.resource_url)
        sftp_url = "sftp://"
        if saga_surl.username!=None and saga_surl.username!="":
            sftp_url = sftp_url + str(saga_surl.username) + "@"
        sftp_url = sftp_url + saga_surl.host + "/"
        outfile = sftp_url + self.working_directory+'/saga_job_submission.out'        
        logger.debug("BigJob/SLURM: get outfile: " + outfile)
        out = saga.filesystem.File(outfile)
        out.copy("sftp://localhost/"+os.getcwd() + "/tmpout")
        errfile = sftp_url + self.working_directory+'/saga_job_submission.err'        
        err = saga.filesystem.File(errfile)
        err.copy("sftp://localhost/"+os.getcwd() + "/tmperr")
        
        tempfile=open(os.getcwd() + "/tmpout")
        outstr = tempfile.read().rstrip()
        tempfile.close()
        os.remove(os.getcwd() + "/tmpout")
        
        tempfile=open(os.getcwd() + "/tmperr")
        errstr = tempfile.read().rstrip()
        tempfile.close()
        os.remove(os.getcwd() + "/tmperr")

        logger.debug("Output - \n" + str(outstr))
        if ((outstr).split("\n")[-1]).split()[0] == "Submitted": 
            self.job_id=((outstr).split("\n")[-1]).split()[3]
            logger.debug("SLURM JobID: " + str(self.job_id))
        if self.job_id==None or self.job_id=="":
            raise Exception("BigJob submission via slurm+ssh:// failed: %s %s" % (outstr,errstr))


    def get_state(self):
        jd = saga.job.Description()
        jd.executable = "squeue"
        jd.arguments=["-j",self.job_id]
        jd.output="jobstate.out"   
        jd.working_directory =  self.working_directory     
        # connect to the local job service
        js = saga.job.service(self.resource_url);
        # submit the job
        job = js.create_job(jd)
        job.run()
        job.wait()
        # print the job's output
        
        outfile = 'sftp://'+saga.Url(self.resource_url).host+self.working_directory+'/jobstate.out'        
        out = saga.filesystem.File(outfile)
        out.move("sftp://localhost/"+os.getcwd() + "/tmpstate")
        
        tempfile=open(os.getcwd() + "/tmpstate")
        output = tempfile.read().rstrip()
        tempfile.close()
        os.remove(os.getcwd() + "/tmpstate")
        
        state=output.split("\n")[-1].split()[4]

        if state.upper() == "R":
            state = "Running"
        elif state.upper() == "CD" or state.upper() == "CF" or state.upper() == "CG":
            state = "Done"
        elif state.upper() == "PD":
            state = "Queue"
        else:
            state = "Unknown"
        return state 


    def cancel(self):
        logger.debug("Cancel SLURM job")
        jd = saga.job.Description()
        jd.executable = "scancel"
        jd.arguments = [self.job_id]        
        # connect to the local job service
        js = saga.job.Service(self.resource_url);
        # submit the job
        job = js.create_job(jd)
        job.run()
        # wait for the job to complete
        job.wait()




if __name__ == "__main__":
    slurm_service = Service("slurm+ssh://stampede.tacc.utexas.edu")
    j = slurm_service.create_job("")
    j.run()
    print j.get_state()
