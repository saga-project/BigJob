#!/usr/bin/env python

from bigjob import logger
import os
import uuid
import time
import traceback
import sys
import subprocess
import saga


class State:
    UNKNOWN="unknown"
    PENDING="pending"
    RUNNING="running"


class Service(object):
    """ Plugin for Amazon EC2 and EUCA

        Manages endpoint in the form of:

            ec2+ssh://<EC2 Endpoint>
            euca+ssh://<EUCA Endpoint>
    """
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
    """ Plugin for Amazon EC2

        Starts VM and executes BJ agent on this VM


        Eucalyptus on FutureGrid uses a self-signed certificate, which 1) needs to be added to boto configuration
        or 2) certificate validation needs to be disabled.
    """

    def __init__(self, job_description, resource_url, pilot_compute_description):

        self.job_description = job_description
        logger.debug("URL: " + str(resource_url) + " Type: " + str(type(resource_url)))
        self.resource_url = saga.Url(str(resource_url))
        self.pilot_compute_description = pilot_compute_description

        self.id="bigjob-" + str(uuid.uuid1())
        self.subprocess_handle=None






    def run(self):
        """ Start VM and start BJ agent via SSH on VM """

        # Submit job
        working_directory = os.getcwd("working_directory")
        if self.pilot_compute_description.has_key():
            working_directory=self.pilot_compute_description["working_directory"]


        args =
        subprocess.Popen

        session = saga.Session()
        session.add_context(ctx)

        TRIAL_MAX=30
        trials=0
        while trials < TRIAL_MAX:
            try:
                js = saga.job.Service(url, session=session)
                logger.debug("Job Description Type: " + str(type(self.job_description)))
                job = js.create_job(self.job_description)
                logger.debug("Attempt: %d, submit pilot job to: %s "%(trials,str(url)))
                job.run()
                if job.get_state()==saga.job.FAILED:
                    logger.warning("Submission failed.")
                    trials = trials + 1
                    time.sleep(30)
                    continue
                else:
                    break
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                logger.warning("Submission failed: " + str(exc_value))
                #self.__print_traceback()
                trials = trials + 1
                time.sleep(30)
                if trials == TRIAL_MAX:
                    raise Exception("Submission of agent failed.")

        logger.debug("Job State : %s" % (job.get_state()))



    def wait_for_running(self):
        while self.get_state()!=State.RUNNING:
            time.sleep(5)


    def get_state(self):
        result = State.UNKNOWN
        try:
            self.instance.update()
            result=self.instance.state
        except:
            logger.warning("Instance not reachable/active yet...")
        return result


    def cancel(self):
        self.instance.terminate()


    ###########################################################################
    # private methods
    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "*** print_tb:"
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print "*** print_exception:"
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=2, file=sys.stdout)



if __name__ == "__main__":
    local_service = Service("subprocess://localhost")
    j = local_service.create_job("test")
    j.run()
    print j.get_state()
