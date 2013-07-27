import os
import sys
import saga    # <=== !!
import pilot
import traceback

""" This tutorial example extends and improves the first example
    (01_bigjob-simple-ensemble.py) by adding file transfer: once
    the 32 tasks have finished executing, we use SAGA-Python to
    transfer the individual output files back to the local machine.
"""


#------------------------------------------------------------------------------
# Redis password and 'user' name a aquired from the environment
REDIS_PWD   = os.environ.get('XSEDE_TUTORIAL_REDIS_PASSWORD')
USER_NAME   = os.environ.get('XSEDE_TUTORIAL_USER_NAME')

# The coordination server
COORD       = "redis://%s@gw68.quarry.iu.teragrid.org:6379" % REDIS_PWD
# The host to run BigJob on
HOSTNAME    = "sagatut@stampede.tacc.utexas.edu"
# The queue on the remote system
QUEUE       = "normal"
# The working directory on the remote cluster / machine
WORKDIR     = "/home1/02554/sagatut/XSEDETutorial/%s/example2" % USER_NAME
# The number of jobs you want to run
NUMBER_JOBS = 32


#------------------------------------------------------------------------------
#
def main():
    try:
        # this describes the parameters and requirements for our pilot job
        pilot_description = pilot.PilotComputeDescription()
        pilot_description.service_url = "slurm+ssh://%s" % HOSTNAME
        pilot_description.queue = QUEUE
        pilot_description.number_of_processes = 32
        pilot_description.working_directory = WORKDIR
        pilot_description.walltime = 10

        # create a new pilot job
        pilot_compute_service = pilot.PilotComputeService(COORD)
        pilotjob = pilot_compute_service.create_pilot(pilot_description)

        # submit tasks to pilot job
        tasks = list()
        for i in range(NUMBER_JOBS):
            task_desc = pilot.ComputeUnitDescription()
            task_desc.executable = '/bin/echo'
            task_desc.arguments = ['I am task number $TASK_NO', ]
            task_desc.environment = {'TASK_NO': i}
            task_desc.number_of_processes = 1
            task_desc.output = 'stdout.txt'
            task_desc.error =  'stderr.txt'

            task = pilotjob.submit_compute_unit(task_desc)
            print "* Submitted task '%s' with id '%s' to %s" % (i, task.get_id(), HOSTNAME)
            tasks.append(task)

        print "Waiting for tasks to finish..."
        pilotjob.wait()

        # all compute units have finished. now we can use saga-python
        # to transfer back the output files...
        d = saga.filesystem.Directory("sftp://%s/" % (HOSTNAME))
        for task in tasks:
            local_filename = "ex-2-stdout-%s.txt" % (task.get_id())
            d.copy("%s/stdout.txt" % (task.get_local_working_directory()), "file://localhost/%s/%s" % (os.getcwd(), local_filename))
            print "* Output for '%s' copied to: './%s'" % (task.get_id(), local_filename)

        return(0)

    except Exception, ex:
            print "AN ERROR OCCURED: %s" % ((str(ex)))
            # print a stack trace in case of an exception -
            # this can be helpful for debugging the problem
            traceback.print_exc()
            return(-1)

    finally:
        # alway try to shut down pilots, otherwise jobs might end up
        # lingering in the queue
        print ("Terminating BigJob...")
        pilotjob.cancel()
        pilot_compute_service.cancel()


if __name__ == "__main__":
    sys.exit(main())
