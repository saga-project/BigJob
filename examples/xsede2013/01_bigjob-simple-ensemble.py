import os
import sys
import pilot
import traceback

""" DESCRIPTION: This example does this...
"""

# Redis password is read from the environment. The example can be run like this:
# REDIS_PASSWORD=ILikeBigJob_wITH-REdIS python examples/example_styleguide.py
# Alternatively, for tutorials, etc. REDIS_PASSWORD can be defined in /etc/profile

#------------------------------------------------------------------------------
#
REDIS_PWD   = os.environ.get('REDIS_PASSWORD')
# The coordination server
COORD       = "redis://%s@gw68.quarry.iu.teragrid.org:6379" % REDIS_PWD
# The host to run BigJob on
HOSTNAME    = "localhost"
# The working directory on the remote cluster / machine 
WORKDIR     = os.getenv("HOME")+"/XSEDETutorial"
# The number of jobs you want to run
NUMBER_JOBS = 4


#------------------------------------------------------------------------------
#
def main():
    try:
        # this describes the parameters and requirements for our pilot job
        pilot_description = pilot.PilotComputeDescription()
        pilot_description.service_url = "ssh://%s" % HOSTNAME
        pilot_description.number_of_processes = 1
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
            task_desc.arguments = ['Hello', '$ENV1', "I am CU number %s" % str(i)]
            task_desc.environment = {'ENV1': 'World'}
            task_desc.number_of_processes = 1
            task_desc.output = 'simple-ensemble-stdout.txt'
            task_desc.error = 'simple-ensemble-stderr.txt'

            task = pilotjob.submit_compute_unit(task_desc)
            print "* Submitted task '%s' with id '%s' to %s" % (i, task.get_id(), HOSTNAME)
            tasks.append(task)

        print "Waiting for tasks to finish..."
        pilotjob.wait()

        # get the output directories
        for task in tasks:
            print "* Output for '%s' can be found remotely in '%s'" % (task.get_id(), task.get_local_working_directory())

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
        print ("Terminating BigJob")
        pilotjob.cancel()
        pilot_compute_service.cancel()


if __name__ == "__main__":
    sys.exit(main())
