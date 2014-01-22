import os
import time
import sys
import pilot
import traceback

""" DESCRIPTION: This example shows how to run BigJob locally to execute chained tasks.
"""

#------------------------------------------------------------------------------
# Redis password and 'user' name
REDIS_PWD   = ""# Fill in the password to your server
USER_NAME   = ""# Fill in your username on the resource you're running on

# The coordination server
COORD       = "redis://%s@localhost:6379" % REDIS_PWD
# The host to run BigJob on
HOSTNAME    = "localhost"
# The working directory on your machine
WORKDIR     = "/home/%s/example1" % USER_NAME
# The number of jobs you want to run
NUMBER_JOBS = 4


#------------------------------------------------------------------------------
#

def main():
    try:
        # this describes the parameters and requirements for our pilot job
        pilot_description = pilot.PilotComputeDescription()
        pilot_description.service_url = "fork://%s" % HOSTNAME
        pilot_description.number_of_processes = 4 
        pilot_description.working_directory = WORKDIR
        pilot_description.walltime = 10

        # create a new pilot job
        pilot_compute_service = pilot.PilotComputeService(COORD)
        pilotjob = pilot_compute_service.create_pilot(pilot_description)

         # submit 'A' tasks to pilot job
        task_set_A = list()
        for i in range(NUMBER_JOBS):
            task_desc = pilot.ComputeUnitDescription()
            task_desc.executable = '/bin/echo'
            task_desc.arguments = ['I am an $TASK_SET task with id $TASK_NO', ]
            task_desc.environment = {'TASK_SET': 'A', 'TASK_NO': i}
            task_desc.number_of_processes = 1
            task_desc.output = 'A-stdout.txt'
            task_desc.error  = 'A-stderr.txt'

	    # Submit task to PilotJob
            task = pilotjob.submit_compute_unit(task_desc)
            print "* Submitted 'A' task '%s' with id '%s'" % (i, task.get_id())
            task_set_A.append(task)

        # Chaining tasks i.e submit a compute unit, when compute unit from A is successfully executed.
        # A 'B' task reads the content of the output file of an 'A' task and writes it into its own
        # output file.
        task_set_B = list()
        while len(task_set_A) > 0:
            for a_task in task_set_A:
                if a_task.get_state() == "Done":
                    print "One 'A' task %s finished. Launching a 'B' task." % (a_task.get_id())
                    task_desc = pilot.ComputeUnitDescription()
                    task_desc.executable = '/bin/echo'
                    task_desc.arguments = ['I am an $TASK_SET task with id $TASK_NO', ]
                    task_desc.environment = {'TASK_SET': 'B', 'TASK_NO': a_task}
                    task_desc.number_of_processes = 1
                    task_desc.output = 'B-stdout.txt'
                    task_desc.error  = 'B-stderr.txt'

		    # Submit task to Pilot Job
                    task = pilotjob.submit_compute_unit(task_desc)
                    print "* Submitted 'B' task '%s' with id '%s'" % (i, task.get_id())
                    task_set_B.append(task)
                    task_set_A.remove(a_task)

    except Exception, ex:
        print "AN ERROR OCCURRED: %s" % ((str(ex)))
        # print a stack trace in case of an exception -
        # this can be helpful for debugging the problem
        traceback.print_exc()
        sys.exit(-1)

    finally:
         # alway try to shut down pilots, otherwise jobs might end up
         # lingering in the queue
         print ("Terminating BigJob...")
         pilotjob.cancel()
         pilot_compute_service.cancel()


