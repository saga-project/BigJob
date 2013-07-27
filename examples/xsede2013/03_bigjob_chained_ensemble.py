import os
import sys
import saga
import pilot
import traceback

""" This tutorial example introduces task synchronization. It submits a 
    set of 32 '/bin/echo' tasks (task set A). For every successfully completed 
    task, we submits another '/bin/cat' task from task set B to the same Pilot-Job.
    Task from set A can be seen as producers and tasks from task set B as
    consumers, since B-tasks read 'consume' the output file an A-tasks.
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
WORKDIR     = "/home1/02554/sagatut/XSEDETutorial/%s/example3" % USER_NAME
# The number of jobs you want to run
NUMBER_JOBS = 32


#------------------------------------------------------------------------------
#
if __name__ == "__main__":
    try:
        # this describes the parameters and requirements for our pilot job
        pilot_description = pilot.PilotComputeDescription()
        pilot_description.service_url = "slurm+ssh://%s" % HOSTNAME
        pilot_description.queue = QUEUE
        pilot_description.number_of_processes = NUMBER_JOBS
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
                    task_desc.executable = '/bin/cat'
                    task_desc.arguments = ["%s/A-stdout.txt" % a_task.get_local_working_directory()]
                    task_desc.number_of_processes = 1
                    task_desc.output = 'B-stdout.txt'
                    task_desc.error  = 'B-stderr.txt'
                    task = pilotjob.submit_compute_unit(task_desc)
                    print "* Submitted 'B' task '%s' with id '%s'" % (i, task.get_id())
                    task_set_B.append(task)
                    task_set_A.remove(a_task)

        # ---------------------------------------------------------------------
        print "Waiting for 'B' tasks to finish..."
        pilotjob.wait()
        # ---------------------------------------------------------------------

        # all 'B' tasks have finished. now we can use saga-python
        # to transfer back the output files...
        d = saga.filesystem.Directory("sftp://%s/" % (HOSTNAME))
        for task in task_set_B:
            local_filename = "ex3-stdout-%s.txt" % (task.get_id())
            d.copy("%s/B-stdout.txt" % (task.get_local_working_directory()), "file://localhost/%s/%s" % (os.getcwd(), local_filename))
            print "* Output for '%s' copied to: './%s'" % (task.get_id(), local_filename)

        sys.exit(0)

    except Exception, ex:
            print "AN ERROR OCCURED: %s" % ((str(ex)))
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
