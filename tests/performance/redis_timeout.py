import os
import sys
import saga
import datetime
import time
import pilot
import optparse
import calendar

#XSEDE_URL       = "pbs+ssh://sierra.futuregrid.org"
XSEDE_URL       = "fork://localhost"
XSEDE_REDIS_PWD = os.environ.get('REDIS_PWD')
XSEDE_COORD     = "redis://localhost"
count = 0

#-------------------------------------------------------------------------------
#
def main():
        global count
        count = 0;
        pilot_service = pilot.PilotComputeService(XSEDE_COORD)
        num_tasks = [256]
        job_length = [5, 10, 30, 60, 720, 1440]
        counter=0
        while(1):
            for j in job_length:
                for i in range(128, 641, 128):
                    run_on_xsede_with_bigjob(xsede_url=XSEDE_URL, number_of_cores=int(i), task_length=int(j),pilot_service=pilot_service)
                    print "Started/stopped %d pilots"%counter
                    counter = counter + 1

def run_on_xsede_with_bigjob(xsede_url, number_of_cores, task_length, pilot_service):
    global count

    try:

        pilot_description = pilot.PilotComputeDescription()
        pilot_description.service_url = XSEDE_URL
        pilot_description.number_of_processes = number_of_cores
        pilot_description.working_directory = "/tmp"
        #pilot_description.walltime = task_length
        #pilot_description.queue = "batch"

        curr_time = calendar.timegm(time.gmtime())
        t_1 = time.time()
        pilotjob = pilot_service.create_pilot(pilot_compute_description=pilot_description)
        pilotjob.wait()
        queue_time = time.time() - t_1

        pilotjob.cancel()
        #f = open("/home/vshah505/Research/hpc_vs_htc/infrastructure/results_sierra", "a");
        #f.write(str(curr_time) + "\t" + str(number_of_cores) + "\t" + str(task_length) + "\t" + str(queue_time) + "\n")
        #f.close()
        count = count + 1
    except Exception, ex:
        print "An exception occured during XSEDE submission: %s " % ((str(ex)))
        print "%i\n" % (count)
        sys.exit()
        
#-------------------------------------------------------------------------------
#
if __name__ == "__main__":
    main()

