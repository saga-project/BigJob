import os
import sys
import saga
import pilot
import traceback

from PIL import Image

""" 
This tutorial example splits up the calculation of a traditional Mandelbrot set using tiles.
This code aims to show the concurrent execution of tasks 
(as opposed to the analagous saga-python example, which submits tasks serially).
It also demonstrates file-movement (calculations are done in a temporary directory, but
the final image is concatenated back in the home directory), as well as working
with external python libraries and alternate executables.
"""

#------------------------------------------------------------------------------
# Redis password and 'user' name a aquired from the environment
REDIS_PWD   = os.environ.get('XSEDE_TUTORIAL_REDIS_PASSWORD')
USER_NAME   = os.environ.get('XSEDE_TUTORIAL_USER_NAME')

# The coordination server
COORD       = "redis://%s@gw68.quarry.iu.teragrid.org:6379" % REDIS_PWD
# The host (+username) to run BigJob on
#HOSTNAME    = "sagatut@stampede.tacc.utexas.edu"
HOSTNAME    = "sagatut@stampede.tacc.utexas.edu"
# The queue on the remote system
QUEUE       = "normal"
# The working directory on the remote cluster / machine
WORKDIR     = "/home1/02554/sagatut/XSEDETutorial/%s/example5" % USER_NAME

# The number of jobs you want to run
NUMBER_JOBS = 16

# the dimension of the whole fractal (in pixel)
IMGX = 8192
IMGY = 8192

# the number of tiles in X and Y direction
TILESX = 4
TILESY = 4


#------------------------------------------------------------------------------
#
def main():
    try:
        # copy the executable and warpper script to the remote host
        workdir = saga.filesystem.Directory("sftp://%s/%s" % (HOSTNAME, WORKDIR),
                                            saga.filesystem.CREATE_PARENTS)
        mbwrapper = saga.filesystem.File("file://localhost/%s/mandelbrot.sh" % os.getcwd())
        mbwrapper.copy(workdir.get_url())
        mbexe = saga.filesystem.File("file://localhost/%s/mandelbrot.py" % os.getcwd())
        mbexe.copy(workdir.get_url())

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

        for x in range(0, TILESX):
            for y in range(0, TILESY):
                # describe a single Mandelbrot job. we're using the
                # directory created above as the job's working directory
                task_desc = pilot.ComputeUnitDescription()
                task_desc.executable = '/bin/sh'
                task_desc.arguments = ["/%s/mandelbrot.sh" % WORKDIR, IMGX, IMGY,
                                       (IMGX/TILESX*x), (IMGX/TILESX*(x+1)),
                                       (IMGY/TILESY*y), (IMGY/TILESY*(y+1)),
                                       '%s/tile_x%s_y%s.gif' % (WORKDIR, x, y)]

                task_desc.wall_time_limit = 10
                task_desc.number_of_processes = 1

                task = pilotjob.submit_compute_unit(task_desc)
                print "* Submitted task '%s' to %s" % (task.get_id(), HOSTNAME)
                tasks.append(task)

        # ---------------------------------------------------------------------
        print "Waiting for tasks to finish..."
        pilotjob.wait()
        # ---------------------------------------------------------------------

        # copy image tiles back to our 'local' directory
        for image in workdir.list('*.gif'):
            print ' * Copying %s/%s back to %s' % (workdir.get_url(),
                                                   image, os.getcwd())
            workdir.copy(image, 'file://localhost/%s/' % os.getcwd())

        # stitch together the final image
        fullimage = Image.new('RGB', (IMGX, IMGY), (255, 255, 255))
        print ' * Stitching together the whole fractal: mandelbrot_full.gif'
        for x in range(0, TILESX):
            for y in range(0, TILESY):
                partimage = Image.open('tile_x%s_y%s.gif' % (x, y))
                fullimage.paste(partimage,
                                (IMGX/TILESX*x, IMGY/TILESY*y,
                                 IMGX/TILESX*(x+1), IMGY/TILESY*(y+1)))
        fullimage.save("mandelbrot_full.gif", "GIF")

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
