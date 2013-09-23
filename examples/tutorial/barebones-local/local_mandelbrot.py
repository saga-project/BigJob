import os, time, sys
from PIL import Image
import bliss.saga as saga 
from pilot import PilotComputeService, ComputeDataService, State

# the dimension (in pixel) of the whole fractal
imgx = 8192 
imgy = 8192

# the number of tiles in X and Y direction
tilesx = 2
tilesy = 2

	
### This is the number of jobs you want to run
NUMBER_JOBS=4
COORDINATION_URL = "redis://localhost:6379"

if __name__ == "__main__":

    pilot_compute_service = PilotComputeService(COORDINATION_URL)
    
    # copy image tiles back to our 'local' directory
    dirname = 'sftp://localhost/%s/PJ-mbrot/' % '/tmp'
    workdir = saga.filesystem.Directory(dirname, saga.filesystem.Create)

    pilot_compute_description={ "service_url": "fork://localhost",
                                "number_of_processes": 12,
                                "working_directory": workdir.get_url().path,
                                "walltime":10
                              }

    pilot_compute_service.create_pilot(pilot_compute_description)

    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)

    print ("Finished Pilot-Job setup. Submitting compute units")

    # submit compute units
    for x in range(0, tilesx):
        for y in range(0, tilesy):                
            # describe a single Mandelbrot job. we're using the 
            # directory created above as the job's working directory
            outputfile = 'tile_x%s_y%s.gif' % (x,y)
                
            compute_unit_description = {
                        "executable": "python",
                        "arguments": [os.getenv("HOME")+'/mandelbrot.py', str(imgx), str(imgy), 
                                        str(imgx/tilesx*x), str(imgx/tilesx*(x+1)),
                                        str(imgy/tilesy*y), str(imgy/tilesy*(y+1)),
                                        outputfile],
                        "number_of_processes": 1,    
                        "working_directory":workdir.get_url().path,        
                        "output": "stdout_x%s_y%s.txt" % (x,y),
                        "error": "stderr_x%s_y%s.txt" % (x,y),
                        }    
            compute_data_service.submit_compute_unit(compute_unit_description)

    print ("Waiting for compute units to complete")
    compute_data_service.wait()
                
    # Preparing the final image
    for image in workdir.list('*.gif'):
        print ' * Copying %s/%s back to %s' % (workdir.get_url(), image, os.getcwd())
        workdir.copy(image, 'sftp://localhost/%s/' % os.getcwd()) 

    # stitch together the final image
    fullimage = Image.new('RGB',(imgx, imgy),(255,255,255))
    print ' * Stitching together the whole fractal: mandelbrot_full.png'
    for x in range(0, tilesx):
        for y in range(0, tilesy):
            partimage = Image.open('tile_x%s_y%s.gif' % (x, y))
            fullimage.paste(partimage, (imgx/tilesx*x, imgy/tilesy*y, imgx/tilesx*(x+1), imgy/tilesy*(y+1)) )
    fullimage.save("mandelbrot_full.gif", "GIF")

    print ("Terminate Pilot Jobs")
    compute_data_service.cancel()    
    pilot_compute_service.cancel()
