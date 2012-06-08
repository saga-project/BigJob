'''
Command Line Util for using BigJob (via the Pilot-API)
'''

# CLI tools
import argparse

import sys
import os
import pdb
import pickle

import bigjob
from pilot import PilotComputeService, PilotCompute, State
BIGJOB_DIRECTORY="~/.bigjob/" 
  
class BigJobCLI(object):
    
    
    
    def __init__(self):
        self.pilots = []
        self.__restore()
        
    def submit_pilot(self, coordination_url="redis://localhost/",
                           resource_url="fork://localhost",
                           working_directory=".",
                           number_cores=1,
                           cores_per_node=1,
                     ):
        pilot_compute_service = PilotComputeService(coordination_url=coordination_url)

        # create pilot job service and initiate a pilot job
        pilot_compute_description = {
                             "service_url": resource_url,
                             "number_of_processes": 1,                             
                             "working_directory": os.path.join(os.getcwd(), working_directory)  ,
                             "number_of_processes": number_cores,
                             "processes_per_node": cores_per_node                             
                            }
    
        pilot_compute = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
        pilot_url = pilot_compute.get_url()
        self.pilots.append(pilot_url)
        print("Started Pilot: %s"%(pilot_url))
        self.__persist()

    def cancel_pilot(self, pilot_url):
        pilot_compute = PilotCompute(pilot_url=pilot_url)
        pilot_compute.cancel()
    
    def list_pilots(self):
        for i in self.pilots:
            print str(i)
    

    def submit_cu(self, pilot_url, command):
        """ submits CUs (does not waits for completion) """
        print "submit CU to %s"%(pilot_url)
        pilot_compute = PilotCompute(pilot_url=pilot_url)
        args= []
        if len(command)>1:
            args = command[1:]
        compute_unit_description = {
            "executable": command[0],
            "arguments": args,
            "total_core_count": 1,
            "number_of_processes": 1,            
            "output": "stdout.txt",
            "error": "stderr.txt",
        }    
        compute_unit = pilot_compute.submit_compute_unit(compute_unit_description)
        return compute_unit
        
        
    def run_cu(self, pilot_url, command):
        """ submits CU and waits for completion """
        compute_unit=self.submit_cu(pilot_url, command)
        compute_unit.wait()
        return compute_unit

    
    def wait_cu(self, pilot_url):
        pilot_compute = PilotCompute(pilot_url=pilot_url)
        pilot_compute.wait()
        
        
    def cancel_cu(self, cu_url):
        pass
    
    
    def list_cus(self, pilot_url):
        pass


    def version(self):
        print "BigJob Version: " + bigjob.version
    
    def clean(self):
        os.remove(self.__get_save_filename())
    
    def __persist(self):
        pickle.dump(self.pilots, open(self.__get_save_filename(), 'wb'))
    
    def __restore(self):
        if os.path.exists(self.__get_save_filename()):
            try:
                self.pilots = pickle.load(open(self.__get_save_filename(), 'rb'))
            except:
                pass

    def __get_save_filename(self):
        return os.path.join(os.path.expanduser(BIGJOB_DIRECTORY), 'pilot-cli.p')

if __name__ == '__main__':

    app = BigJobCLI()
    
    parser = argparse.ArgumentParser(add_help=True, description="""BigJob Command Line Utility""")
    
    parser.add_argument('--coordination', '-c', default="redis://localhost")
    parser.add_argument('--clean', action="store_true")
    parser.add_argument('--version', action="store_true")    
    
    #subparsers = parser.add_subparsers(help='Commands for managing pilots and compute units')
    #pilot_parser = subparsers.add_parser("pilot", help='Manage pilots')
    #cu_parser = subparsers.add_parser("cu", help='Manage compute units')
    
    pilot_group = parser.add_argument_group('Manage pilots')
    pilot_group.add_argument('--number_cores', default="1")
    pilot_group.add_argument('--cores_per_node',  default="1")    
    pilot_group.add_argument('--submit_pilot', action="store", metavar="RESOURCE_URL", 
                              help="submit a pilot to specified resource, e.g. fork://localhost")    
    pilot_group.add_argument('--cancel_pilot', action="store", metavar="PILOT_URL", 
                               help="Cancel pilot")
    pilot_group.add_argument('--list_pilots', action="store_true", default=False, help="list all pilots")
    
    #pilot_parser.add_argument('cancel', nargs=2, metavar="cancel <PILOT_URL>",  action="store", 
    #                          help="cancel pilot")
    
    cu_group = parser.add_argument_group('Manage compute units')
    cu_group.add_argument('--submit_cu', action="store", nargs="+", metavar=("PILOT_URL", "COMMAND ARGS"),  default=False)
    cu_group.add_argument('--cancel_cu', action="store", metavar="CU_URL", default=False)
    cu_group.add_argument('--list_cus', action="store_true", default=False)
    
    
    parsed_arguments = parser.parse_args()    
    print(str(parsed_arguments))
    
    
    if parsed_arguments.submit_pilot!=None:
        app.submit_pilot(coordination_url=parsed_arguments.coordination,
                         resource_url=parsed_arguments.submit_pilot,
                         number_cores=parsed_arguments.number_cores,
                         cores_per_node=parsed_arguments.cores_per_node)    
    elif parsed_arguments.list_pilots==True:
        app.list_pilots()
    elif parsed_arguments.submit_cu!=None:
        app.submit_cu(parsed_arguments.submit_cu[0], parsed_arguments.submit_cu[1:])
    elif parsed_arguments.clean==True:
        app.clean()
    elif parsed_arguments.version==True:
        app.version()
    
    #print("Finished Processing")
    
    #parser.print_help()
    
    
    
#    p = optparse.OptionParser(description='Manage a set of Pilots (BigJobs)',)
#    p.add_option('--coordination', '-c', default="redis://localhost")
#    p.add_option('--resource', '-r', default="fork://localhost")
#    p.add_option('--number_cores', '-n', default="1")
#    p.add_option('--cores_per_node',  default="1")
#    p.add_option('--list', '-l')
#    p.add_option('--start')
#    p.add_option('--stop')
#    p.add_option('--submit')
#    p.add_option('-v', '--verbose', description="raise the verbosity")
#    options, arguments = p.parse_args()