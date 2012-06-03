'''
Command Line Util for using BigJob (via the Pilot-API)
'''

# CLI tools
import argparse

import sys
import os
import pdb
import pickle

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
        pilot_url = os.path.join(coordination_url, pilot_compute.get_url())
        self.pilots.append(pilot_url)
        print("Started Pilot: %s"%(pilot_url))
        self.__persist()

    def cancel_pilot(self, pilot_url):
        pass
    
    def list_pilots(self):
        for i in self.pilots:
            print str(i)
    

    def submit_cu(self, pilot_url):
        pilot_compute = Pilot

    def cancel_cu(self, cu_url):
        pass
    
    def list_cus(self, pilot_url):
        pass

    
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
    
    parser = argparse.ArgumentParser(description="BigJob Command Line Utility")
    
    parser.add_argument('--coordination', '-c', default="redis://localhost")
    parser.add_argument('--resource', '-r', default="fork://localhost")
    parser.add_argument('--number_cores', '-n', default="1")
    parser.add_argument('--cores_per_node', '-cpn',  default="1")
    parser.add_argument('--list_pilots', '-lp', action="store_true", default=False)
    parser.add_argument('--submit_pilot', '-sp', action="store_true", default=False)
    parser.add_argument('--cancel_pilot', '-cp', action="store_true", default=False)
    parser.add_argument('--submit_cu', '-sc', action="store_true", default=False)
    parser.add_argument('--cancel_cu', '-cc', action="store_true", default=False)
    parser.add_argument('--list_cu', '-lc', action="store_true", default=False)
    parser.add_argument_group()
    parser.add_argument('--clean', action="store_true")    
    parsed_arguments = parser.parse_args()    
    print(str(parsed_arguments))
    
    
    if parsed_arguments.submit_pilot==True:
        app.submit_pilot(coordination_url=parsed_arguments.coordination,
                         resource_url=parsed_arguments.resource,
                         number_cores=parsed_arguments.number_cores,
                         cores_per_node=parsed_arguments.cores_per_node)    
    elif parsed_arguments.list_pilots==True:
        app.list_pilots()
    elif parsed_arguments.submit_cu==True:
        pass
    elif parsed_arguments.clean==True:
        app.clean()
    
    print("Finished Processing")
    
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