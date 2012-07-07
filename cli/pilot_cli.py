'''
Command Line Util for using BigJob (via the Pilot-API)
'''
import argparse
import sys
import os
import pdb
import pickle

import bigjob
from pilot import PilotComputeService, PilotCompute, ComputeUnit, State
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
        
        # create pilot job service and initiate a pilot job
        pilot_compute_description = {
                             "service_url": resource_url,
                             "number_of_processes": 1,                             
                             "working_directory": os.path.join(os.getcwd(), working_directory)  ,
                             "number_of_processes": number_cores,
                             "processes_per_node": cores_per_node                             
                            }
        self.submit_pilot_by_description(coordination_url, pilot_compute_description)
        
    
    def submit_pilot_by_description(self, coordination_url="redis://localhost/", pilot_compute_description={}):
        pilot_compute_service = PilotComputeService(coordination_url=coordination_url)
        pilot_compute = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
        pilot_url = pilot_compute.get_url()
        self.pilots.append(pilot_url)
        print("Started Pilot: %s"%(pilot_url))
        self.__persist()
        

    def cancel_pilot(self, pilot_url):
        pilot_compute = PilotCompute(pilot_url=pilot_url)
        pilot_compute.cancel()
        self.pilots.remove(pilot_url)
        self.__persist()
        
    
    def list_pilots(self):
        print "\nPilot Compute\t\t\t\t\t\t\t\t\tState"
        print "-----------------------------------------------------------------------------------------------------"
        if len(self.pilots)==0:
            print "No pilot found"
        for i in self.pilots:
            pilot_compute = PilotCompute(pilot_url=i)
            print "%s\t%s"%(pilot_compute.get_url(), pilot_compute.get_state())
        print ""
        
    
    def list_cus(self, pilot_url):
        pilot_compute = PilotCompute(pilot_url=pilot_url)
        cus = pilot_compute.list_compute_units()
        counter=1
        print "\nPilot Compute: %s"%(pilot_compute.get_url())
        print "State: %s"%(pilot_compute.get_state())
        print "#\tCompute Unit\t\t\t\t\tState\tQueue\tRuntime"
        print "-----------------------------------------------------------------------------------------"
        if len(cus)==0:
            print "No Compute Unit found."
        for i in cus:
            url = i.get_url()
            short_url = url[url.index("sj"):]
            if short_url.find("/?")!=-1:
                short_url=short_url[:short_url.index("/?")]
            queue_time = 0.0
            run_time = 0.0
            try:
                pass
                #details = i.get_details()
                #state = details["state"]
            except:
                pass
                
            state = i.get_state()
            details=i.get_details()
            if details.has_key("start_time") and details.has_key("end_queue_time"):
                queue_time = float(details["end_queue_time"]) - float(details["start_time"])
            if details.has_key("end_time") and details.has_key("end_queue_time"):
                run_time = float(details["end_time"]) - float(details["end_queue_time"])
            print "%d\t%s\t\t%s\t%.1f\t%.1f"%(counter, short_url, state, queue_time, run_time)
            counter = counter + 1 
        print ""
            

    def submit_cu(self, pilot_url, command):
        """ submits CUs (does not waits for completion) """
        #print "Submit CU to %s"%(pilot_url)
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
        return self.submit_cu_by_description(pilot_url, compute_unit_description)
    
    
    
    def submit_cu_by_description(self, pilot_url, compute_unit_description={}):
        pilot_compute = PilotCompute(pilot_url=pilot_url)
        compute_unit = pilot_compute.submit_compute_unit(compute_unit_description)
        print "Started ComputeUnit: %s"%(compute_unit.get_url())
        return compute_unit
            
        
    def run_cu(self, pilot_url, command):
        """ submits CU and waits for completion """
        compute_unit=self.submit_cu(pilot_url, command)
        print "Waiting for termination"
        compute_unit.wait()
        print "CU %s terminated"%compute_unit.get_url()
        return compute_unit
    
    
    def run_cu_by_description(self, pilot_url, compute_unit_description={}):
        compute_unit=self.submit_cu_by_description(pilot_url, compute_unit_description)
        print "Waiting for termination"
        compute_unit.wait()
        print "CU %s terminated"%compute_unit.get_url()
        return compute_unit
    
    
    def wait_cu(self, pilot_url):
        pilot_compute = PilotCompute(pilot_url=pilot_url)
        pilot_compute.wait()
        
        
    def cancel_cu(self, cu_url):
        compute_unit = ComputeUnit(cu_url=cu_url)
        compute_unit.cancel()
        print("Terminated CU: %s"%(cu_url))
    
    
    def get_cu_state(self, cu_url):
        compute_unit = ComputeUnit(cu_url=cu_url)
        print "Compute Unit: %s State: %s"%(cu_url, compute_unit.get_state())
    
    ###########################################################################
    # auxiliary methods

    def version(self):
        print "BigJob Version: " + bigjob.version
    
    def clean(self):
        os.remove(self.__get_save_filename())
    
    ###########################################################################
    # private and protected methods
    
    def __persist(self):
        f = open(self.__get_save_filename(), 'wb')
        pickle.dump(self.pilots, f)
        f.close()
    
    def __restore(self):
        if os.path.exists(self.__get_save_filename()):
            try:
                f = open(self.__get_save_filename(), 'rb')
                self.pilots = pickle.load(f)
                f.close()
            except:
                pass

    def __get_save_filename(self):
        return os.path.join(os.path.expanduser(BIGJOB_DIRECTORY), 'pilot-cli.p')


def main():
    app = BigJobCLI()
    parser = argparse.ArgumentParser(add_help=True, description="""BigJob Command Line Utility""")
    
    parser.add_argument('--coordination', '-c', default="redis://localhost")
    parser.add_argument('--clean', action="store_true", default=False)
    parser.add_argument('--version', action="store_true", default=False)    
    
    pilot_group = parser.add_argument_group('Manage pilots')
    pilot_group.add_argument('--number_cores', default="1")
    pilot_group.add_argument('--cores_per_node',  default="1")    
    pilot_group.add_argument('--submit_pilot', action="store", nargs="?", metavar="RESOURCE_URL", 
                              help="submit a pilot to specified resource, e.g. fork://localhost",
                              default=False)    
    pilot_group.add_argument('--pilot_description', action="store", metavar="PILOT_COMPUTE_DESCRIPTION", 
                           help="""accepts a Pilot-API pilot compute description in JSON format: 
                                   \"{'service_url': 'fork://localhost', 'number_of_processes': 1, 'working_directory': '/tmp'}\"""", 
                                default=False)
    
    
    pilot_group.add_argument('--cancel_pilot', action="store", default=False, metavar="PILOT_URL", 
                               help="Cancel pilot")
    pilot_group.add_argument('--list_pilots', action="store_true", default=False, help="list all pilots")
    pilot_group.add_argument('--wait_cus', action="store_true", default=False, help="wait for termination of all CUs")
    pilot_group.add_argument('--list_cus', action="store",  metavar="PILOT_URL", default=False)
    
    
    cu_group = parser.add_argument_group('Manage compute units')
    cu_group.add_argument('--submit_cu', action="store", nargs="+", metavar=("PILOT_URL", "COMMAND ARGS"), 
                           help="submit CU to pilot", default=False)
    cu_group.add_argument('--compute_unit_description', action="store", metavar="COMPUTE_UNIT_DESCRIPTION", 
                           help="""accepts a Pilot-API compute unit description in JSON format: 
                           \"{'executable': '/bin/date', 'arguments': [''], 'total_core_count': 1, 'number_of_processes': 1, 'output': 'stdout.txt', 'error': 'stderr.txt'}\"""", 
                            default=False)
    
    cu_group.add_argument('--run_cu', action="store", nargs="+", metavar=("PILOT_URL", "COMMAND ARGS"), 
                           help="submit CU to pilot and wait for completion", default=False)
    cu_group.add_argument('--get_cu_state', action="store", metavar="CU_URL", default=False)
    cu_group.add_argument('--cancel_cu', action="store", metavar="CU_URL", default=False)
    
    parsed_arguments = parser.parse_args()    
    #print(str(parsed_arguments))
    
    if parsed_arguments.submit_pilot!=False:
        if parsed_arguments.pilot_description!=False:
            pilot_description = eval(parsed_arguments.pilot_description)
            app.submit_pilot_by_description(coordination_url=parsed_arguments.coordination,
                                            pilot_compute_description=pilot_description)            
        else:
            app.submit_pilot(coordination_url=parsed_arguments.coordination,
                             resource_url=parsed_arguments.submit_pilot,
                             number_cores=parsed_arguments.number_cores,
                             cores_per_node=parsed_arguments.cores_per_node)    
    elif parsed_arguments.list_pilots!=False:
        app.list_pilots()
    elif parsed_arguments.cancel_pilot!=False:
        app.cancel_pilot(parsed_arguments.cancel_pilot)
    elif parsed_arguments.submit_cu!=False:
        if parsed_arguments.compute_unit_description!=False:
            compute_unit_description = eval(parsed_arguments.compute_unit_description)
            app.submit_cu_by_description(parsed_arguments.submit_cu[0], 
                                         compute_unit_description=compute_unit_description)            
        else:
            app.submit_cu(parsed_arguments.submit_cu[0], parsed_arguments.submit_cu[1:])
    elif parsed_arguments.run_cu!=False:
        if parsed_arguments.compute_unit_description!=False:
            compute_unit_description = eval(parsed_arguments.compute_unit_description)
            app.run_cu_by_description(parsed_arguments.run_cu[0],
                                            compute_unit_description=compute_unit_description)            
        else:
            app.run_cu(parsed_arguments.run_cu[0], parsed_arguments.run_cu[1:])    
    elif parsed_arguments.get_cu_state!=False:
        app.get_cu_state(parsed_arguments.get_cu_state)
    elif parsed_arguments.wait_cus!=False:
        app.wait_cu(parsed_arguments.wait_cus)
    elif parsed_arguments.list_cus!=False:
        app.list_cus(parsed_arguments.list_cus)
    elif parsed_arguments.cancel_cu!=False:
        app.list_cus(parsed_arguments.cancel_cu)
    elif parsed_arguments.clean==True:
        app.clean()
    elif parsed_arguments.version==True:
        app.version()
        
if __name__ == '__main__':
    main()
    
    