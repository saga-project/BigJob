#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

import uuid
import saga
import state

class osg_bigjob(object):

    __slots__ = ['lrms_url', 'pilot_url', 'state', '_condor_pool']
       
    def __init__(self, database_host):  
        self.lrms_url = None
        self.pilot_url = None
        self.state = state.Unknown
        self._condor_pool = None
    
    def start_pilot_job(self, 
                        lrms_url, 
                        bigjob_agent_executable=None,
                        number_nodes=None,
                        queue=None,
                        project=None,
                        working_directory=None,
                        userproxy=None,
                        walltime=None,
                        processes_per_node=None):

        if lrms_url.startswith("condor://localhost") is not True:
            raise Exception("bigjob_osg only supports 'condor://localhost' as lrms_url.")
        else:
            self.lrms_url = lrms_url         
            self.pilot_url = "%s/%s" % (lrms_url, uuid.uuid4())       
            
            try:
                self._condor_pool = saga.job.service("condor://localhost")
            except saga.exception, e:
                print "Oh noes! A SAGA error: "
                for err in e.get_all_messages():
                    print err
                raise Exception("A SAGA error occured.")

            self.state = state.Running

    def get_state(self):        
        return self.state
    
    def get_state_detail(self): 
        pass
    
    def cancel(self):        
        self.state = state.Done

