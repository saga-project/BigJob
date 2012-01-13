#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

import saga
import state

class osg_subjob(object):
    
    __slots__ = ['state', 'description', '_condor_job']
    
    def __init__(self):
        self.state = state.Unknown
        self._condor_job = None
    
    def submit_job(self, bigjob, jd):
        self.description = jd
        try:
            self._condor_job = bigjob._condor_pool.create_job(self.description)
            self._condor_job.run()
        except saga.exception, e:
            print "Oh noes! A SAGA error: " 
            for err in e.get_all_messages():
                print err
            raise Exception("A SAGA error occured.")

    
    def get_state(self):        
        self._condor_job.get_state()
    
    def cancel(self):
        self._condor_job.cancel()

