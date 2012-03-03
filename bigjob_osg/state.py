#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

import saga

Running = "Running"
New     = "New"
Failed  = "Failed"
Done    = "Done"
Unknown = "Unknown"

def saga_to_subjob_state(saga_state):
    if saga_state == saga.job.Running:
        return Running
    elif saga_state == saga.job.Failed: 
        return Failed
    elif saga_state == saga.job.New:
        return New
    elif saga_state == saga.job.Done:
        return Done
    else:
        return Unknown
