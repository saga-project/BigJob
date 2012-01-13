#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

from bigjob_osg import bigjob, subjob, description, state


def bigjob_osg_example_simple():
    lrms_url = "condor://localhost"

    print "Start Pilot Job/BigJob at: " + lrms_url
    bj = bigjob(None)

    bj.start_pilot_job(lrms_url)
    print "Pilot Job/BigJob URL: " + bj.pilot_url + " State: " + str(bj.get_state())


    bj.cancel()
    print "Pilot Job/BigJob URL: " + bj.pilot_url + " State: " + str(bj.get_state())

if __name__ == "__main__":
    bigjob_osg_example_simple()
