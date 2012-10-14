#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2011-2012, Ole Christian Weidner"
__license__   = "MIT"

import bigjob.saga as saga
import unittest

###############################################################################
#
class JobIssueTests(unittest.TestCase):
    """
    Tests for job-related issues filed at https://github.com/saga-project/bliss/issues
    """
    def setUp(self):
        # Fixture:
        # called immediately before calling the test method
        pass 

    def tearDown(self):
        # Fixture:
        # called immediately after the test method has been called
        pass

    ###########################################################################
    #
    def test_issue_46(self):
        """
        https://github.com/saga-project/bliss/issues/46
        """

        jd = saga.job.Description()
        jd.executable = "/bin/date"
        jd.output = "/dev/null"
        js = saga.job.Service ("fork://localhost/") 
        j = js.create_job (jd)
        j.run ()
        j.wait()
