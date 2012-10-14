#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2011-2012, Ole Christian Weidner"
__license__   = "MIT"

import bigjob.saga as saga
import unittest
import os

from bliss.utils.which import which

###############################################################################
#
class JobMiscTests(unittest.TestCase):
    """
    Misc. job tests
    """
    def setUp(self):
        # Fixture:
        # called immediately before calling the test method
        if os.path.exists("/tmp/bliss_unittest_tmpfile"):
            os.remove("/tmp/bliss_unittest_tmpfile")

    def tearDown(self):
        # Fixture:
        # called immediately after the test method has been called
        #if os.path.exists("/tmp/bliss_unittest_tmpfile"):
        #    os.remove("/tmp/bliss_unittest_tmpfile")
        pass 

    def _file_len(self, fname):
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1


    ###########################################################################
    #
    def test_spmd_variation(self):
        """
        https://github.com/saga-project/bliss/issues/46
        """

        if which('mpirun') == None:
            self.skipTest("MPI is not installed.")

        jd = saga.job.Description()
        jd.executable          = "/bin/date"
        jd.spmd_variation      = "MPI"
        jd.number_of_processes = 8
        jd.output              = "/tmp/bliss_unittest_tmpfile"


        js = saga.job.Service ("fork://localhost/") 
        j = js.create_job (jd)
        j.run ()
        j.wait()

        if self._file_len("/tmp/bliss_unittest_tmpfile") <= 1:
            self.fail("Unexpected output for SPMDVariation=8")

