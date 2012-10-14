#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2011-2012, Ole Christian Weidner"
__license__   = "MIT"

import bigjob.saga as saga
import unittest
import traceback

###############################################################################
#
class JobDescriptionTests(unittest.TestCase):
    """
    Tests for the saga.job.Description API
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
    def test_scalar_string_properties(self):
        """
        Tests the saga.job.description attribute interface (scalar string attributes)
        """
 
        properties = {"Executable"       : "executable",
                      "Queue"            : "queue",
                      "Project"          : "project",
                      "Output"           : "output",
                      "Error"            : "error",
                      "WorkingDirectory" : "working_directory",
                      "Contact"          : "contact",
                      "Name"             : "name"
        }

        try:
            svalue = 'jsaoiudjca89s7d'
            vvalue = ['sdsd', 'cus6v']
            jd = saga.job.Description()

            for (attr_key, attr_val) in properties.items():

                if getattr(jd, attr_val) != None:
                    self.fail("Attribute Error - should have been None")
                if jd.attribute_exists(attr_key) != False:
                    self.fail("Attribute Error - should have been False")

                setattr(jd, attr_val, svalue)
                if getattr(jd, attr_val) != svalue:
                    self.fail("Attribute Error - unexpected value")
                if jd.get_attribute(attr_key) != svalue:
                    self.fail("Attribute Error - unexpected value")
 
                try:
                    setattr(jd, attr_val, 12)  # shouldn't accept anything but str
                #   self.fail("Attribute Error - shouldn't accept non-str value")
                #   AM: auto-conversion ...
                except saga.Exception:
                    pass
                jd.set_attribute(attr_key, "XX")
                if getattr(jd, attr_val) != "XX":
                    self.fail("Attribute Error - unexpected value")


                # # JD attributes cannot be removed on API level - they can only be unset
                # jd.remove_attribute(attr_key)
                # try:
                #     getattr(jd, attr_val)  # should raise DoesNotExist
                #     self.fail("Attribute Error - should have Failed")
                # except saga.Exception:
                #     pass
                
                # Queue cannot be removed as attribute - it can only be unset
                jd.set_attribute(attr_key, None)
                try:
                    if None != getattr(jd, attr_val) :
                        self.fail("Attribute Error - should have Failed")
                except saga.Exception:
                    pass
 
        except saga.Exception, e: 
            self.fail(e)

    ###########################################################################
    #
    def test_scalar_int_properties(self):
        """
        Tests the saga.job.description attribute interface (scalar int attributes)
        """
 
        properties = {#"TotalCPUCount"    : "total_cpu_count",
                      #"NumberOfProcesses": "number_of_processes",
                      "WallTimeLimit"    : "wall_time_limit"
        }

        try:
            svalue = 123423
            vvalue = [5345, 567567]
            jd = saga.job.Description()

            for (attr_key, attr_val) in properties.items():

                if getattr(jd, attr_val) != None:
                    self.fail("Attribute Error - should have been None")
                if jd.attribute_exists(attr_key) != False:
                    self.fail("Attribute Error - should have been False")

                setattr(jd, attr_val, svalue)
                if getattr(jd, attr_val) != svalue:
                    self.fail("Attribute Error - unexpected value")
                if jd.get_attribute(attr_key) != svalue:
                    self.fail("Attribute Error - unexpected value")

                try:
                    setattr(jd, attr_val, [1])  # shouldn't accept anything but int
                  # self.fail("Attribute Error - shouldn't accept non-int value")
                  # auto attribute conversion works...
                except saga.Exception:
                    pass 

                jd.set_attribute(attr_key, 16)
                if getattr(jd, attr_val) != 16:
                    self.fail("Attribute Error - unexpected value")

                jd.remove_attribute(attr_key)
                try:
                    getattr(jd, attr_val)
                    self.fail("Attribute Error - should have failed")
                except : 
                    pass
 
        except saga.Exception, e: 
            self.fail(e)

    ###########################################################################
    #
    def test_vector_dict_properties(self):
        """
        Tests the saga.job.description attribute interface (vector 'dict' attributes)
        """
 
        properties = {"Environment"    : "environment",
        }

        try:
            svalue = {'foo':'bar'}
            vvalue = [{'key1':'val1'}, {'key2':'val2'}]
            jd = saga.job.Description()

            for (attr_key, attr_val) in properties.items():

                if getattr(jd, attr_val) != None:
                    self.fail("Attribute Error - should have been None")
                if jd.attribute_exists(attr_key) != False:
                    self.fail("Attribute Error - should have been False")

                setattr(jd, attr_val, svalue)
                if getattr(jd, attr_val) != svalue:
                    self.fail("Attribute Error - unexpected value %s / %s" %  (getattr(jd, attr_val), svalue))
                if jd.get_vector_attribute(attr_key) != svalue:
                    self.fail("Attribute Error - unexpected value")

                try:
                    setattr(jd, attr_val,["ss=1", "gg=2"])  # this should work, too.
                except saga.Exception:
                    self.fail("Attribute Error - list -> dict conversion failed")

                jd.set_vector_attribute(attr_key, {"x":"z"})
                if getattr(jd, attr_val) != {"x":"z"}:
                    self.fail("Attribute Error - unexpected value")

                jd.remove_attribute(attr_key)
                try:
                    getattr(jd, attr_val)
                    self.fail("Attribute Error - should have failed")
                except : 
                    pass
 
        except saga.Exception, e: 
            self.fail(e)



    ###########################################################################
    #
    def test_vector_list_properties(self):
        """
        Tests the saga.job.description attribute interface (vector 'dict' attributes)
        """
 
        properties = {"Arguments"    : "arguments",
        }

        try:
            svalue = ['foo','bar']
            vvalue = [['key1','val1'], ['key2','val2']]
            jd = saga.job.Description()

            for (attr_key, attr_val) in properties.items():

                if getattr(jd, attr_val) != None:
                    self.fail("Attribute Error - should have been None")
                if jd.attribute_exists(attr_key) != False:
                    self.fail("Attribute Error - should have been False")

                setattr(jd, attr_val, svalue)
                if getattr(jd, attr_val) != svalue:
                    self.fail("Attribute Error - unexpected value")
                if jd.get_vector_attribute(attr_key) != svalue:
                    self.fail("Attribute Error - unexpected value")

                try:
                    setattr(jd, attr_val, {"ss":"gg"})  # shouldn't accept anything but list
                  # self.fail("Attribute Error - shouldn't accept non-list value")
                  # auto attribute conversion works
                except saga.Exception:
                    pass

                jd.set_vector_attribute(attr_key, ["x","z"])
                if getattr(jd, attr_val) != ["x","z"]:
                    self.fail("Attribute Error - unexpected value")

                jd.remove_attribute(attr_key)
                try:
                    getattr(jd, attr_val)
                    self.fail("Attribute Error - should have failed")
                except : 
                    pass
 
        except saga.Exception, e: 
            self.fail(e)

    ###########################################################################
    #
    def test_deep_copy(self) :
        """
        Test if job description is deep copy on create_job() as required by GFD.90 
        """

        try :
            jd = saga.job.Description()
            jd.executable = 'true'

            js = saga.job.Service ('fork://localhost')
            j  = js.create_job (jd)
            j.run  ()
            j.wait ()

            jd.executable = 'false'

            jd_ret = j.get_description()

            if jd_ret.executable != 'true' : 
                self.fail("Deep Copy Error: job description has changed unexpectedly")

        except saga.Exception, e : 
            self.fail(e)

    ###########################################################################
    #
    def test_spmd_variation(self) :
        """
        Test if job description spmd_variation works as expected
        """

        jd = saga.job.Description()

        try :
            jd.spmd_variation = 'MPI'
            if jd.get_attribute("SPMDVariation") != 'MPI':
                self.fail("Unexpected SPMDVariation attribute")
            if jd.spmd_variation != 'MPI':
                self.fail("Unexpected SPMDVariation attribute")

            jd.spmd_variation = "OpenMP"
            if jd.get_attribute("SPMDVariation") != 'OpenMP':
                self.fail("Unexpected SPMDVariation attribute")
            if jd.spmd_variation != 'OpenMP':
                self.fail("Unexpected SPMDVariation attribute")
        
        except saga.Exception, e:
            print "XXXX: " + jd.spmd_variation
            self.fail(e)

        try:
            jd.spmd_variation = 'now what?'
            self.fail("Exception in case of unsupported attribute value expected!")
        except saga.Exception, e:
            pass
