#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__Author__ = "Klesti Muco"

import sys
import getpass
import bigjob.saga as saga

compute_unit_description = {
	"executable": "/bin/cat",
    "arguments": ["test.txt"],
    "number_of_processes": 1,
    "output": "stdout.txt",
    "error": "stderr.txt",
    "input_data" : [data_unit.get_url()], # this stages the
content of the data unit to the working directory of the compute unit
}
