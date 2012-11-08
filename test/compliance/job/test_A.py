#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__Author__ = "Klesti Muco"

import sys
import getpass
import bigjob.saga as saga

compute_unit_description = {
	"executable": "/bin/echo",
    "arguments": ["$HOSTNAME", "Hello","$ENV1","$ENV2"],
    "environment": ['ENV1=env_arg1','ENV2=env_arg2'],
    "number_of_processes": 4,
    "spmd_variation":"mpi",
    "output": "stdout.txt",
    "error": "stderr.txt",
    "output_data" : "url to localhost"
}

