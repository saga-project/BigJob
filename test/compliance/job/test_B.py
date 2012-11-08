#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__Author__ = "Klesti Muco"

import sys
import getpass
import bigjob.saga as saga

compute_unit_description = {
	"executable": "$MY_EXEC",
	"arguments": ["$HOSTNAME", "Hello","$ENV1","$ENV2"],
	"environment": ['MY_EXEC=/bin/echo','ENV1=env_arg1','ENV2=env_arg2'],
	"number_of_processes": 1,
	"spmd_variation":"single",
	"output": "stdout.txt",
	"error": "stderr.txt",
	"output_data" : "url to localhost"
}
