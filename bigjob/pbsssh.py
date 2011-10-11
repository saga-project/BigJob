#!/usr/bin/env python

import textwrap

class pbsssh:
    """Constructor"""
    def __init__(self):
        bootstrap_script=None
        walltime=None
        nodes=1
        ppn=4
  
    def submit_remote_pbs_script(self,bootstrap_script,walltime,nodes,ppn):
        script = textwrap.dedent("""import sys
import os
import urllib
import sys
import time
import textwrap

qsub_file_name="bigjob_pbs_ssh"

qsub_file = open(qsub_file_name, "w")
qsub_file.write("#PBS -l nodes=%s:ppn=%s")
qsub_file.write("\\n")
qsub_file.write("#PBS -l walltime=%s")
qsub_file.write("\\n")
qsub_file.write("python -c XX" + textwrap.dedent(\"\"%s\"\") + "XX")
qsub_file.close()
os.system( "/usr/local/bin/qsub  " + qsub_file_name)
""" % (str(nodes),str(ppn),str(walltime),bootstrap_script))
        return script

    def escape_pbs_ssh(self,bootstrap_script):
        ### escaping characters
        bootstrap_script = bootstrap_script.replace("\"","\\\"")
        bootstrap_script = bootstrap_script.replace("\\\\","\\\\\\\\\\")
        bootstrap_script = bootstrap_script.replace("XX","\\\\\\\"")
        bootstrap_script = "\"" + bootstrap_script+ "\""
        return bootstrap_script
