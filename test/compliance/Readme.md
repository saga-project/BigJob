Compliance Test Results
=======================

Plug-In: SSH Job
----------------

  * URL: **ssh://192.168.2.112**, Info: OS X 10.7, Date: 05/01/2012, 
    Git hash (git log -1): 991b5b8670fc2702ccd5c16c794e5b6b2eac37f9
    * 01_run_remote_exe.py ssh://192.168.2.112 s1063117: **PASS**
    * 02_run_shell_command_newline.py ssh://192.168.2.112 s1063117: **PASS**
    * 03_run_shell_command_multiline.py ssh://192.168.2.112 s1063117: **PASS**
    * 04_run_python_command_newline.py ssh://192.168.2.112 s1063117: **PASS**
    * 05_run_python_command_multiline.py ssh://192.168.2.112 s1063117: **PASS**

  * URL: ssh://ranger from Lonestar
   * By default, All tests fail with:

Exception: Couldn't find a plugin for URL scheme 'ssh://' and API type 'saga.job'

This is because the locally-installed BIGJOB is older than the version in the virtualenv, and the
virtualenv still picks it up.

You can work around this by doing:
export PYTHONPATH=

and then running the tests.  After you do this:

  * 01_run_remote_exe.py: **PASS**
  * 02_run_shell_command_newline.py: **PASS**
  * 03_run_shell_command_multiline.py: **FAIL**
  * 04_run_python_command_newline.py: **FAIL**
  * 05_run_python_command_multiline.py: **FAIL**

The problem seems to be with this line of code in the SSH adaptor:

    args = ""
        if self.arguments is not None:
            for arg in self.arguments:
                cmdline += " %r" % arg

Which creates things like:
04/16/2012 09:56:42 AM - BigJob.SSHJobPlugin(0x12122a28) - DEBUG - Sending command echo $$ && (env MYOUTPUT="Hello from BigJob"  /bin/sh -c 'python '-c' '"\nimport sys\nimport os  \nprint os.environ[\'MYOUTPUT\']\nprint sys.version\n"'')> BigJob_job.stdout 2> BigJob_job.stderr to remote server:

If you change it to:

    args = ""
        if self.arguments is not None:
            for arg in self.arguments:
                cmdline += " %s" % arg
                
Then we get:
04/16/2012 10:00:17 AM - BigJob.SSHJobPlugin(0x130bda28) - DEBUG - Sending command echo $$ && (env MYOUTPUT="Hello from BigJob"  /bin/sh -c 'python -c "import sys 

import os 

print os.environ['MYOUTPUT'] 

print sys.version"')> BigJob_job.stdout 2> BigJob_job.stderr to remote server:

Which still fails.

Any ideas?  The obvious way of getting around this is creating a remote bash script that is executed by the SSH adaptor.  The problem with this approach is that then you have to worry about multiple SSH adaptor instances potentially overwriting each other's scripts.  Is there an easier way to pass multi-line commands with SSH?

Plug-In: Local (Fork) Job
-------------------------

  * URL: **fork://localhost**, Info: OS X 10.7, Date: 04/08/2012, 
    Git hash (git log -1): 81b5df0eff27ade309416afb066c4e691c8d5f70 
    * 01_run_remote_exe.py: **PASS**
    * 02_run_shell_command_newline.py: **PASS**
    * 03_run_shell_command_multiline.py: **PASS**
    * 04_run_python_command_newline.py: **PASS**
    * 05_run_python_command_multiline.py: **PASS**
    * 06_job_container_01.py **FAIL** (Containers not implemented)
    * 07_work_directory.py fork://localhost /tmp **PASS**
    * 08_absolute_output.py fork://localhost /tmp/F **PASS**

  * URL: **fork://localhost**, Info: Ubuntu Server 10.11, Date: 04/08/2012, 
    Git hash (git log -1): 823a82b1b0c797bc97063530cc17a84b35d0dc92
    * 01_run_remote_exe.py: **PASS**
    * 02_run_shell_command_newline.py: **PASS**
    * 03_run_shell_command_multiline.py: **PASS**
    * 04_run_python_command_newline.py: **PASS**
    * 05_run_python_command_multiline.py: **PASS**

AM: can confirm fork to be working, output ok, on ubuntu-10.10 python 2.6.6

Plug-In: PBS(+SSH) Job
----------------------

  * URL: **pbs+ssh://alamo.futuregrid.org**, Info: FutureGrid Alamo Cluster, PBS 2.4.8, Date: 04/21/2012, 
    Git hash (git log -1): 81b5df0eff27ade309416afb066c4e691c8d5f70 
    * 01_run_remote_exe.py: **PASS**
    * 02_run_shell_command_newline.py: **PASS**
    * 03_run_shell_command_multiline.py: **PASS**
    * 04_run_python_command_newline.py: **PASS**
    * 05_run_python_command_multiline.py: **PASS**
    * 06_
    * 07_
    * 08_absolute_output.py pbs+ssh://alamo.futuregrid.org /tmp/ **PASS**

** installation on india.fg ** (AM)

Red Hat ELS 5.8, Python 2.7

pip, easy install and virtualenv are not available, neither with the
default system python, nor with the python module loaded.
So, installation has to be done via the pystrap script linked on
https://github.com/saga-project/BigJob/wiki/Installation, and then from
git (which needs module load git)

The pystrap script seems to work, but gives the following errors with
the default system python:

    ...
    Running setup.py install for furl
      File "/N/u/merzky/mypy_u10054/lib/python2.4/site-packages/furl/furl.py", line 92
        self._isabsolute = True if segments else False
                                 ^
    SyntaxError: invalid syntax
      File "/N/u/merzky/mypy_u10054/lib/python2.4/site-packages/furl/__init__.py", line 17
        from .furl import *
             ^
    SyntaxError: invalid syntax
    Running setup.py install for pycrypto-on-pypi
    ...
    src/MD2.c:31:20: error: Python.h: No such file or directory
    ...
    error: command 'gcc' failed with exit status 1
    ...


'module load python' gets pystrap running successfully (seems to have
python devel included).




