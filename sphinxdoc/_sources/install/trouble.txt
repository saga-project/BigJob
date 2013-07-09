###############
Troubleshooting
###############

Having trouble with your BigJob installation? We're here to help! Below is a list of some common installation problems. If your problem persists, you can always message us at `bigjob-users@googlegroups.com <bigjob-users@googlegroups.com>`_.

If you are encountering errors that aren't listed below, set the environment variable :code:`$BIGJOB_VERBOSE=100` in your :code:`.bashrc`.

======================
Common Error Messages
======================

1. The most common problems we encounter are with incorrect python version. 

In these cases, :code:`import pilot` may return::

	Traceback (most recent call last):
  	File "<string>", line 1, in <module>
	ImportError: No module named pilot

Using a virtualenv will modify your Python path, but you can verify that you are using the correct Python in two ways. From command line::
	
	which python

should return the installation directory where you installed BigJob (i.e. $HOME/.bigjob/python/...).

On remote resources such as XSEDE, before installing your virtualenv, you must be using Python 2.7.x. Some of these resources use Python 2.4 or Python 2.6 by default. You can use :code:`module load python` to upgrade to Python 2.7.x.

Verify that your python version is correct at the destination by trying::

	ssh <name-of.remote.resource> "python -V"

If this does not give the correct python version, check your :code:`.bashrc` at the destination to verify that you source your virtual environment.

2. My stdout file doesn't contain the output of /bin/date but "ssh: connect to host localhost port 22: Connection refused"

BigJob utilizes ssh for the execution of sub-jobs. Please ensure that your local SSH daemon is up and running and that you can login without password.

==========================
Frequently Asked Questions
==========================

**Q: How can I update my existing BigJob package?**::

	easy_install -U bigjob

**Q: How do I execute and reconnect to long-running sessions of BigJob in a Unix terminal?**

The UNIX :code:`screen` tool can / should be used to re-connect to a running BigJob session on a remote machine. For documentation on screen, please see `Screen Manpage <http://www.slac.stanford.edu/comp/unix/package/epics/extensions/iocConsole/screen.1.html>`_.

You should not just submit a BigJob from your local machine to a remote host and then close the terminal without the use of screen.

**Q: Can I reconnect to a current running BigJob?**

Yes, if your BigJob manager (or application) terminates before all ComputeUnits terminate, you can reconnect to a running pilot by providing a :code:`pilot_url` to the PilotCompute constructor. For example::


	pilot = PilotCompute(pilot_url="redis://localhost:6379/bigjob:bj-a7bfae68-25a0-11e2-bd6c-705681b3df0f:localhost")

**Q: Why is BigJob downloading an installation package?**

BigJob attempts to install itself, if it can't find a valid BJ installation on a resource (i.e. if :code:`import pilot` fails). By default BigJob searches for :code:`$HOME/.bigjob/python` for a working BJ installation. Please, make sure that the correct Python is found in your default paths. If BJ attempts to install itself despite being already installed on a resource, this can be a sign that the wrong Python is found.










