###################
XSEDE Specific Tips
###################

This page provides both general and specific tips for running on XSEDE infrastructure. General information is provided first, and then tips are listed by machine name (i.e. Lonestar, Kraken, Trestles, Stampede etc). If you are interested in running on a specific machine, please scroll down until you see the machine name.

If you do not see a particular machine name, BigJob may run on this machine but not be supported yet in the documentation. Please feel free to email :code:`bigjob-users@googlegroups.com` to request machine information to be added.

===================
General
===================

------------------
Where to Run
------------------

In general, on XSEDE machines, production-grade science should be done in either the :code:`$SCRATCH` or `$WORK` directories on the machine. This means you will run your BigJob script and make your BigJob :code:`agent` directory in either $SCRATCH or $WORK and **not** in $HOME.

------------------------------
Adding your Project Allocation
------------------------------

When creating BigJob scripts for XSEDE machines, it is necessary to add the :code:`project` field to the :code:`pilot_compute_description`. ::

	"project": "TG-XXXXXXXXX"

TG-XXXXX must be replaced with your individual allocation SU number as provided to you by XSEDE.

===================
Stampede
===================

----------------------
service_url
----------------------

Stampede uses the SLURM batch queuing system. When editing your scripts, the :code:`service_url` should be set to :code:`slurm+ssh://login1.stampede.tacc.utexas.edu`.


===================
Lonestar
===================

Installation of a virtual environment on Lonestar requires the use of a higher python version than the default. In order to load Python 2.7.x before installing the virtual environment, please execute::

	module load python

Then you can proceed with the Installation instructions, and make sure that you activate your virtual environment in your :code:`.bashrc` before you try to run BigJob.

You will need to put the following two lines in both your :code:`.bashrc` and your :code:`.bash_profile` in order to run on Ranger. This is due to the fact that interactive shells source a different file than regular shells. ::

	module load python
	source $HOME/bigjob/.python/bin/activate

----------------------
service_url
----------------------

Lonestar uses the Sun Grid Engine (SGE) batch queuing system. When editing your scripts, the :code:`service_url` should be set to :code:`sge://localhost` for running locally on Lonestar or :code:`sge+ssh://lonestar.tacc.utexas.edu` for running remotely.

----------------------
queues
----------------------

Commonly used queues on Lonestar to run BigJob:

+------------+------------+-----------+------------------+ 
| Queue Name | Max Runtime| Max Procs | Purpose		 |
+============+============+===========+==================+
| normal     | 24 hrs     | 4104      | normal priority	 |
+------------+------------+-----------+------------------+
| development| 1 hr       | 264       | development	 |
+------------+------------+-----------+------------------+ 
| largemem   | 24 hrs     | 48        | large memory jobs|
+------------+------------+-----------+------------------+

A complete list of Lonestar queues can be found `here <http://www.tacc.utexas.edu/user-services/user-guides/lonestar-user-guide>`_.

===================
Kraken
===================

------------------------------
Load Proper Python Environment
------------------------------

Before installing your virtual environment, you must do a :code:`module load python` on Kraken to ensure you're using Python 2.7.x instead of the system-level Python.

------------------------------
Using Lustre Scratch
------------------------------

Prior to running code on Kraken, you will need to make a directory called :code:`agent` in the same location that you are running your scripts from. The BigJob agent relies on :code:`aprun` to execute subjobs. :code:`aprun` works only if the working directory of the Pilot and Compute Units is set to the scratch space of Kraken. 

Create your agent directory in :code:`/lustre/scratch/<username>` by typing::

	cd /lustre/scratch/<username>
	mkdir agent

Replace :code:`<username>` with your Kraken username.

------------------------------
Activate your Credentials
------------------------------

To submit jobs to Kraken from another resource using gsissh, the use of myproxy is required. To start a my proxy server, execute the following command::

	myproxy-logon -T -t <number of hours> -l <your username>

You need to use your XSEDE portal username and password. To verify that your my proxy server is running, type :code:`grid-proxy-info`.

If it was successful, you should see a valid proxy running.

----------------------
service_url
----------------------

Kraken is a Cray machine with a special Torque queuing system. It requires the use of GSISSH (Globus certificates required). Initiate a grid proxy (using :code:`myproxy-logon`) before executing the BigJob application.  When editing your scripts, the :code:`service_url` should be set to :code:`xt5torque+gsissh://gsissh.kraken.nics.xsede.org`.

===================
Trestles
===================

----------------------
service_url
----------------------

Trestles uses the Torque queuing system.  When editing your scripts, the :code:`service_url` should be set to :code:`pbs+ssh://trestles.sdsc.edu`.
