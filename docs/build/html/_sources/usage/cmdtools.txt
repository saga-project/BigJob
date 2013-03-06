##################
Command-Line Tools
##################

BigJob (>0.4.65) provides a command-line interface (:code:`pilot-cli`), which is installed with the BigJob Package.

Pilot-CLI
-----------

1. To start a pilot::

	$ pilot-cli --coordination redis://localhost:6379  --submit_pilot fork://localhost
	Started Pilot: redis://localhost:6379/bigjob:bj-2ccb20f8-b18c-11e1-b5e4-109addae22a3:localhost

2. List all pilots::

	$ pilot-cli --list_pilots

	Pilot Compute                                           			State
	-----------------------------------------------------------------------------------------------------
	redis://localhost:6379/bigjob:bj-2ccb20f8-b18c-11e1-b5e4-109addae22a3:localhost Running
	redis://localhost:6379/bigjob:bj-a174e4a2-b18c-11e1-bc81-109addae22a3:localhost Running

3. Submit a simple command as compute unit to a pilot::

	pilot-cli --submit_cu redis://localhost:6379/bigjob:bj-2ccb20f8-b18c-11e1-b5e4-109addae22a3:localhost  /bin/date
	submit CU to redis://localhost:6379/bigjob:bj-2ccb20f8-b18c-11e1-b5e4-109addae22a3:localhost
	Started ComputeUnit: redis://localhost:6379/bigjob:bj-2ccb20f8-b18c-11e1-b5e4-109addae22a3:localhost:jobs:sj-dbb62be0-b190-11e1-b6e0-109addae22a3

4. Check the state of all compute units of a pilot::

	$ pilot-cli --list_cus redis://localhost:6379/bigjob:bj-2ccb20f8-b18c-11e1-b5e4-109addae22a3:localhost 

	Pilot Compute: redis://localhost:6379/bigjob:bj-2ccb20f8-b18c-11e1-b5e4-109addae22a3:localhost
	State: Running
	#   Compute Unit                    		State
	-------------------------------------------------------------------
	1   sj-dbb62be0-b190-11e1-b6e0-109addae22a3     Done

5. Terminate Pilot::
	
	pilot-cli --cancel_pilot redis://localhost:6379/bigjob:bj-2ccb20f8-b18c-11e1-b5e4-109addae22a3:localhost 

**Help**::

    $ pilot-cli --help
    usage: pilot-cli [-h] [--coordination COORDINATION] [--clean] [--version]
                 [--number_cores NUMBER_CORES]
                 [--cores_per_node CORES_PER_NODE]
                 [--submit_pilot RESOURCE_URL] [--cancel_pilot PILOT_URL]
                 [--list_pilots] [--wait_cus] [--list_cus PILOT_URL]
                 [--submit_cu PILOT_URL [COMMAND ARGS ...]]
                 [--run_cu PILOT_URL [COMMAND ARGS ...]]
                 [--get_cu_state CU_URL] [--cancel_cu CU_URL]

    BigJob Command Line Utility

    optional arguments:
      -h, --help            show this help message and exit
      --coordination COORDINATION, -c COORDINATION
      --clean
      --version

    Manage pilots:
      --number_cores NUMBER_CORES
      --cores_per_node CORES_PER_NODE
      --submit_pilot RESOURCE_URL
                        submit a pilot to specified resource, e.g.
                        fork://localhost
      --cancel_pilot PILOT_URL
                        Cancel pilot
      --list_pilots         list all pilots
      --wait_cus            wait for termination of all CUs
      --list_cus PILOT_URL

    Manage compute units:
      --submit_cu PILOT_URL [COMMAND ARGS ...]
                        submit CU to pilot
      --run_cu PILOT_URL [COMMAND ARGS ...]
                        submit CU to pilot and wait for completion
      --get_cu_state CU_URL
      --cancel_cu CU_URL