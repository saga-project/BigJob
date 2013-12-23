#!/bin/bash
# Experimental new bootstrap script used by BigJob YARN adaptor at the moment
env
whoami

##### Configuration
# PUT IN PATH OF DIRECTOR THAT script should check for an
# existing BigJob installation
CHECK_BIGJOB_DEFAULT_DIR=/Users/luckow/.bigjob/python

# IF not in system-level configuration BigJob will be installed into relative 
# to working directory of BigJob agent.
BIGJOB_RELATIVE_DIRECTORY=.bigjob/python

function run_bigjob {
    BIGJOB_DIRECTORY=$1
    echo "Activate BigJob environment: $BIGJOB_DIRECTORY"
    ACTIVATE_VIRTUALENV_CMD=". $BIGJOB_DIRECTORY/bin/activate"
    echo $ACTIVATE_VIRTUALENV_CMD
    $ACTIVATE_VIRTUALENV_CMD
    OUT=$?
    if [ $OUT -ne 0 ];then
        echo "Couldn't activate BigJob virtualenv. Exiting"
        exit 1
    fi

    echo "Start BigJob Agent - Version: "
    python -c "import bigjob; print bigjob.version"
    START_BIGJOB_AGENT_CMD="python -m bigjob.bigjob_agent  $2 $3 $4"
    echo $START_BIGJOB_AGENT_CMD
    $START_BIGJOB_AGENT_CMD
    OUT=$?
    if [ $OUT -ne 0 ];then
        echo "Couldn't start BigJob. Exiting"
        exit 1
    fi
}


echo "Check for existing BigJob installation in: $CHECK_BIGJOB_DEFAULT_DIR" 
if [ -d $CHECK_BIGJOB_DEFAULT_DIR ] 
then
    echo "Using existing BigJob"
    run_bigjob $CHECK_BIGJOB_DEFAULT_DIR $*
else
    mkdir -p $BIGJOB_RELATIVE_DIRECTORY
    BOOTSTRAP_DOWNLOAD_CMD="curl -OL --insecure -s https://raw.github.com/saga-project/BigJob/develop/bootstrap/bigjob2-bootstrap.py"
    echo "## Downloading BigJob2"
    echo $BOOTSTRAP_DOWNLOAD_CMD
    $BOOTSTRAP_DOWNLOAD_CMD
    OUT=$?
    if [ $OUT -ne 0 ];then
        echo "Couldn't download BigJob. Exiting"
        exit 1
    fi
    echo "## Install BigJob2"    
    INSTALL_CMD="python bigjob2-bootstrap.py $BIGJOB_RELATIVE_DIRECTORY"
    echo "Command: "$INSTALL_CMD
    $INSTALL_CMD
    OUT=$?
    if [ $OUT -ne 0 ];then
        echo "Couldn't install BigJob. Exiting"
        exit 1
    fi
    run_bigjob $BIGJOB_RELATIVE_DIRECTORY $*
fi

