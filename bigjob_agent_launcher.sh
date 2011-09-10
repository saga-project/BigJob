#!/bin/bash
#
# This is a wrapper script for the bigjob agent. In this script all necessary paths to applications and
# required libraries (e.g. MPI) should be set to ensure that the sub-processes launched by the 
# agent will find these libraries 

. ~/.bashrc

#soft add +mpichvmi-intel
#soft add +mpichvmi-intel-ofed1.2
#echo "Run bigjob agent: " `dirname $0`/bigjob_agent.py $*
#echo "MPI Path" `which mpirun`

# debugging output
#env
export SAGA_VERBOSE=100

# make sure that Grid certificate is available on all nodes
#grid-proxy-info
#scp $PBS_O_HOST:/tmp/x509up_u`id -u` /tmp/

# run the bigjob_agent
# python `dirname $0`/bigjob_agent_generic.py $*


# bootstrap bigjob
# check whether SAGA, SAGA Python and BigJob is available on the respective machine
python -c "import saga"
if [ $? -ne 0 ]
then
        echo "SAGA / SAGA Python not found"
        exit 1
fi

BIGJOB_INSTALL_DIR=$HOME/.bigjob/python
BOOTSTRAP_URL=http://localhost/~luckow/bigjob-bootstrap.py
python -c "import bigjob.bigjob_manager"
if [ $? -ne 0 ]
then
        echo "BigJob not found. Attempting to install it to ~/.bigjob/python"
        curl $BOOTSTRAP_URL -o bigjob-bootstrap.py
        echo "Installing to ${BIGJOB_INSTALL_DIR}"
        rm -rf $BIGJOB_INSTALL_DIR
        python bigjob-bootstrap.py ${BIGJOB_INSTALL_DIR}
        echo "Activate virtualenv"
        . $BIGJOB_INSTALL_DIR/bin/activate
fi

export PYTHONPATH=`dirname $0`:$PYTHONPATH
python -m bigjob.bigjob_agent $*