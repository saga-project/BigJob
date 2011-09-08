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

export PYTHONPATH=`dirname $0`:$PYTHONPATH
python -m bigjob.bigjob_agent $*

