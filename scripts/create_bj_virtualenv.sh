#!/bin/bash

DIR=`pwd`
BIGJOB_DIR=".bigjob/python"
python ../bootstrap/bigjob-bootstrap.py ${DIR}/${BIGJOB_DIR}
python ../bootstrap/bigjob-bootstrap.py --relocatable ${DIR}/${BIGJOB_DIR}
. ${DIR}/${BIGJOB_DIR}/bin/activate
easy_install -U bigjob

MACHINE=`uname -m`
KERNEL=`uname -s`
tar -czvf bigjob-${KERNEL}-${MACHINE}.tar.gz .bigjob

rm -rf ${BIGJOB_DIR}
deactivate

