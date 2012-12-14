#!/bin/bash


PYTHONHOME=/home/luckow/sw

mkdir -p .bigjob/python
cp -r $PYTHONHOME .bigjob/python


MACHINE=`uname -m`
KERNEL=`uname -s`
tar -czvf bigjob-${KERNEL}-${MACHINE}.tar.gz .bigjob

rm -rf .bigjob
