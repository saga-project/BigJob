#!/bin/bash


PYTHONHOME=/home/luckow/sw/*

mkdir -p .bigjob/python/
cp -r $PYTHONHOME .bigjob/python/


MACHINE=`uname -m`
KERNEL=`uname -s`
tar -czvf bigjob-${KERNEL}-${MACHINE}.tar.gz .bigjob

s3cmd put bigjob-Linux-x86_64.tar.gz s3://bigjob/
s3cmd setacl --acl-public -r s3://bigjob/

rm -rf .bigjob
