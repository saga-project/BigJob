# YARN JOB Plugin

## Requirements:

* local Hadoop 2.2.0 installation

## BigJob YARN Application (binary within plugin directory)
This plugin depends on the BigJob YARN Application Master found here:

<https://github.com/drelu/BigJob-YARN>

A build of the BigJob YARN application is distributed with BigJob.

BIGJOB_SOURCE_DIRECTORY/bigjob/job_plugin/yarn/BigJob-YARN-0.1-SNAPSHOT-jar-with-dependencies.jar

## Configuration

Before using BigJob with YARN please go through the settings within the adaptor:

""" HADOOP/JAVA Configuration"""
JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.7.0_40.jdk/Contents/Home"
YARN_HOME="/usr/local/hadoop-2.2.0/"

JAVA_HOME and YARN_HOME need to be adjusted to your environment.

Reinstall BigJob from BigJob source directory:

    python setup.py install