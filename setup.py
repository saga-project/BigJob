#!/usr/bin/env python

#from distutils.core import setup
import sys
from setuptools import setup, find_packages

try:
    import saga
except:
    print "SAGA and SAGA Python Bindings not found: Please install SAGA first (http://saga.cct.lsu.edu)."
    sys.exit(1)
    
setup(name='BigJob',
      version='0.11',
      description='SAGA-based Pilot-Job Implementation',
      author='Andre Luckow',
      author_email='aluckow@cct.lsu.edu',
      url='http://faust.cct.lsu.edu/trac/bigjob',
      classifiers = ['Development Status :: 4 - Beta',                    
                    'Programming Language :: Python',
                    'Environment :: Console',                    
                    'Topic :: Utilities',
                    ],
      platforms = ('Unix', 'Linux', 'Mac OS'),
      packages=['bigjob', 'coordination', 'examples', 'api', 'bootstrap'],
      data_files=['bigjob_agent.conf', 'bigjob_agent_launcher.sh'],
      install_requires=['redis', 'uuid', 'threadpool', 'virtualenv']
      
      
)