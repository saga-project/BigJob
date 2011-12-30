#!/usr/bin/env python

#from distutils.core import setup
import sys
import bigjob
from setuptools import setup, find_packages

try:
    import saga
except:
    print "SAGA and SAGA Python Bindings not found: Please install SAGA first (http://saga.cct.lsu.edu)."
    #sys.exit(1)
    
setup(name='BigJob',
      version=bigjob.version,
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
      packages=['bigjob', 'bigjob_dynamic', 'coordination', 'examples', 'api', 'bootstrap'],
      data_files=['bigjob_agent.conf', "VERSION", "README"],
      install_requires=['uuid', 'threadpool', 'virtualenv', 'redis==2.2.4', 'paramiko'],
      entry_points = {
        'console_scripts': [
            'test-bigjob = examples.example_local_single:main',
            'test-bigjob-dynamic = examples.example_manyjob_local:main'            
        ]
        }
)
