#!/usr/bin/env python

from distutils.core import setup
import sys

try:
    import saga
except:
    print "SAGA and SAGA Python Bindings not found: Please install SAGA first (http://saga.cct.lsu.edu)."
    sys.exit(1)
    
setup(name='BigJob',
      version='0.1',
      description='SAGA-based Pilot-Job Implementation',
      author='Andre Luckow',
      author_email='aluckow@cct.lsu.edu',
      url='http://faust.cct.lsu.edu/trac/bigjob',
      classifiers = ['Development Status :: 4 - Beta',                    
                    'Programming Language :: Python',
                    'Intended Audience :: Developer',
                    'Environment :: Console',                    
                    'Topic :: Utilities',
                    ],
      platforms = ('Unix', 'Linux', 'Mac OS'),
      packages=['bigjob', 'coordination', 'examples', 'api'],
      data_files=['bigjob_agent.conf', 'bigjob_agent_launcher.sh'],
      requires=['redis', 'uuid', 'threadpool']
)