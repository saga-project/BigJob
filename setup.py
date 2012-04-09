#!/usr/bin/env python

#from distutils.core import setup
import os
from setuptools import setup

try:
    import saga
except:
    print "SAGA and SAGA Python Bindings not found: Please install SAGA first (http://saga.cct.lsu.edu)."
    #sys.exit(1)
    
fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
version = open(fn).read().strip()
    
setup(name='BigJob',
      version=version,
      description='SAGA-based Pilot-Job Implementation',
      author='Andre Luckow',
      author_email='aluckow@cct.lsu.edu',
      url='https://github.com/saga-project/BigJob',
      classifiers = ['Development Status :: 4 - Beta',                    
                    'Programming Language :: Python',
                    'Environment :: Console',                    
                    'Topic :: Utilities',
                    ],
      platforms = ('Unix', 'Linux', 'Mac OS'),
      packages=['bigjob', 'bigjob_dynamic', 'coordination', 'pilot', 'pilot.api','pilot.api.compute', 'pilot.api.data', 'pilot.coordination', 
                'pilot.filemanagement', 'pilot.impl', 'pilot.scheduler', 'examples', 'api', 'bootstrap'],
      #data_files=['bigjob_agent.conf', 
      #            'bigjob.conf', "VERSION", "README.md"],
      data_files = [( '', ['bigjob_agent.conf', 'bigjob_agent.conf']),  
                    ('', ['bigjob.conf', 'bigjob.conf']), 
                    ('', ['README.md', 'README.md']), 
                    ('', ['VERSION', 'VERSION'])],
#      package_data = {
#        '': ['bigjob_agent.conf', 'bigjob_agent.conf'],
#        '': ['bigjob.conf', 'bigjob.conf'],
#        '': ['README', 'README'],
#        '': ['VERSION', 'VERSION']
#      },
      install_requires=['paramiko-on-pypi', 'uuid', 'threadpool', 'virtualenv', 'redis==2.2.4', 'bliss'],
      entry_points = {
        'console_scripts': [
            'test-bigjob = examples.example_local_single:main',
            'test-bigjob-dynamic = examples.example_manyjob_local:main'            
        ]
        }
)
