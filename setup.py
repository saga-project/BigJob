#!/usr/bin/env python

import os
import sys

#import ez_setup
#ez_setup.use_setuptools()

from setuptools import setup, find_packages

import subprocess

try:
    import saga
except:
    print "Installing BigJob and SAGA/Python."

if sys.version_info < (2, 6):
    sys.stderr.write("BigJob requires Python 2.6 and above. Installation unsuccessful!")
    sys.exit(1)

VERSION_FILE="VERSION"    
    

def update_version():
    if not os.path.isdir(".git"):
        print "This does not appear to be a Git repository."
        return
    try:
        p = subprocess.Popen(["git", "describe",
                              "--tags", "--always"],
                             stdout=subprocess.PIPE)
    except EnvironmentError:
        print "Warning: Unable to run git, not modifying VERSION"
        return
    stdout = p.communicate()[0]
    if p.returncode != 0:
        print "Warning: Unable to run git, not modifying VERSION"
        return
    
    ver = stdout.strip()
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
    f = open(fn, "w")
    f.write(ver)
    f.close()
    print "BigJob VERSION: '%s'" % ver


def get_version():
    try:
        fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
        f = open(fn)
        version = f.read().strip()
        f.close()
    except EnvironmentError:
        return "-1"
    return version    

    
update_version()
    
setup(name='BigJob',
      version=get_version(),
      description='P* Pilot-Job Implementation based on SAGA-Python',
      author='Andre Luckow, et al.',
      author_email='aluckow@cct.lsu.edu',
      url='https://github.com/saga-project/BigJob',
      classifiers = ['Development Status :: 5 - Production/Stable',                  
                    'Programming Language :: Python',
                    'Environment :: Console',                    
                    'Topic :: Utilities',
                    ],
      platforms = ('Unix', 'Linux', 'Mac OS'),
      packages=['bigjob', 'bigjob_dynamic', 'coordination', 'pilot', 'bigjob.job_plugin', 'pilot.api','pilot.api.compute', 'pilot.api.data', 'pilot.coordination', 
                'pilot.filemanagement', 'pilot.impl', 'pilot.scheduler', 'examples', 'api', 'bootstrap', 'cli'],
      include_package_data=True,
      # data files for easy_install
      data_files = [('', ['bigjob.conf', 'bigjob.conf']), 
                    ('', ['bigjob_agent.conf', 'bigjob_agent.conf']), 
                    ('', ['README.md', 'README.md']), 
                    ('', ['VERSION', 'VERSION'])],
      
      # data files for pip
      package_data = {'': ["ez_setup.py", '*.conf']},

      install_requires=['setuptools>2.0', 'uuid', 'threadpool', 'virtualenv', 'redis', 
                        'radical.utils', 'saga-python', 'google-api-python-client', 'python-hostlist',
                        'globusonline-transfer-api-client', 'boto>=2.2,<2.3', 'simplejson<2.1', 'pexpect', 'tldextract'],
      entry_points = {
        'console_scripts': [
            'test-bigjob = examples.example_local_single:main',
            'test-bigjob-dynamic = examples.example_manyjob_local:main',
            'pilot-cli = cli.pilot_cli:main'            
        ]
            }
)
