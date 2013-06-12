#!/usr/bin/env python

#from distribute_setup import use_setuptools
#use_setuptools()

#from distutils.core import setup
import os
from setuptools import setup
import subprocess

try:
    import saga
except:
    print "SAGA C++ and SAGA Python Bindings not found. Using Bliss/SAGA."
    #sys.exit(1)

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
        print "Unable to run git, not modifying VERSION"
        return
    stdout = p.communicate()[0]
    if p.returncode != 0:
        print "Unable to run git, not modifying VERSION"
        return
    # we use tags like "python-ecdsa-0.5", so strip the prefix
    
    ver = stdout.strip()
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
    f = open(fn, "w")
    f.write(ver)
    f.close()
    print "set VERSION to '%s'" % ver


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
      description='P* Pilot-Job Implementation',
      author='Andre Luckow',
      author_email='aluckow@cct.lsu.edu',
      url='https://github.com/saga-project/BigJob',
      classifiers = ['Development Status :: 4 - Beta',                    
                    'Programming Language :: Python',
                    'Environment :: Console',                    
                    'Topic :: Utilities',
                    ],
      platforms = ('Unix', 'Linux', 'Mac OS'),
      packages=['bigjob', 'bigjob_dynamic', 'coordination', 'pilot', 'bigjob.job_plugin', 'pilot.api','pilot.api.compute', 'pilot.api.data', 'pilot.coordination', 
                'pilot.filemanagement', 'pilot.impl', 'pilot.scheduler', 'examples', 'api', 'bootstrap', 'cli'],
      include_package_data=True,
      # data files for easy_install
      data_files = [('', ['bigjob_agent.conf', 'bigjob_agent.conf']),  
                    ('', ['bigjob.conf', 'bigjob.conf']), 
                    ('', ['README.md', 'README.md']), 
                    ('', ['VERSION', 'VERSION'])],
      
      # data files for pip
      package_data = {'': ['*.conf']},
      install_requires=['uuid', 'threadpool', 'redis==2.2.4', 'bliss', 'google-api-python-client', 'python-hostlist',
                        'globusonline-transfer-api-client', 'boto>=2.2,<2.3', 'simplejson<2.1', 'pexpect', 'tldextract'],
      entry_points = {
        'console_scripts': [
            'test-bigjob = examples.example_local_single:main',
            'test-bigjob-dynamic = examples.example_manyjob_local:main',
            'pilot-cli = cli.pilot_cli:main'            
        ]
        }
)
