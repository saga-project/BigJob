
__author__    = "Andre Luckow, Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2013, RADICAL Research, Rutgers University"
__license__   = "MIT"


""" Setup script. Used by easy_install and pip. """

import os
import sys
import subprocess

from setuptools              import setup, Command
from distutils.command.sdist import sdist


#-----------------------------------------------------------------------------
#
# versioning mechanism:
#
#   - short_version:  1.2.3 - is used for installation
#   - long_version:  v1.2.3-9-g0684b06  - is used as runtime (ru.version)
#   - both are derived from the last git tag
#   - the file bigjob/VERSION is created with the long_version, und used
#     by ru.__init__.py to provide the runtime version information. 
#
def get_version():

    short_version = None  # 0.4.0
    long_version  = None  # 0.4.0-9-g0684b06

    try:
        import subprocess as sp
        import re

        srcroot       = os.path.dirname (os.path.abspath (__file__))
        VERSION_MATCH = re.compile (r'(([\d\.]+)\D.*)')

        # attempt to get version information from git
        p   = sp.Popen ('cd %s && git describe --tags --always' % srcroot,
                        stdout=sp.PIPE, stderr=sp.STDOUT, shell=True)
        out = p.communicate()[0]


        if  p.returncode != 0 or not out :

            # the git check failed -- its likely that we are called from
            # a tarball, so use ./VERSION instead
            out=open ("%s/VERSION" % srcroot, 'r').read().strip()


        # from the full string, extract short and long versions
        v = VERSION_MATCH.search (out)
        if v:
            long_version  = v.groups ()[0]
            short_version = v.groups ()[1]


        # sanity check if we got *something*
        if  not short_version or not long_version :
            sys.stderr.write ("Cannot determine version from git or ./VERSION\n")
            import sys
            sys.exit (-1)


        # make sure the version files exist for the runtime version inspection
        open (       '%s/VERSION' % srcroot, 'w').write (long_version+"\n")
        open ('%s/bigjob/VERSION' % srcroot, 'w').write (long_version+"\n")


    except Exception as e :
        print 'Could not extract/set version: %s' % e
        import sys
        sys.exit (-1)

    return short_version, long_version


short_version, long_version = get_version ()
    
#-----------------------------------------------------------------------------
# check python version. we need > 2.6, <3.x
if  sys.hexversion < 0x02060000 or sys.hexversion >= 0x03000000:
    raise RuntimeError("SAGA requires Python 2.x (2.6 or higher)")


#-----------------------------------------------------------------------------
class our_test(Command):
    user_options = []
    def initialize_options (self) : pass
    def finalize_options   (self) : pass
    def run (self) :
        testdir = "%s/tests/" % os.path.dirname(os.path.realpath(__file__))
        retval  = subprocess.call([sys.executable, 
                                   '%s/test_pty_exhaustion.py' % testdir])
        raise SystemExit(retval)

    
#-----------------------------------------------------------------------------
setup_args = {
    'name'             : "BigJob",
    'version'          : short_version,
    'description'      : "P* Pilot-Job Implementation based on SAGA-Python",
    'long_description' : "P* Pilot-Job Implementation based on SAGA-Python",
    'author'           : "Andre Luckow, et al.",
    'author_email'     : "aluckow@cct.lsu.edu",
    'maintainer'       : "Andre Luckow",
    'maintainer_email' : "aluckow@cct.lsu.edu",
    'url'              : "https://github.com/saga-project/BigJob",
    'license'          : "MIT",
    'classifiers'      : [
        'Development Status   :: 5 - Production/Stable',                  
        'Intended Audience    :: Developers',
        'Environment          :: Console',                    
        'License              :: OSI Approved :: MIT',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic                :: Utilities',
        'Topic                :: System :: Distributed Computing',
        'Topic                :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Operating System     :: MacOS :: MacOS X',
        'Operating System     :: POSIX',
        'Operating System     :: Unix',
        ],
    'packages' : [
        "bigjob", 
        "bigjob_dynamic", 
        "coordination", 
        "pilot", 
        "bigjob.job_plugin", 
        "pilot.api", 
        "pilot.api.compute", 
        "pilot.api.data", 
        "pilot.coordination", 
        "pilot.filemanagement", 
        "pilot.impl", 
        "pilot.scheduler", 
        "examples", 
        "api", 
        "bootstrap", 
        "cli", 
    ],
    'zip_safe'             : False,
    'scripts'              : [],
    'package_data'         :  {'' : ['VERSION', '*.conf', '*.md']},
    'data_files'           : [('',  ['bigjob.conf',       'bigjob.conf']), 
                              ('',  ['bigjob_agent.conf', 'bigjob_agent.conf']), 
                              ('',  ['README.md',         'README.md']), 
                              ('',  ['VERSION',           'VERSION'])],
    'cmdclass'             : {
        'test'         : our_test,
    },
    'install_requires' : ['uuid', 'threadpool', 'virtualenv', 
                          'redis', 'radical.utils', 'saga-python', 'pexpect', 
                          'google-api-python-client', 'python-hostlist',
                          'globusonline-transfer-api-client', 'boto>=2.2,<2.3', 
                          'simplejson<2.1', 'tldextract'],
}

#-----------------------------------------------------------------------------

setup (**setup_args)

#-----------------------------------------------------------------------------

