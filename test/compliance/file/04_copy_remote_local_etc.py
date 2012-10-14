# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

import sys, time, uuid
import getpass
import bigjob.saga as saga

def run(local_base_url, remote_file_to_copy):
    """Test if we can lists a (remote) directory
    """
    try:
        failed = False
        tmpdirname = "sagaproj-%s" % uuid.uuid1()

        local_tdir = saga.filesystem.Directory('%s' % local_base_url)
        local_tdir.make_dir(tmpdirname)
        print "Size: %s" %str(local_tdir.get_size())

        myremotefile = saga.filesystem.File("%s" % remote_file_to_copy) 
        print "Size: %s" %str(myremotefile.get_size())
        
        myremotefile.copy("%s/%s/" % (local_base_url, tmpdirname))
        myremotefile.copy("%s/%s/bh-copy" % (local_base_url, tmpdirname))

        local_tdir = saga.filesystem.Directory("%s/%s/" % (local_base_url, tmpdirname)) 
        local_tdir.make_dir("A")
        local_tdir.make_dir("B")

        print local_tdir.list()       
        local_tdir.remove("A")
        local_tdir.remove("B")      

        print local_tdir.list()        
        local_tdir.remove()

        local_tdir.close()

    except saga.Exception, ex:
        failed = True
        why = str(ex)

    if failed == True:
       print ""
       print "============================================"
       print "File / directory tests seems to have FAILED!"
       print "============================================"
       print "                                            "
       print "%s" % (why)
       print "Please run this test again with SAGA_VERBOSE=5 "
       print "and report the results at: "
       print ""
       print "https://github.com/saga-project/bliss/issues\n"

    else: 
      print ""
      print "============================================"
      print "File / directory tests have passed!"
      print "============================================"
      print "                                            "
   
    return failed

def usage():
    print 'Usage: python %s ' % __file__
    print '                <LOCALDIR> (e.g., sftp:///tmp/'
    print '                <REMOTEFILE> (e.g., sftp://oweidner@qb.loni.org/etc/passwd)'

def main():
    remoteusername = getpass.getuser()

    args = sys.argv[1:]
    if len(args) != 2:
        usage()
        sys.exit(-1)
    else:
        local_base_url = args[0]
        remote_file_to_copy = args[1]

    return run(local_base_url, remote_file_to_copy)

if __name__ == '__main__':
    sys.exit(main())

