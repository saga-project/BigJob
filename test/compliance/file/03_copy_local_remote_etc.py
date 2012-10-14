# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

import sys, time, uuid
import getpass
import bigjob.saga as saga

def run(remote_base_url, local_file_to_copy):
    """Test if we can lists a (remote) directory
    """
    try:
        failed = False
        tmpdirname = "sagaproj-%s" % uuid.uuid1()

        remote_tdir = saga.filesystem.Directory(remote_base_url)
        remote_tdir.make_dir(tmpdirname)
        print "Size: %s" %str(remote_tdir.get_size())

        mylocalfile = saga.filesystem.File("sftp://localhost/%s" % local_file_to_copy)
        print "File Size: %s" %str(mylocalfile.get_size())

        mylocalfile.copy("%s/%s/" % (remote_base_url, tmpdirname))
        mylocalfile.copy("%s/%s/bh-copy" % (remote_base_url, tmpdirname))

        remote_tdir = saga.filesystem.Directory("%s/%s/" % (remote_base_url, tmpdirname)) 
        remote_tdir.make_dir("A")
        remote_tdir.make_dir("B")

        print remote_tdir.list()    
        print "Size: %s" %str(remote_tdir.get_size())   
        remote_tdir.remove("A")
        remote_tdir.remove("B")      

        print remote_tdir.list()        
        print "Size: %s" %str(remote_tdir.get_size())
        remote_tdir.remove()

        remote_tdir.close()

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
    print '                <REMOTEURL> (e.g., sftp://oweidner@qb.loni.org)'
    print '                <LOCAL_FILE_TO_COPY>'

def main():
    remoteusername = getpass.getuser()

    args = sys.argv[1:]
    if len(args) != 2:
        usage()
        sys.exit(-1)
    else:
        remoteurl = args[0]
        local_file_to_copy = args[1]

    return run(remoteurl, local_file_to_copy)

if __name__ == '__main__':
    sys.exit(main())

