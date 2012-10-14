# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

import sys
import getpass
import bigjob.saga as saga

def run(url, username):
    """Test if we can lists a (remote) directory
    """
    try:
        failed = False
        ctx = saga.Context()
        ctx.context_type = saga.Context.SSH
        ctx.user_id  = username # like 'ssh username@host ...'
      
        session = saga.Session()
        session.contexts.append(ctx)

        mydir = saga.filesystem.Directory(url, session=session)
        for entry in mydir.list():
            print entry

    except saga.Exception, ex:
        failed = True
        why = str(ex)

    if failed == True:
       print ""
       print "============================================"
       print "Directory list seems to have FAILED!"
       print "============================================"
       print "                                            "
       print "%s" % (why)
       print "Please run this test again with SAGA_VERBOSE=5 "
       print "and report the results at: "
       print ""
       print "https://github.com/saga-project/bliss/issues\n"
       return 255

    else: 
      print ""
      print "============================================"
      print "The job seems to have executed successfully!"
      print "============================================"
      print "                                            "
      return 0

def usage():
    print 'Usage: python %s ' % __file__
    print '                <URL>'
    print '                <REMOTEUSERNAME (default: local username)>'

def main():
    remoteusername = getpass.getuser()

    args = sys.argv[1:]
    if len(args) < 1:
        usage()
        sys.exit(-1)
    else:
        js_url = args[0]

    try:
        remoteusername = args[1]
    except IndexError:
        pass 

    return run(js_url, remoteusername)

if __name__ == '__main__':
    sys.exit(main())
