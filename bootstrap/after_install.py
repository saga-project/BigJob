import subprocess



#print "load distribute"
#subprocess.call(["curl", "-O", "http://python-distribute.org/distribute_setup.py"])    
#print "install distribute"
#subprocess.call([join(home_dir, 'bin', 'python'), 'distribute_setup.py'])

#print "load distribute"
#subprocess.call(["curl", "-O", "https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py"])    
#print "install setuptools"
#subprocess.call([join(home_dir, 'bin', 'python'), 'ez_setup.py'])
#print "install bigjob"
#subprocess.call([join(home_dir, 'bin', 'easy_install'), 'bigjob'])    

print "load distribute"
subprocess.call(["curl", "-O", "https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py"])    
print "install setuptools"
subprocess.call([join(home_dir, 'bin', 'python'), 'ez_setup.py'])
print "install bigjob"
subprocess.call([join(home_dir, 'bin', 'easy_install'), 'bigjob'])    