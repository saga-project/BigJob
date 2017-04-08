# Install notes for Mesos on Cent OS 6.5

yum groupinstall "Development tools"
yum install java-1.6.0-openjdk.x86_64  java-1.6.0-openjdk-devel.x86_64 python python-devel libcurl libcurl-devel zlib devel cyrus-sasl-devel readline-devel sqlite-devel zlib-devel openssl-devel

wget http://mirror-fpt-telecom.fpt.net/fedora/epel/6/i386/epel-release-6-8.noarch.rpm
rpm -ivh epel-release-6-8.noarch.rpm
yum update
yum install python-pip

./configure --prefix=
make 
make install

Go to src/python

python setup.py install

# Use BigJob with Mesos

service_url: mesos://mesos_master:port, e.g. mesos://localhost:5050

TODO:
- manage memory and core usage according to Mesos offers
- scale-up and scale-down number of agents