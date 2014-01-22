#!/bin/bash
sudo yum -y install git
git config --global user.name "Andre Luckow"
git config --global user.email andre.luckow@gmail.com
alias vi=vim

sudo mkdir /data

sudo ln -sf /usr/bin/python2.7 /usr/bin/python
sudo easy_install bigjob2
