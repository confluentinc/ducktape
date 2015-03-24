#!/bin/bash -e

# This script should be run once on your aws test driver machine before
# attempting to run any ducttape tests

# Install dependencies
sudo apt-get install maven openjdk-6-jdk build-essential \
            ruby-dev zlib1g-dev realpath
wget https://dl.bintray.com/mitchellh/vagrant/vagrant_1.7.2_x86_64.deb
sudo dpkg -i vagrant_1.7.2_x86_64.deb
rm -f vagrant_1.7.2_x86_64.deb
vagrant plugin install vagrant-hostmanager
# Do NOT install vagrant-cachier since it doesn't work on AWS and only
# adds log noise
vagrant plugin install vagrant-aws
wget https://services.gradle.org/distributions/gradle-2.2.1-bin.zip && \
    unzip gradle-2.2.1-bin.zip

# Create Vagrantfile.local as a convenience
cp aws-example-Vagrantfile.local Vagrantfile.local

# Ensure aws access keys are in the environment
grep "AWS ACCESS KEYS" ~/.bashrc
if [ $? != 0 ]; then
  echo "# --- AWS ACCESS KEYS ---" >> ~/.bashrc
  echo ". `realpath aws-access-keys-commands`" >> ~/.bashrc
  echo "# -----------------------" >> ~/.bashrc
  source ~/.bashrc
fi

./build.sh --aws
