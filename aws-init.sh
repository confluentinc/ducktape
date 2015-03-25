#!/bin/bash

# This script should be run once on your aws test driver machine before
# attempting to run any ducttape tests

# Install dependencies
sudo apt-get install -y maven openjdk-6-jdk build-essential \
            ruby-dev zlib1g-dev realpath

if [ -z `which vagrant` ]; then
    echo "Installing vagrant..."
    wget https://dl.bintray.com/mitchellh/vagrant/vagrant_1.7.2_x86_64.deb
    sudo dpkg -i vagrant_1.7.2_x86_64.deb
    rm -f vagrant_1.7.2_x86_64.deb
fi

# Install necessary vagrant plugins
# Note: Do NOT install vagrant-cachier since it doesn't work on AWS and only
# adds log noise
vagrant_plugins="vagrant-aws vagrant-hostmanager"
existing=`vagrant plugin list`
for plugin in $vagrant_plugins; do
    echo $existing | grep $plugin > /dev/null
    if [ $? != 0 ]; then
        vagrant plugin install $plugin
    fi
done

# Create Vagrantfile.local as a convenience
if [ ! -e "Vagrantfile.local" ]; then
    cp aws-example-Vagrantfile.local Vagrantfile.local
fi

if [ -z `which gradle` ]; then
    gradle="gradle-2.2.1"
    if [ ! -e $gradle-bin.zip ]; then
        wget https://services.gradle.org/distributions/$gradle-bin.zip
        rm -rf $gradle-bin.zip
    fi
    unzip $gradle-bin.zip
fi

# Ensure aws access keys are in the environment
grep "AWS ACCESS KEYS" ~/.bashrc > /dev/null
if [ $? != 0 ]; then
  echo "# --- AWS ACCESS KEYS ---" >> ~/.bashrc
  echo ". `realpath aws-access-keys-commands`" >> ~/.bashrc
  echo "# -----------------------" >> ~/.bashrc
  source ~/.bashrc
fi

./build.sh --aws
