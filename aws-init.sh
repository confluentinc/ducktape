#!/bin/bash -e

sudo apt-get update && sudo apt-get -y upgrade && \
            sudo apt-get install git maven openjdk-6-jdk build-essential \
            ruby-dev zlib1g-dev
wget https://dl.bintray.com/mitchellh/vagrant/vagrant_1.7.2_x86_64.deb
sudo dpkg -i vagrant_1.7.2_x86_64.deb
vagrant plugin install vagrant-hostmanager
# Do NOT install vagrant-cachier since it doesn't work on AWS and only
# adds log noise
vagrant plugin install vagrant-aws
git clone https://github.com/confluentinc/ducttape.git
cd ducttape
wget https://services.gradle.org/distributions/gradle-2.2.1-bin.zip && \
    unzip gradle-2.2.1-bin.zip
./build.sh --aws
