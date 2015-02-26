System Integration & Performance Testing
========================================

This repository contains scripts for running system integraton and performance
tests. It provides utilities for pulling up and tearing down services
easily, using Vagrant to let you test things on local VMs or run on EC2
nodes. Tests are just Python scripts that run a set of services, possibly
triggering special events (e.g. bouncing a service), collect results (such as
logs or console output) and report results (expected conditions met, performance
results, etc.).

1. Use the `build.sh` script to make sure you have all the projects checked out
   and built against the specified versions.
2. Configure your Vagrant setup by creating the file `Vagrantfile.local`. At a
   minimum, you *MUST* set the value of num_workers high enough for the tests
   you're trying to run.
3. Bring up the cluster with Vagrant for testing, making sure you have enough
   workers, with `vagrant up`. If you want to run on AWS, use `vagrant up
   --provider=aws --no-parallel`.
4. Run one or more tests. Individual tests can be run directly:

        $ python -m ducttape.tests.native_vs_rest_performance

   There isn't yet a test runner to run all scripts in sequence.
5. To iterate/run again if you already initialized the repositories:

        $ build.sh --update
        $ vagrant rsync # Re-syncs build output to cluster

Writing New Tests
-----------------

"Tests" is currently a misnomer -- there's no test runner or assertion code
currently. Tests are just a series of service operations. The simplest tests
just create a number of services with the desired settings, call `run()` on each
and report the results.

Each service has a few required methods that you can call:

* start - start the service (possibly waiting to ensure it started successfully)
* wait - wait for the service to complete; only meaningful for services with a
  fixed amount of work or clear exit condition
* stop - stop the service (possibly waiting to ensure it stopped
  successfully). May also perform additional cleanup, such as deleting log
  files.
* run - call `start`, `wait`, and `stop` in sequence.

There is also a static helper method in `Test`:

* run_parallel - Call `start`, `wait`, and `stop` for each of the given
  services, allowing them to run in parallel and waiting for all of them to
  complete successfully

The `test` base class sets up logger you can use which is tagged by class name
so adding some logging for debugging or to track the progress of tests is easy:

    self.logger.debug("End-to-end latency %d: %s", idx, line.strip())

Since these types of tests are difficult to debug without sufficient logging,
you should err on the side of too much logging and make your tests report a
summary when they complete to make it easy to find the results of the test.

There are a few helper base classes in `ducttape.tests.test` in addition to the
`Test` base class that provides minimal logging support. These tests setup some
common scenarios (e.g. pull up a full Kafka cluster) so tests can focus on the
code unique to them.

Adding New Services
-------------------

"Services" refers generally to any process, possibly long-running, which you
want to run on the test cluster. These can be services you would actually deploy
(e.g., Kafka brokers, ZK servers, REST proxy) or processes used during testing
(e.g. producer/consumer performance processes). You should also make each
service class support starting a variable number of instances of the service so
test code is as concise as possible.

Each service is implemented as a class and should at least implement `start` and
`stop` methods. These may block to ensure services start or stop properly, but
must *not* block for the full lifetime of the service. If you need to run a
blocking process (e.g. run a process via SSH and iterate over its output), this
should be done in a background thread. For services that exit after completing a
fixed operation (e.g. produce N messages to topic foo), you should also
implement `wait`, which will usually just wait for background worker threads to
exit. The `Service` base class provides a helper method `run` which wraps
`start`, `wait`, and `stop` for tests that need to start a service and wait for
it to finish. You can also provide additional helper methods for common test
functionality: normal services might provide a `bounce` method.

Most of the code you'll write for a service will just be series of SSH commands
and tests of output. You should request the number of nodes you'll need using
the `num_nodes` parameter to the Service base class's constructor. Then, in your
Service's methods you'll have access to `self.nodes` to access the nodes
allocated to your service. Each node has an associated
`ducttape.cluster.RemoteAccount` instance which lets you easily perform remote
operations such as running commands via SSH or creating files. By default, these
operations try to hide output (but provide it to you if you need to extract
some subset of it) and *checks status codes for errors* so any operations that
fail cause an obvious failure of the entire test.

There is no standard interface for extracting results. It is assumed the user of
your service will know how to extract the information wherever you store it. For
example, the output of the `*PerformanceService` classes is stored in a field
called `results` with one entry per worker, where each entry is a dict
containing a set of fields based on the output of the final line of those
programs. They also maintains all the intermediate stats in the same format in a
field called `stats`. Users of these classes need to know the names of the
fields to get the information they want.

Adding New Repositories
-----------------------

If your new service requires a new code base to be included, you may also need
to modify the `build.sh` script. Ideally you just need to add a new line like

    build_maven_project "kafka-rest" "git@github.com:confluentinc/kafka-rest.git" "package"

at the bottom of the script. The generic `build_maven_project` function uses the
given directory name, git repository, and Maven action to check out and build
the code. Any code under the repository is automatically pushed to worker nodes
and made available under `/vagrant`. (You'll need to use the `"install"` action
if the repository is for a shared library since subsequent packages will need to
be able to find it in the local Maven repository. You probably also want to
update the `.gitignore` file to ignore the new subdirectory and
`vagrant/base.sh` to provide a symlink under `/opt` in addition to the code
under `/vagrant` to get Vagrant-agnostic naming.

EC2 Quickstart
--------------

* Start jump server instance (manually through the AWS management console)
 - If you haven't already, setup a keypair to use for SSH access.
 - Nothing too big, t2.small or t2.medium is easily enough since this machine is
   just a driver. t2.micro is even usable, but you do have to do builds locally
   so you might want something a bit bigger.
 - Ubuntu is recommended (only because the initial setup has been documented and
   tested below)
 - Most defaults are fine. Make sure you get an auto-assigned public IP
   (normally on for the default settings) and set IAM role ->
   ducttape-master, which gives permissions to launch/kill additional
   machines. Tagging the instance with a useful name,
   e.g. "<you>-ducttape-master" is recommended. Set the security group to
   'ducttape-insecure', which leaves the machine open on a variety of ports for
   traffic coming from other machines in the security group and enables SSH
   access from anywhere. This is less secure than a production config, but makes
   it so we generally don't have to worry about adding ports whenever we add new
   services to ducttape.
 - Once started, grab the public hostname/IP and SSH into the host using
   `ssh -i /path/to/keypair.pem ubuntu@<public.hostname.amazonaws.com`. All
   remaining steps will be run on the jump server. Highly recommended: use tmux
   so any connectivity issues don't kill your session.
 - Put a copy of the SSH key you're using (the pem file) on the jump server using scp.

* Start by making sure you're up to date and installing a few dependencies,
  getting ducttape, and building:

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
        ./build.sh --http

  Now is a good time to install any extra stuff you might need, e.g. your
  preferred text editor.

* Setup your Vagrant config by putting something like the following in
  Vagrantfile.local:

        ec2_instance_type = "..." # Pick something appropriate for your
                                  # test. Note that the default m3.medium has
                                  # a small disk.
        enable_dns = true
        num_workers = 4
        ec2_keypair_name = 'ewen'
        ec2_keypair_file = '/home/ubuntu/confluent-ewen.pem'
        ec2_security_groups = ['ducttape-insecure']
        ec2_region = 'us-west-2'
        ec2_ami = "ami-29ebb519"

  These settings work for the default AWS setup you get with Confluent's
  account (of course `num_workers`, `ec2_keypair_name` and `ec2_keypair_file`
  should all be customized for your setup).

* Make sure your access keys are configured:

        export AWS_ACCESS_KEY=`curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/ducttape-master | grep AccessKeyId | awk -F\" '{ print $4 }'`
        export AWS_SECRET_KEY=`curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/ducttape-master | grep SecretAccessKey | awk -F\" '{ print $4 }'`
        export AWS_SESSION_TOKEN=`curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/ducttape-master | grep Token | awk -F\" '{ print $4 }'`

* Start up the instances:

        vagrant up --provider=aws --no-provision && vagrant provision

* Now you should be able to run tests:

        python -m ducttape.tests.native_vs_rest_performance

* Once configured, you can just shutdown your machines until you need them
  again. To do so, use

        vagrant halt

  to stop your cluster and manually stop your jump server from the management
  console. To bring them back up, restart your jump server manually, then log in
  and run

         vagrant up --provider=aws --no-provision && vagrant provision

  to restore the cluster. You need to reprovision to fix up some permissions for
  ephemeral drives (under `/mnt`) which are lost when you halt the VMs, but it
  should be very fast since most of the provisioning work is maintained across
  reboots.
