System Integration & Performance Testing
========================================

This repository contains scripts for running system integraton and performance
tests. It provides a bunch of utilities for pulling up and tearing down services
easily, using Vagrant to let you test things on local VMs or run on EC2 nodes.

1. Use the `build.sh` script to make sure you have all the projects checked out
   and built against the specified versions.
2. Bring up the cluster with Vagrant for testing, making sure you have enough
   workers, with `vagrant up`.
3. Run one or more tests. Individual tests can be run directly:

    $ python -m ducttape.tests.native_vs_rest_performance

4. To iterate/run again if you already initialized the repositories:

    $ build.sh --update
    $ vagrant rsync # Re-syncs build output to cluster
