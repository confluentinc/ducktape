.. _topics-index:

============================================================
Distributed System Integration & Performance Testing Library
============================================================
ducktape contain tools for running system integration and performance tests. It provides utilities for pulling up and tearing down services easily, using Vagrant to let you test things on local VMs or run on EC2 nodes. Tests are just Python scripts that run a set of services, possibly triggering special events (e.g. bouncing a service), collect results (such as logs or console output) and report results (expected conditions met, performance results, etc.).

.. toctree::
   install
   run_tests
   new_tests
   new_services
   api
   misc

Contribute
==========

- Source Code: https://github.com/confluentinc/ducktape
- Issue Tracker: https://github.com/confluentinc/ducktape/issues

License
=======

The project is licensed under the Apache 2 license.
