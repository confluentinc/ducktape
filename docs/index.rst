.. _topics-index:

============================================================
Distributed System Integration & Performance Testing Library
============================================================
Ducktape contains tools for running system integration and performance tests. It provides the following features:

   * Write tests for distributed systems in a simple unit test-like style
   * Isolation by default so system tests are as reliable as possible.
   * Utilities for pulling up and tearing down services easily in clusters in different environments (e.g. local, custom cluster, Vagrant, K8s, Mesos, Docker, cloud providers, etc.)
   * Trigger special events (e.g. bouncing a service)
   * Collect results (e.g. logs, console output)
   * Report results (e.g. expected conditions met, performance results, etc.)

.. toctree::
   install
   test_clusters
   run_tests
   new_tests
   new_services
   debug_tests
   api
   misc
   changelog

Contribute
==========

- Source Code: https://github.com/confluentinc/ducktape
- Issue Tracker: https://github.com/confluentinc/ducktape/issues

License
=======

The project is licensed under the Apache 2 license.
