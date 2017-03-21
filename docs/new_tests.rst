.. _topics-new_tests:

================
Create New Tests
================

Writing ducktape Tests
======================

Subclass :class:`~ducktape.tests.test.Test` and implement as many ``test`` methods as you
want. The name of each test method must start or end with ``test``,
e.g. ``test_functionality`` or ``example_test``. Typically, a test will
start a few services, collect and/or validate some data, and then finish.

If the test method finishes with no exceptions, the test is recorded as successful, otherwise it is recorded as a failure.


Here is an example of a test that just starts a Zookeeper cluster with 2 nodes, and a
Kafka cluster with 3 nodes::

    class StartServicesTest(Test):
        """Make sure we can start Kafka and Zookeeper services."""
        def __init__(self, test_context):
            super(StartServicesTest, self).__init__(test_context=test_context)
            self.zk = ZookeeperService(test_context, num_nodes=2)
            self.kafka = KafkaService(test_context, num_nodes=3, self.zk)

        def test_services_start(self):
            self.zk.start()
            self.kafka.start()

Test Parameters
===============

Use test decorators to parametrize tests, examples are provided below

.. autofunction:: ducktape.mark.parametrize
.. autofunction:: ducktape.mark.matrix
.. autofunction:: ducktape.mark.resource.cluster
.. autofunction:: ducktape.mark.ignore

Logging
=======

The :class:`~ducktape.tests.test.Test` base class sets up logger you can use which is tagged by class name,
so adding some logging for debugging or to track the progress of tests is easy::

    self.logger.debug("End-to-end latency %d: %s", idx, line.strip())

These types of tests can be difficult to debug, so err toward more rather than
less logging.

.. note:: Logs are collected a multiple log levels, and only higher log levels are displayed to the console while the test runs. Make sure you log at the appropriate level.
