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

New test example
================

Lets expand on the StartServicesTest example. The test starts a Zookeeper cluster with 2 nodes, and a
Kafka cluster with 3 nodes, and then bounces a kafka broker node which is either a special controller node or a non-controller node, depending on the `bounce_controller_broker` test parameter.

.. code-block:: python

    class StartServicesTest(Test):
        def __init__(self, test_context):
            super(StartServicesTest, self).__init__(test_context=test_context)
            self.zk = ZookeeperService(test_context, num_nodes=2)
            self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk)

        def setUp(self):
            self.zk.start()
            self.kafka.start()

        @matrix(bounce_controller_broker=[True, False])
        def test_broker_bounce(self, bounce_controller_broker=False):
            controller_node = self.kafka.controller()
            self.logger.debug("Found controller broker %s", controller_node.account)
            if bounce_controller_broker:
                bounce_node = controller_node
            else:
                bounce_node = self.kafka.nodes[(self.kafka.idx(controller_node) + 1) % self.kafka.num_nodes]

            self.logger.debug("Will hard kill broker %s", bounce_node.account)
            self.kafka.signal_node(bounce_node, sig=signal.SIGKILL)

            wait_until(lambda: not self.kafka.is_registered(bounce_node),
                       timeout_sec=self.kafka.zk_session_timeout + 5,
                       err_msg="Failed to see timely deregistration of hard-killed broker %s"
                               % bounce_node.account)

            self.kafka.start_node(bounce_node)

This will run two tests, one with ‘bounce_controller_broker’: False and another with 'bounce_controller_broker': True arguments. We moved start of Zookeeper and Kafka services to :meth:`~ducktape.tests.test.Test.setUp`, which is called before every test run.

The test finds which of Kafka broker nodes is a special controller node via provided ``controller`` method in KafkaService. The ``controller`` method in KafkaService with raise an exception if the controller node is not found. Make sure to check the behavior of methods provided by a service or other helper classes and fail the test as soon as an issue is found. That way, it will be much easier to find the cause of the test failure.

The test then finds the node to bounce based on `bounce_controller_broker` test parameter and then forcefully terminates the service process on that node via ``signal_node`` method of KafkaService. This method just sends a signal to forcefully kill the process, and does not do any further check. Thus, our test needs to check that the hard killed kafka broker is not part of the Kafka cluster anymore, before restarting the killed broker process. We do this by waiting on ``is_registered`` method provided by KafkaService to return False with a timeout, since de-registering the broker may take some time. Notice the use of ``wait_until`` method instead of a check after ``time.sleep``. This allows the test to continue as soon as de-registration happens.

We don’t check if the restarted broker is registered, because this is already done in KafkaService  ``start_node`` implementation, which will raise an exception if the service is not started successfully on a given node.
