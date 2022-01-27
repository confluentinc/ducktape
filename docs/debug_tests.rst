.. _topics-debug_tests:

===========
Debug Tests
===========

The test results go in ``results/<date>—<test_number>``. For results from a particular test, look for ``results/<date>—<test_number>/test_class_name/<test_method_name>/`` directory. The ``test_log.debug`` file will contain the log output from the python driver, and logs of services used in the test will be in ``service_name/node_name`` sub-directory.

If there is not enough information in the logs, you can re-run the test with ``--no-teardown`` argument.

.. code-block:: bash

    ducktape dir/tests/my_test.py::TestA.test_a --no-teardown


This will run the test but will not kill any running processes or remove log files when the test finishes running. Then, you can examine the state of a running service or the machine when the service process is running by logging into that machine. Suppose you suspect a particular service being the cause of the test failure. You can find out which machine was allocated to that service by either looking at ``test_log.debug`` or at directory names under ``results/<date>—<test_number>/test_class_name/<test_method_name>/service_name/``. It could be useful to add an explicit debug log to ``start_node`` method with a node ID and node’s hostname information for easy debugging:

.. code-block:: python

    def start_node(self, node):
        idx = self.idx(node)
        self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)

The log statement will look something like this::

    [INFO  - 2017-03-28 22:07:25,222 - zookeeper - start_node - lineno:50]: Starting ZK node 1 on worker1

If you are using Vagrant for example, you can then log into that node via:

.. code-block:: bash

    vagrant ssh worker1



Use Logging
===========

Distributed system tests can be difficult to debug. You want to add a lot of logging for debugging and tracking progress of the test. A good approach would be to log an intention of an operation with some useful information before any operation that can fail. It could be a good idea to use a higher logging level than you would in production so more info is available. For example, make your log levels default to DEBUG instead of INFO. Also, put enough information to a message of ``assert`` to help figure out what went wrong as well as log messages. Consider an example of testing ElasticSearch service:

.. code-block:: python

        res = es.search(index="test-index", body={"query": {"match_all": {}}})
        self.logger.debug("result: %s" % res['hits'])
        assert res['hits']['total'] == 1, "Expected total 1 hit, but got %d" % res['hits']['total']
        for hit in res['hits']['hits']:
            assert 'kimchy’ == hit['_source']['author’], "Expected author kimchy but got %s" % hit['_source']['author']
            assert 'Elasticsearch: cool.' == hit['_source']['text’], "Expected text Elasticsearch: cool. but got %s" % hit['_source']['text’]

First, the tests outputs the result of a search, so that if any of the following assertions fail, we can see the whole result in ``test_log.debug``. Assertion messages help to quickly see the difference in expected and retrieved results. 


Fail early
==========

Try to avoid a situation where a test fails because of an uncaught failure earlier in the test. Suppose we write a ``start_node`` method that does not check if the service starts successfully. The service fails to start, but we get a test failure indication that there was a problem querying the service. It would be much faster to debug the issue if the test failure pointed to the issue with starting the service. So make sure to add checks for operations that may fail, and fail the test earlier than later.


Flaky tests
============

Flaky tests are hard to debug due to their non-determinism, they waste time, and sometimes hide real bugs: developers tend to ignore those failures, and thus could miss real bugs. Flakiness can come from the test itself, the system it is testing, or the environmental issues.

Waiting on Conditions
^^^^^^^^^^^^^^^^^^^^^

A common cause of a flaky test is asynchronous wait on conditions. A test makes an asynchronous call and does not properly wait for the result of the call to become available before using it::

	node.account.kill_process("zookeeper", allow_fail=False)
	time.sleep(2)
	assert not self.alive(node), “Expected Zookeeper service to stop” 

In this example, the test terminates a zookeeper service via ``kill_process`` and then uses ``time.sleep`` to wait for it to stop. If terminating the process takes longer, the test will fail. The test may intermittently fail based on how fast a process terminates. Of course, there should be a timeout for termination to ensure that test does not run indefinitely. You could increase sleep time, but that also increases the test run length. A more explicit way to express this condition is to use :meth:`~ ducktape.utils.util.wait_until` with a timeout::

	node.account.kill_process("zookeeper", allow_fail=False)
	wait_until(lambda: not self.alive(node),
                   timeout_sec=5,
                   err_msg="Timed out waiting for zookeeper to stop.")

The test will progress as soon as condition is met, and timeout ensures that the test does not run indefinitely if termination never ends.

Think carefully about the condition to check. A common source of issues is incorrect choice of condition of successful service start in ``start_node`` implementation. One way to check that a service starts successfully is to wait for some specific log output. However, make sure that this specific log message is always printed after the things run successfully. If there is still a chance that service may fail to start after the log is printed, this may cause race conditions and flaky tests. Sometimes it could be better to check if the service runs successfully by querying a service or checking some metrics if they are available.


Test Order Dependency
^^^^^^^^^^^^^^^^^^^^^

Make sure that your services properly cleanup the state in ``clean_node`` implementation. Failure to properly clean up the state can cause the next run of the test to fail or fail intermittently if other tests happen to clean same directories for example. One of the benefits of isolation that ducktape assumes is that you can assume you have complete control of the machine. It is ok to delete the entire working space. It is also safe to kill all java processes you can find rather than being more targeted. So, clean up aggressively.

Incorrect Assumptions
^^^^^^^^^^^^^^^^^^^^^

It is possible that assumptions about how the system works that we are testing are incorrect. One way to help debug this is to use more detailed comments why certain checks are made.


Tools for Managing Logs
=======================

Analyzing and matching up logs from a distributed service could be time consuming. There are many good tools for working with logs. Examples include http://lnav.org/, http://list.xmodulo.com/multitail.html, and http://glogg.bonnefon.org/.

Validating Ssh Issues
=======================

Ducktape supports running custom validators when an ssh error occurs, allowing you to run your own validation against a host.
this is done simply by running ducktape with the `--ssh-checker-function`, followed by the module path to your function, so for instance::
    
    ducktape my-test.py --ssh-checker-function my.module.validator.validate_ssh

this function will take in the ssh error raised as its first argument, and the remote account object as its second.
