Distributed System Integration & Performance Testing Library
============================================================

This repository contains tools for running system integraton and performance
tests. It provides utilities for pulling up and tearing down services
easily, using Vagrant to let you test things on local VMs or run on EC2
nodes. Tests are just Python scripts that run a set of services, possibly
triggering special events (e.g. bouncing a service), collect results (such as
logs or console output) and report results (expected conditions met, performance
results, etc.).


Install
-------

Note: as a general rule, it's recommended to use an isolation tool such as `virtualenv`.

Prerequisites:
* ducktape uses paramiko, which depends upon cryptography, which has non-python external requirements
* https://cryptography.io/en/latest/installation/
* OSX should just work
* Ubuntu:

    ```
    $ sudo apt-get install build-essential libssl-dev libffi-dev python-dev
    ```
* Fedora and RHEL-derivatives:

    ```
    $ sudo yum install gcc libffi-devel python-devel openssl-devel
    ```


Install ducktape: `pip install ducktape`

Use
---
ducktape discovers and runs tests in the path provided. 

    ducktape <relative_path_to_testdirectory>               # e.g. ducktape dir/tests
    ducktape <relative_path_to_file>                        # e.g. ducktape dir/tests/my_test.py
    ducktape <path_to_test>[::SomeTestClass]                # e.g. ducktape dir/tests/my_test.py::TestA
    ducktape <path_to_test>[::SomeTestClass[.test_method]]  # e.g. ducktape dir/tests/my_test.py::TestA.test_a

Use the `--collect-only` flag to discover tests without running any:

    ducktape <path_to_testfile_or_directory> --collect-only

Use the `--parameters` flag followed by a JSON string to specify which parameters to use when running the discovered test:

    ducktape path/to/test.py::TestClass.test_method --parameters '{"x": 1, "y": 20}'

Configuration
-------------

To see a complete listing of options run:

    ducktape --help

You can configure options in three locations: on the command line (highest priority), in a user configuration file in
`~/.ducktape/config`, and in a project-specific configuration `<project_dir>/.ducktape/config` (lowest priority).
Configuration files use the same syntax as command line arguments and may split arguments across multiple lines, e.g.:

    --debug
    --exit-first
    --cluster=ducktape.cluster.json.JsonCluster

Test Output
-----------
Test results go in `results/<session_id>`. `<session_id>` looks like `<date>--<test_number>`. For example, `results/2015-03-28--002`

ducktape does its best to group test results and log files in a sensible way. The output directory is 
structured like so:

```
<session_id> 
    session_log.info
    session_log.debug
    report.txt   # Summary report of all tests run in this session
    report.html  # Open this to see summary report in a browser
    report.css
    
    <test_class_name>
        <test_method_name>
            test_log.info
            test_log.debug
            report.txt   # Report on this single test
            [data.json]  # Present if the test returns data
        
            <service_1>
                <node_1>
                    some_logs
                <node_2>
                    some_logs
    ...
```

To see an example of the output structure, go to http://testing.confluent.io/confluent_platform/latest and click on one of the details links.

Write ducktape Tests
--------------------

Subclass ducktape.tests.test.Test and implement as many `test` methods as you
want. The name of each test method must start or end with `test`,
e.g. `test_functionality` or `example_test`. Typically, a test will 
start a few services, collect and/or validate some data, and then finish.

If the test method finishes with no exceptions, the test is recorded as successful, otherwise it is recorded as a failure.

The `test` base class sets up logger you can use which is tagged by class name,
so adding some logging for debugging or to track the progress of tests is easy:

    self.logger.debug("End-to-end latency %d: %s", idx, line.strip())
    
These types of tests can be difficult to debug, so err toward more rather than
less logging. However, keep in mind that logs are collected a multiple log
levels, and only higher log levels are displayed to the console while the test
runs. Make sure you log at the appropriate level.

Here is an example of a test that just starts a Zookeeper cluster with 2 nodes, and a 
Kafka cluster with 3 nodes.

    class StartServicesTest(Test):
        """Make sure we can start Kafka and Zookeeper services."""
        def __init__(self, test_context):
            super(StartServicesTest, self).__init__(test_context=test_context)
            self.zk = ZookeeperService(test_context, num_nodes=2)
            self.kafka = KafkaService(test_context, num_nodes=3, self.zk)

        def test_services_start(self):
            self.zk.start()
            self.kafka.start()

Add New Services
-------------------

"Service" refers generally to any process, possibly long-running, which you
want to run on the test cluster. 

These can be services you would actually deploy
(e.g., Kafka brokers, ZK servers, REST proxy) or processes used during testing
(e.g. producer/consumer performance processes). You should also make each
service class support starting a variable number of instances of the service so
test code is as concise as possible.

Each service is implemented as a class and should at least implement the following:

    start_node(self, node) - start the service (possibly waiting to ensure it started successfully)
    stop_node(self, node) - kill processes on the given node
    clean_node(self, node) - remove persistent state leftover from testing, e.g. log files

These may block to ensure services start or stop properly, but
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
`ducktape.cluster.RemoteAccount` instance which lets you easily perform remote
operations such as running commands via SSH or creating files. By default, these
operations try to hide output (but provide it to you if you need to extract
some subset of it) and *checks status codes for errors* so any operations that
fail cause an obvious failure of the entire test.

Developer Install
-----------------
If you are are a ducktape developer, consider using the develop command instead of install. This allows you to make code changes without constantly reinstalling ducktape (see http://stackoverflow.com/questions/19048732/python-setup-py-develop-vs-install for more information)

    cd ducktape
    python setup.py develop
    
To uninstall:

    cd ducktape
    python setup.py develop --uninstall


Unit Tests
----------
You can run the tests via the setup.py script:

    python setup.py test

Alternatively, if you've installed pytest (`sudo pip install pytest`) you can run
it directly on the `tests` directory`:

    py.test tests
    
Contribute
----------

- Source Code: https://github.com/confluentinc/ducktape
- Issue Tracker: https://github.com/confluentinc/ducktape/issues

License
-------
The project is licensed under the Apache 2 license.
