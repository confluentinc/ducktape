Distributed System Integration & Performance Testing Library
============================================================

This repository contains tools for running system integraton and performance
tests. It provides utilities for pulling up and tearing down services
easily, using Vagrant to let you test things on local VMs or run on EC2
nodes. Tests are just Python scripts that run a set of services, possibly
triggering special events (e.g. bouncing a service), collect results (such as
logs or console output) and report results (expected conditions met, performance
results, etc.).

Users
----
This section contains basic information on how to run tests with ducktape.

Installation
------------
While ducktape is in active development, clone this repository and run:

    cd ducktape
    python setup.py install
    
This makes the ducktape script available in your PATH, and the ducktape modules
available for import in your own projects.

If you are are a ducktape developer, consider using the develop command instead of install. This allows you to make code changes without constantly reinstalling ducktape (see http://stackoverflow.com/questions/19048732/python-setup-py-develop-vs-install for more information)

    cd ducktape
    python setup.py develop

Running Tests
-------------
To run one or more tests, run
`ducktape <relative_path_to_testfile>`
`ducktape <relative_path_to_testdirectory>`

ducktape will discover tests and run all tests that it finds. ducktape can take multiple test files and/or test directories
 as its arguments, and will discover and run tests from every path provided.

Test Output
-----------
Test results are output in the directory `<session_id>`. `<session_id>` looks like `<date>--<test_number>`. For
example: `2015-03-28--002`

ducktape does its best to group test results and log files in a sensible way. The output directory structure is 
the following:

```
<test_id>_results
    summary - top level summary - indicates single aggregate PASS/FAIL and all individual PASS/FAIL results. Each entry gives enough information to easily
    service_log
    <test_class>/<test_method> (e.g. test_thing)
        test_logs - log(s) from logic in test driver go here
            log.info
            log.debug
        service_logs - log(s) collected from service nodes go here
            <service_name>[__<num>]/<instance_id>  - e.g. kafka_service__1/3 - means 1st kafka service in test on
            metadata
    ...
```

Test Discovery
--------------
ducktape searches recursively in the given path(s) for your tests. If the path is a directory, it follows a few rules for discovery:

* Try to import all modules that "look like" test modules. I.e. the module name is either "test_*.py" or "*_test.py"
* From imported modules, gather classes that are leaf subclasses of the ducktape.tests.test.Test class. Leaf subclass means the class has no subclass.

If you run ducktape on a file, i.e. `ducktape <path_to_file>`, ducktape will ignore the module name and search for leafy subclasses of Test.

Developers
----------
This section is for those of you who might want to implement a test or a service using the ducktape framework.

Writing New Tests
-----------------

"Test" is currently a misnomer -- there's no test runner or assertion code
currently. A test is just a series of service operations. The simplest tests
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
`ducktape.cluster.RemoteAccount` instance which lets you easily perform remote
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

Unit Tests
----------

It's a good idea to write and run unit tests on the ducktape framework itself. You can run the tests via the setup.py script:

    python setup.py test

Alternatively, if you've installed pytest (`sudo pip install pytest`) you can run
it directly on the `tests` directory`:

    py.test tests
