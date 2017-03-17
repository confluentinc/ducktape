.. _topics-run_tests:

=========
Run Tests
=========

Running Tests
=============

ducktape discovers and runs tests in the path provided, here are some ways to run tests::

    ducktape <relative_path_to_testdirectory>               # e.g. ducktape dir/tests
    ducktape <relative_path_to_file>                        # e.g. ducktape dir/tests/my_test.py
    ducktape <path_to_test>[::SomeTestClass]                # e.g. ducktape dir/tests/my_test.py::TestA
    ducktape <path_to_test>[::SomeTestClass[.test_method]]  # e.g. ducktape dir/tests/my_test.py::TestA.test_a

Options
=======

To see a complete listing of options run::

    ducktape --help


Here is a list of run options:

* **--collect-only**

    display collected tests, but do not run

* **--parameters**

    inject these arguments into the specified test(s). Specify parameters as a JSON string.

    For example::

        ducktape path/to/test.py::TestClass.test_method --parameters '{"x": 1, "y": 20}'

* **--repeat**

    use this flag to repeat all discovered tests the given number of times

* **--debug**

    turn on debug mode, pipe more verbose test output to stdout

* **--exit-first**

    exit after first failure

* **--no-teardown**

    don't kill running processes or remove log files when a test has finished running. "

    .. note:: This is primarily useful for test developers who want to interact with running services after a test has run

* **--max-parallel**

    upper bound on number of tests run simultaneously

* **--subsets**

    number of subsets of tests to statically break the tests into to allow for parallel execution without coordination between test runner processes

* **--subset**

    which subset of the tests to run, based on the breakdown using the parameter for ``--subsets``

* **--results-root**

    path to custom root results directory. Running ducktape with this root specified will result in new test results being stored in a subdirectory of this root directory
    defaults to ``./results``

* **--default-num-nodes**

    global hint for cluster usage. A test without the @cluster annotation will default to this value for expected cluster usage

* **--config-file**

    path to project-specific configuration file.
    defaults to ``~/.ducktape/config``

* **--historical-report**

    URL of a JSON report file containing stats from a previous test run. If specified, this will be used when creating subsets of tests to divide evenly by total run time instead of by number of tests

* **--compress**

    compress remote logs before collection

* **--cluster**

    cluster class to use to allocate nodes for tests

* **--cluster-file**

    path to a json file which provides information needed to initialize a json cluster.
    The file is used to read/write cached cluster info if cluster is ``ducktape.cluster.vagrant.VagrantCluster``

* **--globals**

    user defined globals. This can be a file containing a JSON object, or a string representing a JSON object

Configuration File
==================

You can configure options in three locations: on the command line (highest priority), in a user configuration file in
``~/.ducktape/config``, and in a project-specific configuration ``<project_dir>/.ducktape/config`` (lowest priority).
Configuration files use the same syntax as command line arguments and may split arguments across multiple lines::

    --debug
    --exit-first
    --cluster=ducktape.cluster.json.JsonCluster

Output
======

Test results go in ``results/<session_id>.<session_id>`` which looks like ``<date>--<test_number>``. For example: ``results/2015-03-28--002``

ducktape does its best to group test results and log files in a sensible way. The output directory is
structured like so::

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


To see an example of the output structure, go to http://testing.confluent.io/confluent_platform/latest and click on one of the details links.
