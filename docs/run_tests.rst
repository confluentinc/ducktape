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

.. argparse::
   :module: ducktape.command_line.parse_args
   :func: create_ducktape_parser
   :prog: ducktape

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


To see an example of the output structure, go `here`_ and click on one of the details links.

.. _here: http://testing.confluent.io/confluent-kafka-system-test-results/
