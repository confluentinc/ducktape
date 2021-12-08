.. _topics-run_tests:

=========
Run Tests
=========

Running Tests
=============

ducktape discovers and runs tests in the path(s) provided.
You can specify a folder with tests (all tests in Python modules named with "test\_" prefix or "_test" suffix will be
run), a specific test file (with any name) or even a specific class or test method, via absolute or relative paths.
You can optionally specify a specific set of parameters for tests with ``@parametrize`` or ``@matrix`` annotations::

    ducktape <relative_path_to_testdirectory>                   # e.g. ducktape dir/tests
    ducktape <relative_path_to_file>                            # e.g. ducktape dir/tests/my_test.py
    ducktape <path_to_test>[::SomeTestClass]                    # e.g. ducktape dir/tests/my_test.py::TestA
    ducktape <path_to_test>[::SomeTestClass[.test_method]]      # e.g. ducktape dir/tests/my_test.py::TestA.test_a
    ducktape <path_to_test>[::TestClass[.method[@params_json]]] # e.g. ducktape 'dir/tests/my_test.py::TestA.test_a@{"x": 100}'


Excluding Tests
===============

Pass ``--exclude`` flag to exclude certain test(s) from the run, using the same syntax::

    ducktape ./my_tests_dir --exclude ./my_tests_dir/test_a.py ./my_tests_dir/test_b.py::TestB.test_b



Test Suites
===========

Test suite is a collection of tests to run, optionally also specifying which tests to exclude. Test suites are specified
via YAML file

.. code-block:: yaml

    # list all tests that are part of the suite under the test suite name:
    my_test_suite:
        - ./my_tests_dir/  # paths are relative to the test suite file location
        - ./another_tests_dir/test_file.py::TestClass.test_method  # same syntax as passing tests directly to ducktape
        - './another_tests_dir/test_file.py::TestClass.parametrized_method@{"x": 100}'  # params are supported too
        - ./third_tests_dir/prefix_*.py  # basic globs are supported (* and ? characters)

    # each YAML file can contain one or more test suites:
    another_test_suite:
        # you can optionally specify excluded tests in the suite as well using the following syntax:
        included:
            - ./some_tests_dir/
        excluded:
            - ./some_tests_dir/*_large_test.py


Running Test Suites
===================

Tests suites are run in the same fashion as separate tests.

Run a single test suite::

    ducktape ./path/to/test_suite.yml

Run multiple test suites::

    ducktape ./path/to/test_suite_1.yml ./test_suite_2.yml

You can specify both tests and test suites at the same time::

    ducktape ./my_test.py ./my_test_suite.yml ./another_test.py::TestClass.test_method

If the same test method is effectively specified more than once, it will only be executed once.

For example, if ``test_suite.yml`` lists ``test_a.py`` then running the following command
will execute ``test_a.py`` only once::

    ducktape test_suite.yml test_a.py

If you specify a folder, all tests (ie python files) under that folder will be discovered, but test suites will be not.

For example, if ``test_dir`` contains ``my_test.py`` and ``my_test_suite.yml``, then running::

    ducktape ./test_dir

will execute ``my_test.py`` but skip ``my_test_suite.yml``.

To execute both ``my_test.py`` and ``my_test_suite.yml`` you need to specify test suite path explicitly::

    ducktape ./test_dir/ ./test_dir/my_test_suite.yml



Exclude and Test Suites
=======================

Exclude section in the test suite applies only to that test suite. ``--exclude`` parameter passed to ducktape applies
to all loaded tests and test suites.

For example, if ``test_dir`` contains ``test_a.py``, ``test_b.py`` and ``test_c.py``, and ``test_suite.yml`` is:

.. code-block:: yaml

    suite_one:
        included:
            - ./test_dir/*.py
        excluded:
            - ./test_dir/test_a.py
    suite_two:
        included:
            - ./test_dir/
        excluded:
            - ./test_dir/test_b.py

Then running::

    ducktape test_suite.yml
runs each of ``test_a.py``, ``test_b.py`` and ``test_c.py`` once


But running::

    ducktape test_suite.yml --exclude test_dir/test_a.py
runs only ``test_b.py`` and ``test_c.py`` once, and skips ``test_a.py``.


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
