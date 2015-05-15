# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.tests.loader import TestLoader, LoaderException
from ducktape.tests.runner import SerialTestRunner
from ducktape.tests.reporter import SimpleStdoutReporter, SimpleFileReporter, HTMLReporter
from ducktape.tests.session import SessionContext
from ducktape.cluster.vagrant import VagrantCluster
from ducktape.command_line.config import ConsoleConfig
from ducktape.tests.session import generate_session_id, generate_results_dir
from ducktape.utils.local_filesystem_utils import mkdir_p

import argparse
import os
import sys


def parse_args():
    """Parse in command-line options.
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(description="Discover and run your tests")
    parser.add_argument('test_path', metavar='test_path', type=str, nargs='+',
                        help='one or more space-delimited strings indicating where to search for tests')
    parser.add_argument("--collect-only", action="store_true", help="display collected tests, but do not run")
    parser.add_argument("--debug", action="store_true", help="pipe more verbose test output to stdout")
    parser.add_argument("--exit-first", action="store_true", help="exit after first failure")
    parser.add_argument("--no-teardown", action="store_true", help="do not stop and clean services when test finishes")

    args = parser.parse_args()
    return args


def extend_import_paths(paths):
    """Extends sys.path with top-level packages found based on a set of input paths. This only adds top-level packages
    in order to avoid naming conflict with internal packages, e.g. ensure that a package foo.bar.os does not conflict
    with the top-level os package.

    Adding these import paths is necessary to make importing tests work even when the test modules are not available on
    PYTHONPATH/sys.path, as they normally will be since tests generally will not be installed and available for import

    :param paths:
    :return:
    """
    for path in paths:
        dir = os.path.abspath(path if os.path.isdir(path) else os.path.dirname(path))
        while(os.path.exists(os.path.join(dir, '__init__.py'))):
            dir = os.path.dirname(dir)
        sys.path.append(dir)


def setup_results_directory(results_dir, session_id):
    """Make directory in which results will be stored"""
    if os.path.isdir(results_dir):
        raise Exception(
            "A test results directory with session id %s already exists. Exiting without overwriting..." % session_id)
    mkdir_p(results_dir)
    latest_test_dir = os.path.join(ConsoleConfig.RESULTS_ROOT_DIRECTORY, "latest")

    if os.path.exists(latest_test_dir):
        os.unlink(latest_test_dir)
    os.symlink(results_dir, latest_test_dir)


def main():
    """Ducktape entry point. This contains top level logic for ducktape command-line program which does the following:

        Discover tests
        Initialize cluster for distributed services
        Run tests
        Report a summary of all results
    """
    args = parse_args()

    # Make .ducktape directory where metadata such as the last used session_id is stored
    if not os.path.isdir(ConsoleConfig.METADATA_DIR):
        os.makedirs(ConsoleConfig.METADATA_DIR)

    # Generate a shared 'global' identifier for this test run and create the directory
    # in which all test results will be stored
    session_id = generate_session_id(ConsoleConfig.SESSION_ID_FILE)
    results_dir = generate_results_dir(session_id)

    setup_results_directory(results_dir, session_id)
    session_context = SessionContext(session_id, results_dir, cluster=None, args=args)

    # Discover and load tests to be run
    extend_import_paths(args.test_path)
    loader = TestLoader(session_context)
    try:
        test_classes = loader.discover(args.test_path)
    except LoaderException as e:
        print "Failed while trying to discover tests: {}".format(e)
        sys.exit(1)

    if args.collect_only:
        print test_classes
        sys.exit(0)

    # Initializing the cluster is slow, so do so only if
    # tests are sure to be run
    session_context.cluster = VagrantCluster()

    # Run the tests
    runner = SerialTestRunner(session_context, test_classes)
    test_results = runner.run_all_tests()

    # Report results
    # TODO command-line hook for type of reporter
    reporter = SimpleStdoutReporter(test_results)
    reporter.report()
    reporter = SimpleFileReporter(test_results)
    reporter.report()

    # Generate HTML reporter
    reporter = HTMLReporter(test_results)
    reporter.report()

    if not test_results.get_aggregate_success():
        sys.exit(1)
