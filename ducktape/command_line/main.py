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

from ducktape.tests.loader import TestLoader
from ducktape.tests.runner import SerialTestRunner
from ducktape.tests.reporter import SimpleStdoutReporter, SimpleFileReporter, HTMLReporter
from ducktape.tests.session_context import TestSessionContext
from ducktape.cluster.vagrant import VagrantCluster
from ducktape.command_line.config import ConsoleConfig
from ducktape.tests.session_context import generate_session_id, generate_results_dir
from ducktape.unit_tests.mock import swap_in_mock_run, swap_in_mock_fixtures

import argparse
import os
import sys


def parse_args():
    """Parse in command-line options.
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(description="Discover and run your tests.")

    parser.add_argument('test_path', metavar='test_path', type=str, nargs='+',
                        help='path which tells ducktape which test(s) to run')

    parser.add_argument("--collect-only", action="store_true", help="display collected tests, but do not run")
    # TODO - delete --mock option - only used for development
    parser.add_argument("--mock", action="store_true", help="dev helper to simulate simple test runs")

    args = parser.parse_args()
    return args


def main():
    """Ducktape entry point. This contains top level logic for ducktape command-line program which does the following:

        Discover tests
        Initialize cluster for distributed services
        Run tests
        Report results
    """
    args = parse_args()

    if not os.path.isdir(ConsoleConfig.METADATA_DIR):
        os.makedirs(ConsoleConfig.METADATA_DIR)

    # Generate a shared 'global' identifier for this test run and create the directory
    # in which all test results will be stored
    session_id = generate_session_id(ConsoleConfig.SESSION_ID_FILE)
    results_dir = generate_results_dir(session_id)
    test_session_context = TestSessionContext(session_id, results_dir)
    if os.path.isdir(results_dir):
        raise Exception(
            "A test results directory with session id %s already exists. Exiting without overwriting..." % session_id)
    os.mkdir(results_dir)

    # Discover and load tests to be run
    loader = TestLoader()
    test_classes = loader.discover(args.test_path[0])
    if args.collect_only:
        print test_classes
        sys.exit(0)

    if args.mock:
        swap_in_mock_run(test_classes)
        swap_in_mock_fixtures(test_classes)

    # Initialize the cluster
    # TODO command-line hooks specify type of cluster and type of test runner
    cluster = VagrantCluster()
    runner = SerialTestRunner(test_session_context, test_classes, cluster)
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
