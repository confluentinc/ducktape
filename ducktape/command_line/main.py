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
from ducktape.tests.reporter import SimpleStdoutSummaryReporter, SimpleFileSummaryReporter, HTMLSummaryReporter
from ducktape.tests.session import SessionContext
from ducktape.command_line.config import ConsoleConfig
from ducktape.tests.session import generate_session_id, generate_results_dir
from ducktape.utils.local_filesystem_utils import mkdir_p
from ducktape.utils.util import ducktape_version

import argparse
import json
import os
import sys
import importlib
import itertools
import traceback


def parse_args():
    """Parse in command-line and config file options.

    Command line arguments have the highest priority, then user configs specified in ~/.ducktape/config, and finally
    project configs specified in <ducktape_dir>/config.

    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(description="Discover and run your tests")
    parser.add_argument('test_path', metavar='test_path', type=str, nargs='*', default=os.getcwd(),
                        help='one or more space-delimited strings indicating where to search for tests.')
    parser.add_argument("--collect-only", action="store_true", help="display collected tests, but do not run.")
    parser.add_argument("--debug", action="store_true", help="pipe more verbose test output to stdout.")
    parser.add_argument("--config-file", action="store", default=ConsoleConfig.PROJECT_CONFIG_FILE,
                        help="path to project-specific configuration file.")
    parser.add_argument("--cluster", action="store", default=ConsoleConfig.CLUSTER_TYPE,
                        help="cluster class to use to allocate nodes for tests.")
    parser.add_argument("--results-root", action="store", default=ConsoleConfig.RESULTS_ROOT_DIRECTORY,
                        help="path to custom root results directory. Running ducktape with this root " +
                             "specified will result in new test results being stored in a subdirectory of " +
                             "this root directory.")
    parser.add_argument("--exit-first", action="store_true", help="exit after first failure")
    parser.add_argument("--no-teardown", action="store_true",
                        help="don't kill running processes or remove log files when a test has finished running. " +
                             "This is primarily useful for test developers who want to interact with running " +
                             "services after a test has run. ")
    parser.add_argument("--version", action="store_true", help="display version")
    parser.add_argument("--parameters", action="store",
                        help="inject these arguments into the specified test(s). Specify parameters as a JSON string.")

    parser_arguments = []

    # The order of concatenation ensures overrides are handled properly -- for action=store arguments, the later options
    # will override earlier instances
    project_configs = []
    project_config_file = ConsoleConfig.PROJECT_CONFIG_FILE
    if os.path.exists(project_config_file):
        project_configs = list(itertools.chain(*[line.split() for line in open(project_config_file).readlines()]))
    parser_arguments.extend(project_configs)

    user_configs = []
    user_config_file = os.path.expanduser(ConsoleConfig.USER_CONFIG_FILE)
    if os.path.exists(user_config_file):
        user_configs = list(itertools.chain(*[line.split() for line in open(user_config_file).readlines()]))
    parser_arguments.extend(user_configs)

    if len(sys.argv[1:]) == 0:
        # Show help if there are no command-line arguments
        parser_arguments.append('-h')
    else:
        parser_arguments.extend(sys.argv[1:])

    args = parser.parse_args(parser_arguments)
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


def setup_results_directory(results_root, new_results_dir):
    """Make directory in which results will be stored"""
    if os.path.exists(new_results_dir):
        raise Exception(
            "A file or directory at %s already exists. Exiting without overwriting." % new_results_dir)
    mkdir_p(new_results_dir)


def update_latest_symlink(results_root, new_results_dir):
    """Create or update symlink "latest" which points to the new test results directory"""
    latest_test_dir = os.path.join(results_root, "latest")
    if os.path.islink(latest_test_dir):
        os.unlink(latest_test_dir)
    os.symlink(new_results_dir, latest_test_dir)


def main():
    """Ducktape entry point. This contains top level logic for ducktape command-line program which does the following:

        Discover tests
        Initialize cluster for distributed services
        Run tests
        Report a summary of all results
    """
    args = parse_args()
    if args.version:
        print ducktape_version()
        sys.exit(0)

    parameters = None
    if args.parameters:
        try:
            parameters = json.loads(args.parameters)
        except ValueError as e:
            print "parameters are not valid json: " + str(e.message)
            sys.exit(1)

    # Make .ducktape directory where metadata such as the last used session_id is stored
    if not os.path.isdir(ConsoleConfig.METADATA_DIR):
        os.makedirs(ConsoleConfig.METADATA_DIR)

    # Generate a shared 'global' identifier for this test run and create the directory
    # in which all test results will be stored
    session_id = generate_session_id(ConsoleConfig.SESSION_ID_FILE)
    results_dir = generate_results_dir(args.results_root, session_id)

    setup_results_directory(args.results_root, results_dir)
    session_context = SessionContext(session_id, results_dir, cluster=None, args=args)
    for k, v in vars(args).iteritems():
        session_context.logger.debug("Configuration: %s=%s", k, v)

    # Discover and load tests to be run
    extend_import_paths(args.test_path)
    loader = TestLoader(session_context, parameters)
    try:
        tests = loader.discover(args.test_path)
    except LoaderException as e:
        print "Failed while trying to discover tests: {}".format(e)
        sys.exit(1)

    if args.collect_only:
        print "Collected %d tests:" % len(tests)
        for test in tests:
            print "    " + str(test)
        sys.exit(0)

    # Initializing the cluster is slow, so do so only if
    # tests are sure to be run
    try:
        (cluster_mod_name, cluster_class_name) = args.cluster.rsplit('.', 1)
        cluster_mod = importlib.import_module(cluster_mod_name)
        cluster_class = getattr(cluster_mod, cluster_class_name)
        session_context.cluster = cluster_class()
    except:
        print "Failed to load cluster: ", str(sys.exc_info()[0])
        print traceback.format_exc(limit=16)
        sys.exit(1)

    # Run the tests
    runner = SerialTestRunner(session_context, tests)
    test_results = runner.run_all_tests()

    # Report results
    # TODO command-line hook for type of reporter
    reporter = SimpleStdoutSummaryReporter(test_results)
    reporter.report()
    reporter = SimpleFileSummaryReporter(test_results)
    reporter.report()

    # Generate HTML reporter
    reporter = HTMLSummaryReporter(test_results)
    reporter.report()

    update_latest_symlink(args.results_root, results_dir)
    if not test_results.get_aggregate_success():
        sys.exit(1)
