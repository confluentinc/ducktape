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

import importlib
import json
import os
import sys
import traceback

import pysistence

from ducktape.command_line.defaults import ConsoleDefaults
from ducktape.command_line.parse_args import parse_args
from ducktape.tests.loader import TestLoader, LoaderException
from ducktape.tests.reporter import SimpleStdoutSummaryReporter, SimpleFileSummaryReporter, HTMLSummaryReporter
from ducktape.tests.runner import SerialTestRunner
from ducktape.tests.session import SessionContext
from ducktape.tests.session import generate_session_id, generate_results_dir
from ducktape.utils.local_filesystem_utils import mkdir_p


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
        while os.path.exists(os.path.join(dir, '__init__.py')):
            dir = os.path.dirname(dir)
        sys.path.append(dir)


def get_user_defined_globals(globals_str):
    """Parse user-defined globals into an immutable dict using globals_str

    :param globals_str Either a file, in which case, attempt to open the file and parse the contents as JSON,
        or a JSON string representing a JSON object. The parsed JSON must represent a collection of key-value pairs,
        i.e. a python dict.
    :return dict containing user-defined global variables
    """
    if globals_str is None:
        return pysistence.make_dict()

    from_file = False
    if os.path.isfile(globals_str):
        # The string appears to be a file, so try loading JSON from file
        # This may raise an IOError if the file can't be read or a ValueError if the contents of the file
        # cannot be parsed.
        user_globals = json.loads(open(globals_str, "r").read())
        from_file = True
    else:
        try:
            # try parsing directly as json if it doesn't seem to be a file
            user_globals = json.loads(globals_str)
        except ValueError as ve:
            message = ve.message
            message += "\nglobals parameter %s is neither valid JSON nor a valid path to a JSON file." % globals_str
            raise ValueError(message)

    # Now check that the parsed JSON is a dictionary
    if not isinstance(user_globals, dict):
        if from_file:
            message = "The JSON contained in file %s must parse to a dict. "
        else:
            message = "JSON string referred to by globals parameter must parse to a dict. "
        message += "I.e. the contents of the JSON must be an object, not an array or primitive. "
        message += "Instead found %s, which parsed to %s" % (str(user_globals), type(user_globals))

        raise ValueError(message)

    # Use pysistence to create the immutable dict
    user_globals = pysistence.make_dict(**user_globals)
    return user_globals


def setup_results_directory(new_results_dir):
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
    args_dict = parse_args(sys.argv[1:])

    parameters = None
    if args_dict["parameters"]:
        try:
            parameters = json.loads(args_dict["parameters"])
        except ValueError as e:
            print "parameters are not valid json: " + str(e.message)
            sys.exit(1)

    args_dict["globals"] = get_user_defined_globals(args_dict.get("globals"))

    # Make .ducktape directory where metadata such as the last used session_id is stored
    if not os.path.isdir(ConsoleDefaults.METADATA_DIR):
        os.makedirs(ConsoleDefaults.METADATA_DIR)

    # Generate a shared 'global' identifier for this test run and create the directory
    # in which all test results will be stored
    session_id = generate_session_id(ConsoleDefaults.SESSION_ID_FILE)
    results_dir = generate_results_dir(args_dict["results_root"], session_id)
    setup_results_directory(results_dir)

    session_context = SessionContext(session_id=session_id, results_dir=results_dir, **args_dict)
    for k, v in args_dict.iteritems():
        session_context.logger.debug("Configuration: %s=%s", k, v)

    # Discover and load tests to be run
    extend_import_paths(args_dict["test_path"])
    loader = TestLoader(session_context, parameters)
    try:
        tests = loader.discover(args_dict["test_path"])
    except LoaderException as e:
        print "Failed while trying to discover tests: {}".format(e)
        sys.exit(1)

    if args_dict["collect_only"]:
        print "Collected %d tests:" % len(tests)
        for test in tests:
            print "    " + str(test)
        sys.exit(0)

    # Initializing the cluster is slow, so do so only if
    # tests are sure to be run
    try:
        (cluster_mod_name, cluster_class_name) = args_dict["cluster"].rsplit('.', 1)
        cluster_mod = importlib.import_module(cluster_mod_name)
        cluster_class = getattr(cluster_mod, cluster_class_name)
        session_context.cluster = cluster_class(cluster_file=args_dict["cluster_file"])
    except:
        print "Failed to load cluster: ", str(sys.exc_info()[0])
        print traceback.format_exc(limit=16)
        sys.exit(1)

    # Run the tests
    runner = SerialTestRunner(session_context, tests)
    test_results = runner.run_all_tests()

    # Report results
    reporter = SimpleStdoutSummaryReporter(test_results)
    reporter.report()
    reporter = SimpleFileSummaryReporter(test_results)
    reporter.report()

    # Generate HTML reporter
    reporter = HTMLSummaryReporter(test_results)
    reporter.report()

    update_latest_symlink(args_dict["results_root"], results_dir)
    if not test_results.get_aggregate_success():
        sys.exit(1)
