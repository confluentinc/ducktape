# Copyright 2016 Confluent Inc.
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

from __future__ import print_function

from ducktape.command_line.defaults import ConsoleDefaults
from ducktape.utils.util import ducktape_version

import argparse
import itertools
import os
import sys


def create_ducktape_parser():
    parser = argparse.ArgumentParser(description="Discover and run your tests")
    parser.add_argument('test_path', metavar='test_path', type=str, nargs='*', default=[os.getcwd()],
                        help='One or more test identifiers or test suite paths to execute')
    parser.add_argument('--exclude', type=str, nargs='*', default=None,
                        help='one or more space-delimited strings indicating which tests to exclude')
    parser.add_argument("--collect-only", action="store_true", help="display collected tests, but do not run.")
    parser.add_argument("--collect-num-nodes", action="store_true",
                        help="display total number of nodes requested by all tests, but do not run anything.")
    parser.add_argument("--debug", action="store_true", help="pipe more verbose test output to stdout.")
    parser.add_argument("--config-file", action="store", default=ConsoleDefaults.USER_CONFIG_FILE,
                        help="path to project-specific configuration file.")
    parser.add_argument("--compress", action="store_true", help="compress remote logs before collection.")
    parser.add_argument("--cluster", action="store", default=ConsoleDefaults.CLUSTER_TYPE,
                        help="cluster class to use to allocate nodes for tests.")
    parser.add_argument("--default-num-nodes", action="store", type=int, default=None,
                        help="Global hint for cluster usage. A test without the @cluster annotation will "
                        "default to this value for expected cluster usage.")
    parser.add_argument("--cluster-file", action="store", default=None,
                        help="path to a json file which provides information needed to initialize a json cluster. "
                             "The file is used to read/write cached cluster info if "
                             "cluster is ducktape.cluster.vagrant.VagrantCluster.")
    parser.add_argument("--results-root", action="store", default=ConsoleDefaults.RESULTS_ROOT_DIRECTORY,
                        help="path to custom root results directory. Running ducktape with this root "
                             "specified will result in new test results being stored in a subdirectory of "
                             "this root directory.")
    parser.add_argument("--exit-first", action="store_true", help="exit after first failure")
    parser.add_argument("--no-teardown", action="store_true",
                        help="don't kill running processes or remove log files when a test has finished running. "
                             "This is primarily useful for test developers who want to interact with running "
                             "services after a test has run.")
    parser.add_argument("--version", action="store_true", help="display version")
    parser.add_argument("--parameters", action="store",
                        help="inject these arguments into the specified test(s). Specify parameters as a JSON string.")
    parser.add_argument("--globals", action="store",
                        help="user-defined globals go here. "
                             "This can be a file containing a JSON object, or a string representing a JSON object.")
    parser.add_argument("--max-parallel", action="store", type=int, default=1,
                        help="Upper bound on number of tests run simultaneously.")
    parser.add_argument("--repeat", action="store", type=int, default=1,
                        help="Use this flag to repeat all discovered tests the given number of times.")
    parser.add_argument("--subsets", action="store", type=int, default=1,
                        help="Number of subsets of tests to statically break the tests into to allow for parallel "
                             "execution without coordination between test runner processes.")
    parser.add_argument("--subset", action="store", type=int, default=0,
                        help="Which subset of the tests to run, based on the breakdown using the parameter for "
                             "--subsets")
    parser.add_argument("--historical-report", action="store", type=str,
                        help="URL of a JSON report file containing stats from a previous test run. If specified, "
                             "this will be used when creating subsets of tests to divide evenly by total run time "
                             "instead of by number of tests.")
    parser.add_argument("--skip-nodes-allocation", action="store_true", help="Use this flag to skip allocating "
                        "nodes for services. Can be used when running specific tests on a running platform")
    parser.add_argument("--sample", action="store", type=int,
                        help="The size of a random test sample to run")
    parser.add_argument("--fail-bad-cluster-utilization", action="store_true",
                        help="Fail a test if the test declared that it needs more nodes than it actually used. "
                             "E.g. if the test had `@cluster(num_nodes=10)` annotation, "
                             "but never used more than 5 nodes during its execution.")
    parser.add_argument("--fail-greedy-tests", action="store_true",
                        help="Fail a test if it has no @cluster annotation "
                             "or if @cluster annotation is empty. "
                             "You can still specify 0-sized cluster explicitly using either num_nodes=0 "
                             "or cluster_spec=ClusterSpec.empty()")
    parser.add_argument("--test-runner-timeout", action="store", type=int, default=1800000,
                        help="Amount of time in milliseconds between test communicating between the test runner"
                             " before a timeout error occurs. Default is 30 minutes")
    parser.add_argument("--ssh-checker-function", action="store", type=str, nargs="+",
                        help="Python module path(s) to a function that takes an exception and a remote account"
                        " that will be called when an ssh error occurs, this can give some "
                        "validation or better logging when an ssh error occurs. Specify any "
                        "number of module paths after this flag to be called."),
    parser.add_argument("--deflake", action="store", type=int, default=1,
                        help="the number of times a failed test should be ran in total (including its initial run) "
                             "to determine flakyness. When not present, deflake will not be used, "
                             "and a test will be marked as either passed or failed. "
                             "When enabled tests will be marked as flaky if it passes on any of the reruns")
    return parser


def get_user_config_file(args):
    """Helper function to get specified (or default) user config file.
    :return Filename which is the path to the config file.
    """
    parser = create_ducktape_parser()
    config_file = vars(parser.parse_args(args))["config_file"]
    assert config_file is not None
    return os.path.expanduser(config_file)


def config_file_to_args_list(config_file):
    """Parse in contents of config file, and return a list of command-line options parseable by the ducktape parser.

    Skip whitespace lines and comments (lines prefixed by "#")
    """
    if config_file is None:
        raise RuntimeError("config_file is None")

    # Read in configuration, but ignore empty lines and comments
    config_lines = [line for line in open(config_file).readlines()
                    if (len(line.strip()) > 0 and line.lstrip()[0] != '#')]

    return list(itertools.chain(*[line.split() for line in config_lines]))


def parse_non_default_args(parser: argparse.ArgumentParser, defaults: dict, args: list) -> dict:
    """
    Parse and remove default args from a list of args, and return the dict of the parsed args.
    """
    parsed_args = vars(parser.parse_args(args))

    # remove defaults
    for key, value in defaults.items():
        if parsed_args[key] == value:
            del parsed_args[key]

    return parsed_args


def parse_args(args):
    """Parse in command-line and config file options.

    Command line arguments have the highest priority, then user configs specified in ~/.ducktape/config, and finally
    project configs specified in <ducktape_dir>/config.
    """

    parser = create_ducktape_parser()

    if len(args) == 0:
        # Show help if there are no arguments
        parser.print_help()
        sys.exit(0)

    # Collect arguments from project config file, user config file, and command line
    # later arguments supersede earlier arguments
    parsed_args_list = []

    # First collect all the default values
    defaults = vars(parser.parse_args([]))

    project_config_file = ConsoleDefaults.PROJECT_CONFIG_FILE
    # Load all non-default args from project config file.
    if os.path.exists(project_config_file):

        parsed_args_list.append(
            parse_non_default_args(
                parser,
                defaults,
                config_file_to_args_list(project_config_file)
            )
        )

    # Load all non-default args from user config file.
    user_config_file = get_user_config_file(args)
    if os.path.exists(user_config_file):
        parsed_args_list.append(
            parse_non_default_args(
                parser,
                defaults,
                config_file_to_args_list(user_config_file)
            )
        )

    # Load all non-default args from the command line.
    parsed_args_list.append(
        parse_non_default_args(
            parser,
            defaults,
            args
        )
    )

    # Don't need to copy, done with the defaults dict.
    # Start with the default args, and layer on changes.
    parsed_args_dict = defaults
    for parsed_args in parsed_args_list:
        parsed_args_dict.update(parsed_args)

    if parsed_args_dict["version"]:
        print(ducktape_version())
        sys.exit(0)
    return parsed_args_dict
