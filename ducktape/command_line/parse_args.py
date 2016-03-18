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

from ducktape.command_line.defaults import ConsoleDefaults
from ducktape.utils.util import ducktape_version

import argparse
import itertools
import os
import sys


def create_ducktape_parser():
    parser = argparse.ArgumentParser(description="Discover and run your tests")
    parser.add_argument('test_path', metavar='test_path', type=str, nargs='*', default=[os.getcwd()],
                        help='one or more space-delimited strings indicating where to search for tests.')
    parser.add_argument("--collect-only", action="store_true", help="display collected tests, but do not run.")
    parser.add_argument("--debug", action="store_true", help="pipe more verbose test output to stdout.")
    parser.add_argument("--config-file", action="store", default=ConsoleDefaults.USER_CONFIG_FILE,
                        help="path to project-specific configuration file.")
    parser.add_argument("--cluster", action="store", default=ConsoleDefaults.CLUSTER_TYPE,
                        help="cluster class to use to allocate nodes for tests.")
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
    args_list = []

    project_config_file = ConsoleDefaults.PROJECT_CONFIG_FILE
    if os.path.exists(project_config_file):
        args_list.extend(config_file_to_args_list(project_config_file))

    user_config_file = get_user_config_file(args)
    if os.path.exists(user_config_file):
        args_list.extend(config_file_to_args_list(user_config_file))

    args_list.extend(args)
    parsed_args_dict = vars(parser.parse_args(args_list))

    if parsed_args_dict["version"]:
        print ducktape_version()
        sys.exit(0)

    return parsed_args_dict
