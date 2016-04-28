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

from ducktape.command_line.parse_args import parse_args

from cStringIO import StringIO
from exceptions import SystemExit

import os
import re
import shutil
import sys
import tempfile


class Capturing(object):
    """This context manager can be used to capture stdout from a function call.
    E.g.
        with Capture() as captured:
            call_function()
        assert captured.output == expected_output
    """
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.output = self._stringio.getvalue()
        sys.stdout = self._stdout


class CheckParseArgs(object):

    def check_empty_args(self):
        """Check that parsing an empty args list results in printing a usage message, followed by sys.exit(0) """
        try:
            with Capturing() as captured:
                parse_args([])
        except SystemExit as e:
            assert e.code == 0
            assert captured.output.find("usage") >= 0

    def check_version(self):
        """If --version is present, ducktape should print version and exit"""
        try:
            with Capturing() as captured:
                parse_args(["--version"])
        except SystemExit as e:
            assert e.code == 0
            assert re.search("[\d]+\.[\d]+\.[\d]+", captured.output) is not None

    def check_empty_test_path(self):
        """Check that default test_path is an array consisting of cwd."""
        args = ["--collect-only"]
        parsed = parse_args(args)
        parsed_paths = [os.path.abspath(p) for p in parsed["test_path"]]

        assert parsed_paths == [os.path.abspath('.')]

    def check_multiple_test_paths(self):
        """Check that parser properly handles multiple "test paths". It should capture a list of test paths. """
        paths = ["path1"]
        args = ["--debug"] + paths + ["--collect-only"]
        parsed = parse_args(args)
        assert parsed["test_path"] == paths

        paths = ["path%d" % i for i in range(10)]
        args = ["--cluster-file", "my-cluster-file"] + paths + ["--debug", "--exit-first"]
        parsed = parse_args(args)
        assert parsed["test_path"] == paths

    def check_config_overrides(self, monkeypatch):
        """Check that parsed arguments pick up values from config files, and that overrides match precedence."""

        tmpdir = tempfile.mkdtemp(dir="/tmp")
        # Create tmp file for global config
        project_cfg_filename = os.path.join(tmpdir, "ducktape-project.cfg")
        user_cfg_filename = os.path.join(tmpdir, "ducktape-user.cfg")

        project_cfg = [
            "--cluster-file CLUSTERFILE-project",
            "--results-root RESULTSROOT-project",
            "--parameters PARAMETERS-project"
        ]

        # user_cfg options should override project_cfg
        user_cfg = [
            "--results-root RESULTSROOT-user",
            "--parameters PARAMETERS-user"
        ]

        try:
            monkeypatch.setattr("ducktape.command_line.defaults.ConsoleDefaults.PROJECT_CONFIG_FILE", project_cfg_filename)
            monkeypatch.setattr("ducktape.command_line.defaults.ConsoleDefaults.USER_CONFIG_FILE", user_cfg_filename)

            with open(project_cfg_filename, "w") as project_f:
                project_f.write("\n".join(project_cfg))

            with open(user_cfg_filename, "w") as user_f:
                user_f.write("\n".join(user_cfg))

            # command-line options should override user_cfg and project_cfg
            args_dict = parse_args(["--parameters", "PARAMETERS-commandline"])

            assert args_dict["cluster_file"] == "CLUSTERFILE-project"
            assert args_dict["results_root"] == "RESULTSROOT-user"
            assert args_dict["parameters"] == "PARAMETERS-commandline"

        finally:
            shutil.rmtree(tmpdir)

    def check_config_file_option(self):
        """Check that config file option works"""
        tmpdir = tempfile.mkdtemp(dir="/tmp")
        user_cfg_filename = os.path.join(tmpdir, "ducktape-user.cfg")

        user_cfg = [
            "--results-root RESULTSROOT-user",
            "--parameters PARAMETERS-user"
        ]

        try:
            with open(user_cfg_filename, "w") as user_f:
                user_f.write("\n".join(user_cfg))
            args_dict = parse_args(["--config-file", user_cfg_filename])
            assert args_dict["results_root"] == "RESULTSROOT-user"
            assert args_dict["parameters"] == "PARAMETERS-user"
        finally:
            shutil.rmtree(tmpdir)
