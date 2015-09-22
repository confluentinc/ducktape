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

from ducktape.command_line.main import setup_results_directory
from ducktape.command_line.main import update_latest_symlink

import os
import os.path
import tempfile


class CheckSetupResultsDirectory(object):
    def setup_method(self, method):
        self.results_root = tempfile.mkdtemp()
        self.results_dir = os.path.join(self.results_root, "results_directory")
        self.latest_symlink = os.path.join(self.results_root, "latest")

    def validate_directories(self):
        """Validate existence of results directory and correct symlink"""
        assert os.path.exists(self.results_dir)
        assert os.path.exists(self.latest_symlink)
        assert os.path.islink(self.latest_symlink)

        # check symlink points to correct location
        assert os.path.realpath(self.latest_symlink) == os.path.realpath(self.results_dir)

    def check_creation(self):
        """Check results and symlink from scratch"""
        assert not os.path.exists(self.results_dir)
        setup_results_directory(self.results_root, self.results_dir)
        update_latest_symlink(self.results_root, self.results_dir)
        self.validate_directories()

    def check_symlink(self):
        """Check "latest" symlink behavior"""

        # if symlink already exists
        old_results = os.path.join(self.results_root, "OLD")
        os.mkdir(old_results)
        os.symlink(old_results, self.latest_symlink)
        assert os.path.islink(self.latest_symlink) and os.path.exists(self.latest_symlink)

        setup_results_directory(self.results_root, self.results_dir)
        update_latest_symlink(self.results_root, self.results_dir)
        self.validate_directories()

        # Try again if symlink exists and points to nothing
        os.rmdir(self.results_dir)
        assert os.path.islink(self.latest_symlink) and not os.path.exists(self.latest_symlink)
        setup_results_directory(self.results_root, self.results_dir)
        update_latest_symlink(self.results_root, self.results_dir)
        self.validate_directories()
