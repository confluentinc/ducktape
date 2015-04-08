# Copyright 2014 Confluent Inc.
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

from ducktape.cluster import VagrantCluster
from ducktape.logger import Logger
import importlib
import logging
import os
import inspect
import re


class Test(Logger):
    """
    Base class for tests that provides some minimal helper utilities'
    """

    def __init__(self, cluster):
        self.cluster = cluster

    def log_start(self):
        self.logger.info("Running test %s", self._short_class_name())

    def min_cluster_size(self):
        """
        Subclasses implement this to provide a helpful heuristic to prevent trying to run a test on a cluster
        with too few nodes.
        """
        raise NotImplementedError("All tests must implement this method.")

    @classmethod
    def run_standalone(cls):
        logging.basicConfig(level=logging.INFO)
        cluster = VagrantCluster()
        test = cls(cluster)

        if test.min_cluster_size() > cluster.num_available_nodes():
            raise RuntimeError(
                "There are not enough nodes available in the cluster to run this test. Needed: %d, Available: %d" %
                (test.min_cluster_size(), cluster.num_available_nodes()))

        test.log_start()
        test.run()


class TestLoader(Logger):
    DEFAULT_TEST_FILE_PATTERN = "(^test_.*\.py$)|(^.*_test\.py$)"

    def discover(self, base_dir, pattern=DEFAULT_TEST_FILE_PATTERN):
        """Recurse through file hierarchy beginning at base_dir and returns a list of all found test classes.

        - Discover modules that 'look like' a test. By default, this means the filename is "test_*" or "*_test.py"
        - Discover test classes within each test module. A test class is a subclass of Test which is a leaf
          (i.e. it has no subclasses).
        """
        test_files = self.find_test_files(base_dir, pattern)
        test_modules = self.import_modules(test_files)

        # pull test_classes out of test_modules
        test_classes = []
        for module in test_modules:
            try:
                test_classes.extend(self.get_test_classes(module))
            except Exception as e:
                self.logger.debug("Error getting test classes from module: " + e.message)

        self.logger.info("Discovered these test classes: " + str(test_classes))
        return test_classes

    def find_test_files(self, base_dir, pattern=DEFAULT_TEST_FILE_PATTERN):
        """Return a list of files underneath base_dir that look like test files.

        The returned file names are absolute paths to the files in question.
        """
        test_files = []

        for pwd, dirs, files in os.walk(base_dir):
            for f in files:
                file_path = os.path.abspath(os.path.join(pwd, f))
                if self.is_test_file(file_path, pattern):
                    test_files.append(file_path)

        return test_files

    def import_modules(self, file_list):
        """Attempt to import modules in the file list.
        Assume all files in the list are absolute paths ending in '.py'

        Return all imported modules.
        """
        module_list = []

        for f in file_list:
            if f[-3:] != ".py" or not os.path.isabs(f):
                raise Exception("Expected absolute path ending in '.py' but got " + f)

            # Try all possible module imports for given file
            path_pieces = f[:-3].split("/")  # Strip off '.py' before splitting
            while len(path_pieces) > 0:
                module_name = '.'.join(path_pieces)
                # Try to import the current file as a module
                try:
                    module_list.append(importlib.import_module(module_name))
                    self.logger.info("Successfully imported " + module_name)
                    break  # no need to keep trying
                except Exception as e:
                    self.logger.debug("Could not import " + module_name + ": " + e.message)
                    continue
                finally:
                    path_pieces = path_pieces[1:]

        return module_list

    def get_test_classes(self, module):
        """Return list of any all classes in the module object."""
        module_objects = module.__dict__.values()
        return filter(lambda x: self.is_test_class(x), module_objects)

    def is_test_file(self, file_name, pattern=DEFAULT_TEST_FILE_PATTERN):
        """By default, a test file looks like test_*.py or *_test.py"""
        return re.match(pattern, os.path.basename(file_name)) is not None

    def is_test_class(self, obj):
        """An object is a test class if it's a leafy subclass of Test.
        """
        return inspect.isclass(obj) and issubclass(obj, Test) and len(obj.__subclasses__()) == 0


