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

    def is_test_file(self, file_name):
        """test_*.py or *_test.py"""
        pattern = "(^test_.*\.py$)|(^.*_test\.py$)"
        return re.match(pattern, file_name) is not None

    def discover(self, base_dir):
        test_classes = []

        for pwd, dirs, files in os.walk(base_dir):
            for f in files:
                if not self.is_test_file(f):
                    continue

                # Try all possible module imports for given file
                path = os.path.abspath(os.path.join(pwd, f))
                path_pieces = path[:-3].split("/")
                while len(path_pieces) > 0:
                    module_name = '.'.join(path_pieces)
                    # Try to import the current file as a module
                    try:
                        module = importlib.import_module(module_name)
                        self.logger.info("Successfully imported " + module_name)
                    except Exception as e:
                        self.logger.warn("Could not import " + module_name + ": " + e.message)
                        continue
                    finally:
                        path_pieces = path_pieces[1:]

                    # Pull out any test classes from the module
                    try:
                        test_classes.extend(self.get_test_classes(module))
                    except Exception as e:
                        self.logger.warn("Error getting test classes from module: " + e.message)

        self.logger.info("Found these test classes: " + str(test_classes))
        return test_classes

    def get_test_classes(self, module):
        """Return list of any all classes in the module object."""
        module_objects = map(lambda x: getattr(module, x), [obj_name for obj_name in dir(module)])
        return filter(lambda x: self.is_test_class(x), module_objects)

    def is_test_class(self, obj):
        """An object is a test class if it's a leafy subclass of Test.
        """
        return inspect.isclass(obj) and issubclass(obj, Test) and len(obj.__subclasses__()) == 0


