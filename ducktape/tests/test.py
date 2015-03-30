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
import logging

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
