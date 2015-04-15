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


class Test(object):
    """Base class for tests.
    """
    def __init__(self, test_context):
        """
        :type test_context: ducktape.tests.session_context.TestContext
        """
        self.cluster = test_context.session_context.cluster
        self.test_context = test_context
        self.logger = test_context.logger

    def log_start(self):
        self.logger.info("Running test %s")

    def min_cluster_size(self):
        """
        Subclasses implement this to provide a helpful heuristic to prevent trying to run a test on a cluster
        with too few nodes.
        """
        raise NotImplementedError("All tests must implement this method.")



