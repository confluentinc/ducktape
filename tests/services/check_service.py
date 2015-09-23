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

from ducktape.services.service import Service
from tests.ducktape_mock import test_context, session_context
from ducktape.cluster.localhost import LocalhostCluster


class DummyService(Service):
    """Simple fake service class."""

    def __init__(self, context, num_nodes):
        super(DummyService, self).__init__(context, num_nodes)

    def who_am_i(self, node=None):
        return "DummyService"

    def idx(self, node):
        return 1


class CheckAllocateFree(object):

    def setup_method(self, method):
        self.cluster = LocalhostCluster()
        self.session_context = session_context(cluster=self.cluster)
        self.context = test_context(self.session_context)

    def check_allocate_free(self):
        """Check that allocating and freeing nodes works.

        This regression test catches the error with Service.free() introduced in v0.3.3 and fixed in v0.3.4
        """

        # Node allocation takes place during service instantiation
        initial_cluster_size = len(self.cluster)
        self.service = DummyService(self.context, 10)
        assert self.cluster.num_available_nodes() == initial_cluster_size - 10

        self.service.free()
        assert self.cluster.num_available_nodes() == initial_cluster_size


