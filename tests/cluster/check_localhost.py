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

from ducktape.cluster.localhost import LocalhostCluster
from ducktape.services.service import Service

import pickle


class CheckLocalhostCluster(object):
    def setup_method(self, _):
        self.cluster = LocalhostCluster()

    def check_size(self):
        len(self.cluster) >= 2 ** 31 - 1

    def check_pickleable(self):
        cluster = LocalhostCluster()
        pickle.dumps(cluster)

    def check_request_free(self):
        available = self.cluster.num_available_nodes()
        initial_size = len(self.cluster)

        # Should be able to allocate arbitrarily many nodes
        nodes = self.cluster.alloc(Service.setup_cluster_spec(num_nodes=100))
        assert len(nodes) == 100
        for i, node in enumerate(nodes):
            assert node.account.hostname == 'localhost%d' % i
            assert node.account.ssh_hostname == 'localhost'
            assert node.account.ssh_config.hostname == 'localhost'
            assert node.account.ssh_config.port == 22
            assert node.account.user is None

        assert self.cluster.num_available_nodes() == (available - 100)
        assert len(self.cluster) == initial_size  # This shouldn't change

        self.cluster.free(nodes)

        assert self.cluster.num_available_nodes() == available
