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

from .cluster import Cluster, ClusterSlot
from .remoteaccount import RemoteAccount

import collections, json, os, os.path


class JsonCluster(Cluster):
    """
    An implementation of Cluster that uses static settings specified in a cluster file.
    """

    def __init__(self, cluster_json=None):
        super(JsonCluster, self).__init__()
        if cluster_json is None:
            cluster_json_path = os.path.abspath(os.path.join(os.getcwd(), "cluster.json"))
            cluster_json = json.load(open(cluster_json_path))
        init_nodes = [RemoteAccount(ninfo["hostname"], ninfo.get("user"), ninfo.get("ssh_args"),
                                    ninfo.get("java_home", "default"), ninfo.get("kafka_home", "default"))
                                    for ninfo in cluster_json["nodes"]]
        self.available_nodes = collections.deque(init_nodes)
        self.in_use_nodes = set()
        self.id_source = 1

    def num_available_nodes(self):
        return len(self.available_nodes)

    def request(self, nslots):
        if nslots > self.num_available_nodes():
            raise RuntimeError(
                "There aren't enough available nodes to satisfy the resource request. Your test has almost " +
                "certainly incorrectly implemented its min_cluster_size() method.")

        result = []
        for i in range(nslots):
            node = self.available_nodes.popleft()
            result.append(ClusterSlot(self, node, slot_id=self.id_source))
            self.in_use_nodes.add(self.id_source)
            self.id_source += 1
        return result

    def free_single(self, slot):
        assert(slot.slot_id in self.in_use_nodes)
        self.in_use_nodes.remove(slot.slot_id)
        self.available_nodes.append(slot.account)

Cluster._FACTORY["json"]      = JsonCluster
