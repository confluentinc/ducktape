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

from __future__ import absolute_import

from ducktape.command_line.defaults import ConsoleDefaults
from .cluster import Cluster, ClusterSlot
from .remoteaccount import RemoteAccount, RemoteAccountSSHConfig

import collections
import json
import os
import traceback


class JsonCluster(Cluster):
    """An implementation of Cluster that uses static settings specified in a cluster file or json-serializeable dict
    """

    def __init__(self, cluster_json=None, *args, **kwargs):
        """Initialize JsonCluster

        JsonCluster can be initialized from:
            - a json-serializeable dict
            - a "cluster_file" containing json

        :param cluster_json: a json-serializeable dict containing node information.
            If cluster_json is None, load from file
        :param cluster_file (optional): Overrides the default location of the json cluster file

        Example json with a local Vagrant cluster:
        {
          "nodes": [
            {
              "externally_routable_ip": "192.168.50.151",

              "ssh_config": {
                "host": "worker1",
                "hostname": "127.0.0.1",
                "identityfile": "/path/to/private_key",
                "password": null,
                "port": 2222,
                "user": "vagrant"
              }
            },
            {
              "externally_routable_ip": "192.168.50.151",

              "ssh_config": {
                "host": "worker2",
                "hostname": "127.0.0.1",
                "identityfile": "/path/to/private_key",
                "password": null,
                "port": 2223,
                "user": "vagrant"
              }
            }
          ]
        }
        """
        super(JsonCluster, self).__init__()
        if cluster_json is None:
            # This is a directly instantiation of JsonCluster rather than from a subclass (e.g. VagrantCluster)
            cluster_file = kwargs.get("cluster_file")
            if cluster_file is None:
                cluster_file = ConsoleDefaults.CLUSTER_FILE
            cluster_json = json.load(open(os.path.abspath(cluster_file)))
        try:
            node_accounts = []
            for ninfo in cluster_json["nodes"]:
                ssh_config_dict = ninfo.get("ssh_config")
                assert ssh_config_dict is not None, \
                    "Cluster json has a node without an ssh_config field: %s\n Cluster json: %s" % (ninfo, cluster_json)

                ssh_config = RemoteAccountSSHConfig(**ninfo.get("ssh_config", {}))
                node_accounts.append(
                    RemoteAccount(
                        ssh_config=ssh_config,
                        externally_routable_ip=ninfo.get("externally_routable_ip")))

            for node_account in node_accounts:
                if node_account.externally_routable_ip is None:
                    node_account.externally_routable_ip = self._externally_routable_ip(node_account)

        except BaseException as e:
            msg = "JSON cluster definition invalid: %s: %s" % (e, traceback.format_exc(limit=16))
            raise ValueError(msg)

        self.available_nodes = collections.deque(node_accounts)
        self.in_use_nodes = set()
        self._id_supplier = 0

    def __len__(self):
        return len(self.available_nodes) + len(self.in_use_nodes)

    def num_available_nodes(self):
        return len(self.available_nodes)

    def alloc(self, num_nodes):
        if num_nodes > self.num_available_nodes():
            err_msg = "There aren't enough available nodes to satisfy the resource request. " \
                "Total cluster size: %d, Requested: %d, Already allocated: %d, Available: %d. " % \
                      (len(self), num_nodes, len(self.in_use_nodes), self.num_available_nodes())
            err_msg += "Make sure your cluster has enough nodes to run your test or service(s)."
            raise RuntimeError(err_msg)

        result = []
        for i in range(num_nodes):
            node = self.available_nodes.popleft()
            cluster_slot = ClusterSlot(node, slot_id=self._id_supplier)
            result.append(cluster_slot)
            self.in_use_nodes.add(node)
            self._id_supplier += 1

        return result

    def free_single(self, slot):
        assert(slot.account in self.in_use_nodes)
        slot.account.close()
        self.in_use_nodes.remove(slot.account)
        self.available_nodes.append(slot.account)

    def _externally_routable_ip(self, account):
        return None
