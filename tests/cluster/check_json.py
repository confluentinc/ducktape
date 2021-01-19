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

from ducktape.cluster.json import JsonCluster
from ducktape.cluster.node_container import InsufficientResourcesError
from ducktape.services.service import Service
import pickle
import pytest


class CheckJsonCluster(object):
    single_node_cluster_json = {
        "nodes": [
            {
                "ssh_config": {"host": "localhost"}
            }
        ]
    }

    def check_invalid_json(self):
        # Missing list of nodes
        with pytest.raises(ValueError):
            JsonCluster(cluster_json={}, is_type_based=False)

        # Missing hostname, which is required
        with pytest.raises(ValueError):
            JsonCluster(cluster_json={"nodes": [{}]}, is_type_based=False)

    @staticmethod
    def cluster_hostnames(nodes):
        return set([node.account.hostname for node in nodes])

    def check_cluster_size(self):
        cluster = JsonCluster(cluster_json={"nodes": []}, is_type_based=False)
        assert len(cluster) == 0

        n = 10
        cluster = JsonCluster(
            cluster_json={
                "nodes": [
                    {"ssh_config": {"host": "localhost%d" % x}} for x in range(n)]},
            is_type_based=False)

        assert len(cluster) == n

    def check_pickleable(self):
        cluster = JsonCluster(
            cluster_json={
                "nodes": [
                    {"ssh_config": {"host": "localhost1"}},
                    {"ssh_config": {"host": "localhost2"}},
                    {"ssh_config": {"host": "localhost3"}}]},
            is_type_based=False)

        pickle.dumps(cluster)

    def check_allocate_free(self):
        cluster = JsonCluster(
            cluster_json = {
                "nodes": [
                    {"ssh_config": {"host": "localhost1"}},
                    {"ssh_config": {"host": "localhost2"}},
                    {"ssh_config": {"host": "localhost3"}}]},
            is_type_based=False)

        assert len(cluster) == 3
        assert(cluster.num_available_nodes() == 3)

        nodes = cluster.alloc(Service.setup_cluster_spec(num_nodes=1))
        nodes_hostnames = self.cluster_hostnames(nodes)
        assert len(cluster) == 3
        assert(cluster.num_available_nodes() == 2)

        nodes2 = cluster.alloc(Service.setup_cluster_spec(num_nodes=2))
        nodes2_hostnames = self.cluster_hostnames(nodes2)
        assert len(cluster) == 3
        assert(cluster.num_available_nodes() == 0)

        assert(nodes_hostnames.isdisjoint(nodes2_hostnames))

        cluster.free(nodes)
        assert(cluster.num_available_nodes() == 1)

        cluster.free(nodes2)
        assert(cluster.num_available_nodes() == 3)

    def check_parsing(self):
        """ Checks that RemoteAccounts are generated correctly from input JSON"""

        node = JsonCluster(
            cluster_json={
                "nodes": [
                    {"ssh_config": {"host": "hostname"}}]},
            is_type_based=False).alloc(Service.setup_cluster_spec(num_nodes=1))[0]

        assert node.account.hostname == "hostname"
        assert node.account.user is None

        ssh_config = {
            "host": "hostname",
            "user": "user",
            "hostname": "localhost",
            "port": 22
        }
        node = JsonCluster(
            cluster_json={
                "nodes": [{"hostname": "hostname",
                                       "user": "user",
                                       "ssh_config": ssh_config}]}, 
            is_type_based=False).alloc(Service.setup_cluster_spec(num_nodes=1))[0]

        assert node.account.hostname == "hostname"
        assert node.account.user == "user"

        # check ssh configs
        assert node.account.ssh_config.host == "hostname"
        assert node.account.ssh_config.user == "user"
        assert node.account.ssh_config.hostname == "localhost"
        assert node.account.ssh_config.port == 22

    def check_exhausts_supply(self):
        cluster = JsonCluster(cluster_json=self.single_node_cluster_json, is_type_based=False)
        with pytest.raises(InsufficientResourcesError):
            cluster.alloc(Service.setup_cluster_spec(num_nodes=2))

    def check_node_names(self):
        cluster = JsonCluster(
            cluster_json={
                "nodes": [
                    {"ssh_config": {"host": "localhost1"}},
                    {"ssh_config": {"host": "localhost2"}},
                    {"ssh_config": {"host": "localhost3"}}]},
            is_type_based=False)
        hosts = set(["localhost1", "localhost2", "localhost3"])
        nodes = cluster.alloc(cluster.available())
        assert hosts == set(node.name for node in nodes)
