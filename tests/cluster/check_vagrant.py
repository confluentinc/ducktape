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

from ducktape.cluster.vagrant import VagrantCluster
from ducktape.services.service import Service
import json
import pickle
import os
import random

TWO_HOSTS = """Host worker1
  HostName 127.0.0.1
  User vagrant
  Port 2222
  UserKnownHostsFile /dev/null
  StrictHostKeyChecking no
  PasswordAuthentication no
  IdentityFile /Users/foo/ducktape.git/.vagrant/machines/worker1/virtualbox/private_key
  IdentitiesOnly yes
  LogLevel FATAL

Host worker2
  HostName 127.0.0.2
  User vagrant
  Port 2200
  UserKnownHostsFile /dev/null
  StrictHostKeyChecking no
  PasswordAuthentication no
  IdentityFile /Users/foo/ducktape.git/.vagrant/machines/worker2/virtualbox/private_key
  IdentitiesOnly yes
  LogLevel FATAL

"""


class CheckVagrantCluster(object):

    def setup_method(self, _):
        # We roll our own tempfile name instead of using python tempfile module because
        # in some cases, we want self.cluster_file to be the name of a file which does not yet exist
        self.cluster_file = "cluster_file_temporary-%d.json" % random.randint(1, 2**63 - 1)
        if os.path.exists(self.cluster_file):
            os.remove(self.cluster_file)

    def teardown_method(self, _):
        if os.path.exists(self.cluster_file):
            os.remove(self.cluster_file)

    def _set_monkeypatch_attr(self, monkeypatch):
        monkeypatch.setattr("ducktape.cluster.vagrant.VagrantCluster._vagrant_ssh_config", lambda vc: (TWO_HOSTS, None))
        monkeypatch.setattr("ducktape.cluster.vagrant.VagrantCluster.is_aws", lambda vc: False)
        monkeypatch.setattr(
            "ducktape.cluster.linux_remoteaccount.LinuxRemoteAccount.fetch_externally_routable_ip",
            lambda vc, node_account: "127.0.0.1")

    def check_pickleable(self, monkeypatch):
        self._set_monkeypatch_attr(monkeypatch)
        cluster = VagrantCluster()
        pickle.dumps(cluster)

    def check_one_host_parsing(self, monkeypatch):
        """check the behavior of VagrantCluster when cluster_file is not specified. VagrantCluster should read
        cluster information from _vagrant_ssh_config().
        """
        self._set_monkeypatch_attr(monkeypatch)

        cluster = VagrantCluster()
        assert len(cluster) == 2
        assert cluster.num_available_nodes() == 2
        node1, node2 = cluster.alloc(Service.setup_cluster_spec(num_nodes=2))

        assert node1.account.hostname == "worker1"
        assert node1.account.user == "vagrant"
        assert node1.account.ssh_hostname == '127.0.0.1'

        assert node2.account.hostname == "worker2"
        assert node2.account.user == "vagrant"
        assert node2.account.ssh_hostname == '127.0.0.2'

    def check_cluster_file_write(self, monkeypatch):
        """check the behavior of VagrantCluster when cluster_file is specified but the file doesn't exist.
        VagrantCluster should read cluster information from _vagrant_ssh_config() and write the information to
        cluster_file.
        """
        self._set_monkeypatch_attr(monkeypatch)
        assert not os.path.exists(self.cluster_file)

        cluster = VagrantCluster(cluster_file=self.cluster_file)
        cluster_json_expected = {}
        nodes = [
            {
                "externally_routable_ip": node_account.externally_routable_ip,
                "ssh_config": {
                    "host": node_account.ssh_config.host,
                    "hostname": node_account.ssh_config.hostname,
                    "user": node_account.ssh_config.user,
                    "identityfile": node_account.ssh_config.identityfile,
                    "password": node_account.ssh_config.password,
                    "port": node_account.ssh_config.port
                }
            }
            for node_account in cluster._available_accounts
        ]

        cluster_json_expected["nodes"] = nodes

        cluster_json_actual = json.load(open(os.path.abspath(self.cluster_file)))
        assert cluster_json_actual == cluster_json_expected

    def check_cluster_file_read(self, monkeypatch):
        """check the behavior of VagrantCluster when cluster_file is specified and the file exists.
        VagrantCluster should read cluster information from cluster_file.
        """
        self._set_monkeypatch_attr(monkeypatch)

        # To verify that VagrantCluster reads cluster information from the cluster_file, the
        # content in the file is intentionally made different from that returned by _vagrant_ssh_config().
        nodes_expected = []
        node1_expected = {
            "externally_routable_ip": "127.0.0.3",
            "ssh_config": {
                "host": "worker3",
                "hostname": "127.0.0.3",
                "user": "vagrant",
                "port": 2222,
                "password": "password",
                "identityfile": "/path/to/identfile3"
            }
        }
        nodes_expected.append(node1_expected)

        node2_expected = {
            "externally_routable_ip": "127.0.0.2",
            "ssh_config": {
                "host": "worker2",
                "hostname": "127.0.0.2",
                "user": "vagrant",
                "port": 2223,
                "password": None,
                "identityfile": "/path/to/indentfile2"
            }
        }
        nodes_expected.append(node2_expected)

        cluster_json_expected = {}
        cluster_json_expected["nodes"] = nodes_expected
        json.dump(cluster_json_expected, open(self.cluster_file, 'w+'),
                  indent=2, separators=(',', ': '), sort_keys=True)

        # Load the cluster from the json file we just created
        cluster = VagrantCluster(cluster_file=self.cluster_file)

        assert len(cluster) == 2
        assert cluster.num_available_nodes() == 2
        node2, node3 = cluster.alloc(Service.setup_cluster_spec(num_nodes=2))

        assert node3.account.hostname == "worker2"
        assert node3.account.user == "vagrant"
        assert node3.account.ssh_hostname == '127.0.0.2'
        assert node3.account.ssh_config.to_json() == node2_expected["ssh_config"]

        assert node2.account.hostname == "worker3"
        assert node2.account.user == "vagrant"
        assert node2.account.ssh_hostname == '127.0.0.3'
        assert node2.account.ssh_config.to_json() == node1_expected["ssh_config"]
