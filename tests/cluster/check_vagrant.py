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
import json
import os

class CheckVagrantCluster(object):
    two_hosts = """Host worker1
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

    cluster_file = "cluster_file_temporary.json"

    def _set_monkeypatch_attr(self, monkeypatch):
        monkeypatch.setattr("ducktape.cluster.vagrant.VagrantCluster._vagrant_ssh_config", lambda vc: (self.two_hosts, None))
        monkeypatch.setattr("ducktape.cluster.vagrant.VagrantCluster.is_aws", lambda vc: False)
        monkeypatch.setattr("ducktape.cluster.vagrant.VagrantCluster._externally_routable_ip", lambda vc, node_account: "127.0.0.1")

    def check_one_host_parsing(self, monkeypatch):
        """check the behavior of VagrantCluster when cluster_file is not specified. VagrantCluster should read
        cluster information from _vagrant_ssh_config().
        """
        self._set_monkeypatch_attr(monkeypatch)

        cluster = VagrantCluster()
        assert len(cluster) == 2
        assert cluster.num_available_nodes() == 2
        node1, node2 = cluster.request(2)

        assert node1.account.hostname == "worker1"
        assert node1.account.user == "vagrant"
        assert node1.account.ssh_args.strip() == "-o 'HostName 127.0.0.1' -o 'Port 2222' -o 'UserKnownHostsFile /dev/null' -o 'StrictHostKeyChecking no' -o 'PasswordAuthentication no' -o 'IdentityFile /Users/foo/ducktape.git/.vagrant/machines/worker1/virtualbox/private_key' -o 'IdentitiesOnly yes' -o 'LogLevel FATAL'"
        assert node1.account.ssh_hostname == '127.0.0.1'

        assert node2.account.hostname == "worker2"
        assert node2.account.user == "vagrant"
        assert node2.account.ssh_args.strip() == "-o 'HostName 127.0.0.2' -o 'Port 2200' -o 'UserKnownHostsFile /dev/null' -o 'StrictHostKeyChecking no' -o 'PasswordAuthentication no' -o 'IdentityFile /Users/foo/ducktape.git/.vagrant/machines/worker2/virtualbox/private_key' -o 'IdentitiesOnly yes' -o 'LogLevel FATAL'"
        assert node2.account.ssh_hostname == '127.0.0.2'

    def check_cluster_file_write(self, monkeypatch):
        """check the behavior of VagrantCluster when cluster_file is specified but the file doesn't exist. VagrantCluster
        should read cluster information from _vagrant_ssh_config() and write the information to cluster_file.
        """
        self._set_monkeypatch_attr(monkeypatch)
        assert not os.path.exists(self.cluster_file)

        cluster = VagrantCluster(cluster_file=self.cluster_file)
        cluster_json_expected = {}
        nodes = [{"hostname": node_account.hostname,
                  "ssh_hostname": node_account.ssh_hostname,
                  "user": node_account.user,
                  "ssh_args": node_account.ssh_args,
                  "externally_routable_ip": node_account.externally_routable_ip}
                  for node_account in cluster.available_nodes]
        cluster_json_expected["nodes"] = nodes
        cluster_json_actual = json.load(open(os.path.abspath(self.cluster_file)))

        os.remove(self.cluster_file)
        assert cluster_json_actual == cluster_json_expected

    def check_cluster_file_read(self, monkeypatch):
        """check the behavior of VagrantCluster when cluster_file is specified and the file exists. VagrantCluster should
        read cluster information from cluster_file.
        """
        self._set_monkeypatch_attr(monkeypatch)

        # To verify that VagrantCluster reads cluster information from the cluster_file, the
        # content in the file is intentionally made different from that returned by _vagrant_ssh_config().
        nodes = []
        nodes.append({
            "hostname": "worker2",
            "ssh_hostname": "127.0.0.2",
            "user": "vagrant",
            "ssh_args": "-o 'HostName 127.0.0.2' -o 'Port 2222' -o 'UserKnownHostsFile /dev/null' -o 'StrictHostKeyChecking no' -o 'PasswordAuthentication no' -o 'IdentityFile /Users/foo/ducktape.git/.vagrant/machines/worker2/virtualbox/private_key' -o 'IdentitiesOnly yes' -o 'LogLevel FATAL'",
            "externally_routable_ip": "127.0.0.2"
        })
        nodes.append({
            "hostname": "worker3",
            "ssh_hostname": "127.0.0.3",
            "user": "vagrant",
            "ssh_args": "-o 'HostName 127.0.0.3' -o 'Port 2223' -o 'UserKnownHostsFile /dev/null' -o 'StrictHostKeyChecking no' -o 'PasswordAuthentication no' -o 'IdentityFile /Users/foo/ducktape.git/.vagrant/machines/worker3/virtualbox/private_key' -o 'IdentitiesOnly yes' -o 'LogLevel FATAL'",
            "externally_routable_ip": "127.0.0.3"
        })

        cluster_json_expected = {}
        cluster_json_expected["nodes"] = nodes
        json.dump(cluster_json_expected, open(self.cluster_file, 'w+'), indent=2, separators=(',', ': '), sort_keys=True)

        cluster = VagrantCluster(cluster_file=self.cluster_file)
        os.remove(self.cluster_file)

        assert len(cluster) == 2
        assert cluster.num_available_nodes() == 2
        node1, node2 = cluster.request(2)

        assert node1.account.hostname == "worker2"
        assert node1.account.user == "vagrant"
        assert node1.account.ssh_args.strip() == "-o 'HostName 127.0.0.2' -o 'Port 2222' -o 'UserKnownHostsFile /dev/null' -o 'StrictHostKeyChecking no' -o 'PasswordAuthentication no' -o 'IdentityFile /Users/foo/ducktape.git/.vagrant/machines/worker2/virtualbox/private_key' -o 'IdentitiesOnly yes' -o 'LogLevel FATAL'"
        assert node1.account.ssh_hostname == '127.0.0.2'

        assert node2.account.hostname == "worker3"
        assert node2.account.user == "vagrant"
        assert node2.account.ssh_args.strip() == "-o 'HostName 127.0.0.3' -o 'Port 2223' -o 'UserKnownHostsFile /dev/null' -o 'StrictHostKeyChecking no' -o 'PasswordAuthentication no' -o 'IdentityFile /Users/foo/ducktape.git/.vagrant/machines/worker3/virtualbox/private_key' -o 'IdentitiesOnly yes' -o 'LogLevel FATAL'"
        assert node2.account.ssh_hostname == '127.0.0.3'


