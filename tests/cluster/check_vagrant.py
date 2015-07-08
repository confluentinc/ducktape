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

    def check_one_host_parsing(self, monkeypatch):
        self._vagrant_ssh_data = self.two_hosts
        monkeypatch.setattr("ducktape.cluster.vagrant.VagrantCluster._vagrant_ssh_config", lambda vc: (self._vagrant_ssh_data, None))
        monkeypatch.setattr("ducktape.cluster.vagrant.VagrantCluster._is_aws", lambda vc: False)
        monkeypatch.setattr("ducktape.cluster.vagrant.VagrantCluster._externally_routable_ip", lambda vc, is_aws, node_account: "127.0.0.1")


        cluster = VagrantCluster()
        assert len(cluster) == 2
        assert(cluster.num_available_nodes() == 2)
        node1, node2 = cluster.request(2)

        assert(node1.account.hostname == "worker1")
        assert(node1.account.user == "vagrant")
        assert(node1.account.ssh_args.strip() == "-o 'HostName 127.0.0.1' -o 'Port 2222' -o 'UserKnownHostsFile /dev/null' -o 'StrictHostKeyChecking no' -o 'PasswordAuthentication no' -o 'IdentityFile /Users/foo/ducktape.git/.vagrant/machines/worker1/virtualbox/private_key' -o 'IdentitiesOnly yes' -o 'LogLevel FATAL'")
        assert(node1.account.ssh_hostname == '127.0.0.1')

        assert(node2.account.hostname == "worker2")
        assert(node2.account.user == "vagrant")
        assert(node2.account.ssh_args.strip() == "-o 'HostName 127.0.0.2' -o 'Port 2200' -o 'UserKnownHostsFile /dev/null' -o 'StrictHostKeyChecking no' -o 'PasswordAuthentication no' -o 'IdentityFile /Users/foo/ducktape.git/.vagrant/machines/worker2/virtualbox/private_key' -o 'IdentitiesOnly yes' -o 'LogLevel FATAL'")
        assert(node2.account.ssh_hostname == '127.0.0.2')
