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

from .json import JsonCluster, make_remote_account
import json
import os
from .remoteaccount import RemoteAccountSSHConfig
import subprocess
from ducktape.json_serializable import DucktapeJSONEncoder


class VagrantCluster(JsonCluster):
    """
    An implementation of Cluster that uses a set of VMs created by Vagrant. Because we need hostnames that can be
    advertised, this assumes that the Vagrant VM's name is a routeable hostname on all the hosts.

    - If cluster_file is specified in the constructor's kwargs (i.e. passed via command line argument --cluster-file)
      - If cluster_file exists on the filesystem, read cluster info from the file
      - Otherwise, retrieve cluster info via "vagrant ssh-config" from vagrant and write cluster info to cluster_file
    - Otherwise, retrieve cluster info via "vagrant ssh-config" from vagrant
    """

    def __init__(self, *args, make_remote_account_func=make_remote_account, **kwargs):
        is_read_from_file = False
        self.ssh_exception_checks = kwargs.get("ssh_exception_checks")
        cluster_file = kwargs.get("cluster_file")
        if cluster_file is not None:
            try:
                cluster_json = json.load(open(os.path.abspath(cluster_file)))
                is_read_from_file = True
            except IOError:
                # It is OK if file is not found. Call vagrant ssh-info to read the cluster info.
                pass

        if not is_read_from_file:
            cluster_json = {
                "nodes": self._get_nodes_from_vagrant(make_remote_account_func)
            }

        super(VagrantCluster, self).__init__(
            cluster_json, *args, make_remote_account_func=make_remote_account_func, **kwargs)

        # If cluster file is specified but the cluster info is not read from it, write the cluster info into the file
        if not is_read_from_file and cluster_file is not None:
            nodes = [
                {
                    "ssh_config": node_account.ssh_config,
                    "externally_routable_ip": node_account.externally_routable_ip
                }
                for node_account in self._available_accounts
            ]
            cluster_json["nodes"] = nodes
            with open(cluster_file, 'w+') as fd:
                json.dump(cluster_json, fd, cls=DucktapeJSONEncoder, indent=2, separators=(',', ': '), sort_keys=True)

        # Release any ssh clients used in querying the nodes for metadata
        for node_account in self._available_accounts:
            node_account.close()

    def _get_nodes_from_vagrant(self, make_remote_account_func):
        ssh_config_info, error = self._vagrant_ssh_config()

        nodes = []
        node_info_arr = ssh_config_info.split("\n\n")
        node_info_arr = [ninfo.strip() for ninfo in node_info_arr if ninfo.strip()]

        for ninfo in node_info_arr:
            ssh_config = RemoteAccountSSHConfig.from_string(ninfo)

            account = None
            try:
                account = make_remote_account_func(ssh_config, ssh_exception_checks=self.ssh_exception_checks)
                externally_routable_ip = account.fetch_externally_routable_ip()
            finally:
                if account:
                    account.close()
                    del account

            nodes.append({
                "ssh_config": ssh_config.to_json(),
                "externally_routable_ip": externally_routable_ip
            })

        return nodes

    def _vagrant_ssh_config(self):
        ssh_config_info, error = subprocess.Popen("vagrant ssh-config", shell=True, stdout=subprocess.PIPE,
                                                  stderr=subprocess.PIPE, close_fds=True,
                                                  # Force to text mode in py2/3 compatible way
                                                  universal_newlines=True).communicate()
        return ssh_config_info, error
