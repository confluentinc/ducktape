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

from .json import JsonCluster
import os
import json
import subprocess


class VagrantCluster(JsonCluster):
    """
    An implementation of Cluster that uses a set of VMs created by Vagrant. Because we need hostnames that can be
    advertised, this assumes that the Vagrant VM's name is a routeable hostname on all the hosts.

    - If cluster_file is specified in the constructor's kwargs (i.e. passed via command line argument --cluster-file)
      - If cluster_file exists on the filesystem, read cluster info from the file
      - Otherwise, retrieve cluster info via "vagrant ssh-config" from vagrant and write cluster info to cluster_file
    - Otherwise, retrieve cluster info via "vagrant ssh-config" from vagrant
    """

    def __init__(self, *args, **kwargs):
        self._is_aws = None
        is_read_from_file = False

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
                "nodes": self._get_nodes_from_vagrant()
            }

        super(VagrantCluster, self).__init__(cluster_json)

        # If cluster file is specified but the cluster info is not read from it, write the cluster info into the file
        if not is_read_from_file and cluster_file is not None:
            nodes = [{"hostname": node_account.hostname,
                      "ssh_hostname": node_account.ssh_hostname,
                      "user": node_account.user,
                      "ssh_args": node_account.ssh_args,
                      "externally_routable_ip": node_account.externally_routable_ip}
                      for node_account in self.available_nodes]
            cluster_json["nodes"] = nodes
            json.dump(cluster_json, open(cluster_file, 'w+'), indent=2, separators=(',', ': '), sort_keys=True)

    def _get_nodes_from_vagrant(self):
        nodes = []
        hostname, ssh_hostname, username, flags = None, None, None, ""
        # Parse ssh-config info on each running vagrant virtual machine into json
        (ssh_config_info, error) = self._vagrant_ssh_config()
        for line in ssh_config_info.split("\n"):
            line = line.strip()
            if len(line.strip()) == 0:
                if hostname is not None:
                    nodes.append({
                        "hostname": hostname,
                        "ssh_hostname": ssh_hostname,
                        "user": username,
                        "ssh_args": flags,
                    })
                    hostname, ssh_hostname, username, flags = None, None, None, ""
                continue
            try:
                key, val = line.split()
            except ValueError:
                # Sometimes Vagrant includes extra messages in the output that need to be ignored
                continue
            if key == "Host":
                hostname = val
            elif key == "HostName":
                # This needs to be handled carefully because of the way SSH in Vagrant is setup. We don't want to rely
                # on the Vagrant VM's hostname (e.g. 'worker1') having been added to the driver host's /etc/hosts file.
                # This is why we use the output of 'vagrant ssh-config'. But that means we need to distinguish between
                # the hostname and the value of hostname we use for SSH commands. We try to satisfy all use cases and
                # keep things simple by a) storing the hostname the user probably expects above (the "Host" branch), b)
                # saving the real value we use for running the SSH command in a place that's accessible and c) including
                # the HostName as an SSH option, which overrides the name specified on the command line. The last part
                # means that running ssh vagrant@worker1 -o 'HostName 127.0.0.1' -o 'Port 2222' will actually use
                # 127.0.0.1 instead of worker1 as the hostname, but we'll be able to use the hostname worker1 pretty
                # much everywhere else.
                ssh_hostname = val
                flags += "-o '" + line + "' "
            elif key == "User":
                username = val
            else:
                flags += "-o '" + line + "' "
        return nodes

    def _vagrant_ssh_config(self):
        return subprocess.Popen("vagrant ssh-config", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                close_fds=True).communicate()

    @property
    def is_aws(self):
        """Heuristic to detect whether the slave nodes are local or aws.

        Return true if they are running on aws.
        """
        if self._is_aws is None:
            proc = subprocess.Popen("vagrant status", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    close_fds=True)
            output, _ = proc.communicate()
            self._is_aws = output.find("aws") >= 0
        return self._is_aws
    def _externally_routable_ip(self, node_account):
        if self.is_aws:
            cmd = "/sbin/ifconfig eth0 "
        else:
            cmd = "/sbin/ifconfig eth1 "
        cmd += "| grep 'inet addr' | tail -n 1 | egrep -o '[0-9\.]+' | head -n 1 2>&1"

        output = "".join(node_account.ssh_capture(cmd))
        return output.strip()

