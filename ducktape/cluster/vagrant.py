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
from .json import JsonCluster

import subprocess

class VagrantCluster(JsonCluster):
    """
    An implementation of Cluster that uses a set of VMs created by Vagrant. Because we need hostnames that can be
    advertised, this assumes that the Vagrant VM's name is a routeable hostname on all the hosts.
    """

    def __init__(self):
        hostname, ssh_hostname, username, flags = None, None, None, ""
        nodes = []

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

        cluster_json = {
            "nodes": nodes
        }

        super(VagrantCluster, self).__init__(cluster_json)

    def _vagrant_ssh_config(self):
        return subprocess.Popen("vagrant ssh-config", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

Cluster._FACTORY["vagrant"]   = VagrantCluster
