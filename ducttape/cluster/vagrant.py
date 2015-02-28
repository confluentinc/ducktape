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
        hostname, username, flags = None, None, ""
        nodes = []

        # Parse ssh-config info on each vagrant virtual machine into json
        for line in (subprocess.check_output("vagrant ssh-config", shell=True)).split("\n"):
            line = line.strip()
            if len(line.strip()) == 0:
                if hostname is not None:
                    nodes.append({
                        "hostname": hostname,
                        "user": username,
                        "ssh_args": flags,
                        # java_home is determined automatically, but we need to explicitly indicate that should be
                        # the case instead of using "default"
                        "java_home": None,
                        "kafka_home": "/opt/kafka",
                    })
                    hostname, username, flags = None, None, ""
                continue
            try:
                key, val = line.split()
            except ValueError:
                # Sometimes Vagrant includes extra messages in the output that need to be ignore
                continue
            if key == "Host":
                hostname = val
            elif key == "Hostname":
                # Ignore since we use the Vagrant VM name
                pass
            elif key == "User":
                username = val
            else:
                flags += "-o '" + line + "' "

        cluster_json = {
            "nodes": nodes
        }

        super(VagrantCluster, self).__init__(cluster_json)

Cluster._FACTORY["vagrant"]   = VagrantCluster
