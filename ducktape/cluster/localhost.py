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
import sys


class LocalhostCluster(Cluster):
    """
    A "cluster" that runs entirely on localhost using default credentials. This doesn't require any user
    configuration and is equivalent to the old defaults in cluster_config.json. There are no constraints
    on the resources available.
    """

    def __init__(self, *args, **kwargs):
        # Use a very large number, but fixed value so accounting for # of available nodes works
        self._available = sys.maxint

    def __len__(self):
        return sys.maxint

    def request(self, nslots):
        self._available -= nslots
        return [ClusterSlot(self, RemoteAccount("localhost")) for i in range(nslots)]

    def num_available_nodes(self):
        return self._available

    def free_single(self, slot):
        self._available += 1
