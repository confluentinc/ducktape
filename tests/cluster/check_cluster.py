# Copyright 2016 Confluent Inc.
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


from ducktape.cluster.cluster import ClusterSlot
from ducktape.cluster.remoteaccount import RemoteAccount

import logging


class CheckClusterSlot(object):
    def check_cluster_slot_equality(self):
        """Different cluster slots intantiated with same parameters should be equal."""
        kwargs = {
            "hostname": "hello",
            "user": "vagrant",
            "ssh_args": "asdf",
            "ssh_hostname": "123",
            "externally_routable_ip": "345",
            "logger": logging.getLogger(__name__)
        }
        r1 = RemoteAccount(**kwargs)
        r2 = RemoteAccount(**kwargs)

        c1 = ClusterSlot(account=r1)
        c2 = ClusterSlot(account=r2)

        assert c1 == c2
