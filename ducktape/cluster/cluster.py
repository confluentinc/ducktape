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

import collections


class ClusterSlot(object):
    def __init__(self, parent, account, **kwargs):
        self.parent = parent
        self.account = account
        for k, v in kwargs.items():
            setattr(self, k, v)

    def free(self):
        self.parent.free(self)


class Cluster(object):
    """ Interface for a cluster -- a collection of nodes with login credentials.
    This interface doesn't define any mapping of roles/services to nodes. It only interacts with some underlying
    system that can describe available resources and mediates reservations of those resources. This is intentionally
    simple right now: the only "resource" right now is a generic VM and it is assumed everything is approximately
    homogeneous.
    """

    def __len__(self):
        """Size of this cluster object. I.e. number of 'nodes' in the cluster."""
        raise NotImplementedError()

    def request(self, nslots):
        """Request the specified number of slots, which will be reserved until they are freed by the caller."""
        raise NotImplementedError()

    def num_available_nodes(self):
        """Number of available slots."""
        raise NotImplementedError()

    def free(self, slots):
        """Free the given slot or list of slots"""
        if isinstance(slots, collections.Iterable):
            for s in slots:
                self.free_single(s)
        else:
            self.free_single(slots)

    def free_single(self, slot):
        raise NotImplementedError()
