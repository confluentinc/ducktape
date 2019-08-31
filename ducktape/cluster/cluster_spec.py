# Copyright 2017 Confluent Inc.
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

import json
import re

from ducktape.cluster.node_container import NodeContainer
from ducktape.cluster.remoteaccount import MachineType

LINUX = "linux"

WINDOWS = "windows"

SUPPORTED_OS_TYPES = [LINUX, WINDOWS]


class NodeSpec(object):
    """
    The specification for a ducktape cluster node.

    :param operating_system:    The operating system of the node.
    :param machine_type:        The machine type of the node including required resource.
    """
    def __init__(self, operating_system=LINUX, machine_type=None):
        self.operating_system = operating_system
        if self.operating_system not in SUPPORTED_OS_TYPES:
            raise RuntimeError("Unsupported os type %s" % self.operating_system)
        self.machine_type = machine_type or MachineType()

    def __str__(self):
        dict = {
            "os": self.operating_system,
            "cpu": self.machine_type.cpu_core,
            "mem": self.machine_type.mem_size_gb,
            "disk": self.machine_type.disk_size_gb,
            "additional_disks": self.machine_type.additional_disks
        }
        return json.dumps(dict, sort_keys=True)


class ClusterSpec(object):
    """
    The specification for a ducktape cluster.
    """

    @staticmethod
    def empty():
        return ClusterSpec([])

    @staticmethod
    def simple_linux(num_nodes):
        """
        Create a ClusterSpec containing some simple Linux nodes.
        """
        node_specs_dict = {'os': LINUX, 'num_nodes': num_nodes}
        return ClusterSpec.from_dict(node_specs_dict)

    @staticmethod
    def from_dict(node_specs_dict):
        """
        Create ClusterSpec from a dict of nodes specifics. Operation system defaults to
        'linux'. Number of nodes default to 1.
        e.g. {'os':'linux', 'cpu':2, 'mem':'4GB', 'disk':'30GB', 'additional_disks':{'/dev/sdb':'100GB'}}

        :param node_specs_dict: The dictionary of node specifics
        :return: ClusterSpec
        """
        os = node_specs_dict.get('os', LINUX)
        cpu_core = node_specs_dict.get('cpu')
        mem_size = node_specs_dict.get('mem')
        disk_size = node_specs_dict.get('disk')
        addl_disks = node_specs_dict.get('additional_disks', {})
        addl_disks_gb = {d: ClusterSpec.to_gigabyte(d_size) for d, d_size in addl_disks.iteritems()}
        num_nodes = node_specs_dict.get('num_nodes', 1)
        return ClusterSpec([NodeSpec(os, MachineType(cpu_core, ClusterSpec.to_gigabyte(mem_size),
                                     ClusterSpec.to_gigabyte(disk_size), addl_disks_gb)) for _ in range(num_nodes)])

    @staticmethod
    def from_list(node_specs_dict_list):
        """
        Create a ClusterSpec from a list of nodes specifics dictionaries.
        e.g. [{'cpu':1, 'mem':'500MB', 'disk':'10GB'},
              {'cpu':2, 'mem':'4GB', 'disk':'30GB', 'num_nodes':2}]

        :param node_specs_dict_list: The list of node specifics dictionaries
        :return: ClusterSpec
        """
        node_specs = []
        for node_specs_dict in node_specs_dict_list:
            cluster_spec = ClusterSpec.from_dict(node_specs_dict)
            node_specs += cluster_spec.nodes
        return ClusterSpec.from_nodes(node_specs)

    @staticmethod
    def to_gigabyte(size):
        """
        Return number of gigabytes parsing from size.

        :param size: The string representation of size in format of <number+[TB|T|GB|G|MB|M|KB|K]>
        :return: number of gigabytes
        """
        unit_definitions = {'kb': 1024, 'k': 1024,
                            'mb': 1024 ** 2, 'm': 1024 ** 2,
                            'gb': 1024 ** 3, 'g': 1024 ** 3,
                            'tb': 1024 ** 4, 't': 1024 ** 4}
        m = re.match(r"(\d*\.?\d+|\d+)\s*(\w+)", size.lower(), re.I)
        number = m.group(1)
        unit = m.group(2)
        num_bytes = float(number) * unit_definitions[unit]
        return num_bytes / unit_definitions['gb']

    @staticmethod
    def from_nodes(nodes):
        """
        Create a ClusterSpec describing a list of nodes.
        """
        return ClusterSpec([NodeSpec(node.operating_system, node.machine_type) for node in nodes])

    def __init__(self, nodes=None):
        """
        Initialize the ClusterSpec.

        :param nodes:           A collection of NodeSpecs, or None to create an empty cluster spec.
        """
        self.nodes = NodeContainer(nodes)

    def __len__(self):
        return self.size()

    def __iter__(self):
        return self.nodes.elements()

    def size(self):
        """Return the total size of this cluster spec, including all types of nodes."""
        return self.nodes.size()

    def add(self, other):
        """
        Add another ClusterSpec to this one.

        :param node_spec:       The other cluster spec.  This will not be modified.
        :return:                This ClusterSpec.
        """
        for node_spec in other.nodes:
            self.nodes.add_node(node_spec)
        return self

    def clone(self):
        """
        Returns a deep copy of this object.
        """
        return ClusterSpec(self.nodes.clone())

    def __str__(self):
        node_spec_to_num = {}
        for node_spec in self.nodes.elements():
            node_spec_str = str(node_spec)
            node_spec_to_num[node_spec_str] = node_spec_to_num.get(node_spec_str, 0) + 1
        rval = []
        for node_spec_str in sorted(node_spec_to_num.keys()):
            node_spec = json.loads(node_spec_str)
            node_spec["num_nodes"] = node_spec_to_num[node_spec_str]
            rval.append(node_spec)
        return json.dumps(rval, sort_keys=True)
