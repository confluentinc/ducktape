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
from typing import Optional

from .consts import LINUX, SUPPORTED_OS_TYPES


class NodeSpec(object):
    """
    Specification for a single cluster node.

    The node_type field is a generic label that can represent size, architecture,
    or any classification scheme defined by the cluster configuration. When None,
    it matches any available node (backward compatible behavior).

    :param operating_system:    The operating system of the node.
    :param node_type:           Node type label (e.g., "large", "small"). None means "match any".
    """

    def __init__(self, operating_system: str = LINUX, node_type: Optional[str] = None):
        self.operating_system = operating_system
        self.node_type = node_type
        if self.operating_system not in SUPPORTED_OS_TYPES:
            raise RuntimeError("Unsupported os type %s" % self.operating_system)

    def matches(self, available_node_spec: "NodeSpec") -> bool:
        """
        Check if this requirement can be satisfied by an available node.

        Matching rules:
            - OS must match exactly
            - If requested node_type is None, match any type
            - If requested node_type is specified, must match exactly

        :param available_node_spec: The specification of an available node
        :return: True if this requirement matches the available node
        """
        if self.operating_system != available_node_spec.operating_system:
            return False
        if self.node_type is None:
            return True  # Requestor doesn't care about type
        return self.node_type == available_node_spec.node_type

    def __str__(self):
        d = {"os": self.operating_system}
        if self.node_type is not None:
            d["node_type"] = self.node_type
        return json.dumps(d, sort_keys=True)

    def __eq__(self, other):
        if not isinstance(other, NodeSpec):
            return False
        return self.operating_system == other.operating_system and self.node_type == other.node_type

    def __hash__(self):
        return hash((self.operating_system, self.node_type))
