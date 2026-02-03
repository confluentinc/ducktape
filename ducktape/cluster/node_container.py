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
from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from ducktape.cluster.cluster_node import ClusterNode
from ducktape.cluster.remoteaccount import RemoteAccount

if TYPE_CHECKING:
    from ducktape.cluster.cluster_spec import ClusterSpec
    from ducktape.cluster.node_spec import NodeSpec

NodeType = Union[ClusterNode, RemoteAccount]
# Key for node grouping: (operating_system, node_type)
NodeGroupKey = Tuple[Optional[str], Optional[str]]


class NodeNotPresentError(Exception):
    pass


class InsufficientResourcesError(Exception):
    pass


class InsufficientHealthyNodesError(InsufficientResourcesError):
    def __init__(self, bad_nodes: List, *args):
        self.bad_nodes = bad_nodes
        super().__init__(*args)


def _get_node_key(node: NodeType) -> NodeGroupKey:
    """Extract the (os, node_type) key from a node."""
    os = getattr(node, "operating_system", None)
    node_type = getattr(node, "node_type", None)
    return (os, node_type)


class NodeContainer(object):
    """
    Container for cluster nodes, grouped by (operating_system, node_type).

    This enables efficient lookup and allocation of nodes matching specific
    requirements. Nodes with node_type=None are grouped under (os, None) and
    can match any request when no specific type is required.
    """

    # Key: (os, node_type) tuple, Value: list of nodes
    node_groups: Dict[NodeGroupKey, List[NodeType]]

    def __init__(self, nodes: Optional[Iterable[NodeType]] = None) -> None:
        """
        Create a NodeContainer with the given nodes.

        Node objects should implement at least an operating_system property,
        and optionally a node_type property.

        :param nodes:           A collection of node objects to add, or None to add nothing.
        """
        self.node_groups = {}
        if nodes is not None:
            for node in nodes:
                self.add_node(node)

    def size(self) -> int:
        """
        Returns the total number of nodes in the container.
        """
        return sum([len(val) for val in self.node_groups.values()])

    def __len__(self):
        return self.size()

    def __iter__(self) -> Iterator[Any]:
        return self.elements()

    def elements(self, operating_system: Optional[str] = None, node_type: Optional[str] = None) -> Iterator[NodeType]:
        """
        Yield the elements in this container.

        :param operating_system:    If this is non-None, we will iterate only over elements
                                    which have this operating system.
        :param node_type:           If this is non-None, we will iterate only over elements
                                    which have this node type.
        """
        for (os, nt), node_list in self.node_groups.items():
            # Filter by OS if specified
            if operating_system is not None and os != operating_system:
                continue
            # Filter by node_type if specified
            if node_type is not None and nt != node_type:
                continue
            for node in node_list:
                yield node

    def add_node(self, node: Union[ClusterNode, RemoteAccount]) -> None:
        """
        Add a node to this collection, grouping by (os, node_type).

        :param node:                        The node to add.
        """
        key = _get_node_key(node)
        self.node_groups.setdefault(key, []).append(node)

    def add_nodes(self, nodes):
        """
        Add a collection of nodes to this collection.

        :param nodes:                       The nodes to add.
        """
        for node in nodes:
            self.add_node(node)

    def remove_node(self, node):
        """
        Removes a node from this collection.

        :param node:                        The node to remove.
        :returns:                           The node which has been removed.
        :throws NodeNotPresentError:        If the node is not in the collection.
        """
        key = _get_node_key(node)
        try:
            return self.node_groups.get(key, []).remove(node)
        except ValueError:
            raise NodeNotPresentError

    def remove_nodes(self, nodes):
        """
        Remove a collection of nodes from this collection.

        :param nodes:                       The nodes to remove.
        """
        for node in nodes:
            self.remove_node(node)

    def _group_spec_by_key(self, cluster_spec: ClusterSpec) -> Dict[NodeGroupKey, List["NodeSpec"]]:
        """
        Group the NodeSpecs in a ClusterSpec by (os, node_type) key.

        :param cluster_spec: The cluster spec to group
        :return: Dictionary mapping (os, node_type) to list of NodeSpecs
        """
        result: Dict[NodeGroupKey, List["NodeSpec"]] = {}
        for node_spec in cluster_spec.nodes.elements():
            key = (node_spec.operating_system, node_spec.node_type)
            result.setdefault(key, []).append(node_spec)
        return result

    def _find_matching_nodes(
        self, required_os: str, required_node_type: Optional[str], num_needed: int
    ) -> Tuple[List[NodeType], List[NodeType], int]:
        """
        Find nodes that match the required OS and node_type.

        Matching rules:
            - OS must match exactly
            - If required_node_type is None, match nodes of ANY type for this OS
            - If required_node_type is specified, match only nodes with that exact type

        :param required_os: The required operating system
        :param required_node_type: The required node type (None means any)
        :param num_needed: Number of nodes needed
        :return: Tuple of (good_nodes, bad_nodes, shortfall) where shortfall is how many more we need
        """
        good_nodes: List[NodeType] = []
        bad_nodes: List[NodeType] = []

        # Collect candidate keys - keys in node_groups that can satisfy this requirement
        candidate_keys: List[NodeGroupKey] = []
        for os, nt in self.node_groups.keys():
            if os != required_os:
                continue
            # If no specific type required, any node of this OS matches
            # If specific type required, only exact match
            if required_node_type is None or nt == required_node_type:
                candidate_keys.append((os, nt))

        # Try to allocate from candidate pools
        for key in candidate_keys:
            if len(good_nodes) >= num_needed:
                break

            avail_nodes = self.node_groups.get(key, [])
            while avail_nodes and len(good_nodes) < num_needed:
                node = avail_nodes.pop(0)
                if isinstance(node, RemoteAccount):
                    if node.available():
                        good_nodes.append(node)
                    else:
                        bad_nodes.append(node)
                else:
                    good_nodes.append(node)

        shortfall = max(0, num_needed - len(good_nodes))
        return good_nodes, bad_nodes, shortfall

    def remove_spec(self, cluster_spec: ClusterSpec) -> Tuple[List[NodeType], List[NodeType]]:
        """
        Remove nodes matching a ClusterSpec from this NodeContainer.

        Allocation strategy:
            - For each (os, node_type) in the spec:
                - If node_type is specified, allocate from that exact pool
                - If node_type is None, allocate from any pool matching the OS

        :param cluster_spec:                    The cluster spec.  This will not be modified.
        :returns:                               Tuple of (good_nodes, bad_nodes).
        :raises:                                InsufficientResourcesError when there aren't enough total nodes
                                                InsufficientHealthyNodesError when there aren't enough healthy nodes
        """
        err = self.attempt_remove_spec(cluster_spec)
        if err:
            raise InsufficientResourcesError(err)

        good_nodes: List[NodeType] = []
        bad_nodes: List[NodeType] = []
        msg = ""

        # Group required specs by (os, node_type)
        grouped_specs = self._group_spec_by_key(cluster_spec)

        for (os, node_type), node_specs in grouped_specs.items():
            num_needed = len(node_specs)
            found_good, found_bad, shortfall = self._find_matching_nodes(os, node_type, num_needed)

            good_nodes.extend(found_good)
            bad_nodes.extend(found_bad)

            if shortfall > 0:
                type_desc = f"{os}" if node_type is None else f"{os}/{node_type}"
                msg += f"{type_desc} nodes requested: {num_needed}. Healthy nodes available: {len(found_good)}. "

        if msg:
            # Return good nodes back to the container
            for node in good_nodes:
                self.add_node(node)
            raise InsufficientHealthyNodesError(bad_nodes, msg)

        return good_nodes, bad_nodes

    def can_remove_spec(self, cluster_spec: ClusterSpec) -> bool:
        """
        Determine if we can remove nodes matching a ClusterSpec from this NodeContainer.
        This container will not be modified.

        :param cluster_spec:                    The cluster spec.  This will not be modified.
        :returns:                               True if we could remove the nodes; false otherwise
        """
        msg = self.attempt_remove_spec(cluster_spec)
        return len(msg) == 0

    def _count_nodes_by_os(self, target_os: str) -> int:
        """
        Count total nodes available for a given OS (regardless of node_type).

        :param target_os: The operating system to count nodes for
        :return: Total number of nodes with the given OS
        """
        count = 0
        for (os, _), nodes in self.node_groups.items():
            if os == target_os:
                count += len(nodes)
        return count

    def _count_nodes_by_os_and_type(self, target_os: str, target_type: str) -> int:
        """
        Count nodes available for a specific (os, node_type) combination.

        :param target_os: The operating system
        :param target_type: The specific node type (not None)
        :return: Number of nodes matching both OS and type
        """
        return len(self.node_groups.get((target_os, target_type), []))

    def attempt_remove_spec(self, cluster_spec: ClusterSpec) -> str:
        """
        Attempt to remove a cluster_spec from this node container.

        Uses holistic per-OS validation to correctly handle mixed typed and untyped
        requirements without double-counting shared capacity.

        Validation strategy:
            1. Check total OS capacity >= total OS demand
            2. Check each specific type has enough nodes
            3. Check remaining capacity (after specific types) >= any-type demand

        :param cluster_spec:                    The cluster spec.  This will not be modified.
        :returns:                               An empty string if we can remove the nodes;
                                                an error string otherwise.
        """
        # if cluster_spec is None this means the test cannot be run at all
        # e.g. users didn't specify `@cluster` annotation on it but the session context has a flag to fail
        # on such tests or any other state where the test deems its cluster spec incorrect.
        if cluster_spec is None:
            return "Invalid or missing cluster spec"
        # cluster spec may be empty and that's ok, shortcut to returning no error messages
        elif len(cluster_spec) == 0 or cluster_spec.nodes is None:
            return ""

        msg = ""

        # Build requirements_by_os: {os -> {node_type -> count}} in a single pass
        requirements_by_os: Dict[str, Dict[Optional[str], int]] = {}
        for node_spec in cluster_spec.nodes.elements():
            os = node_spec.operating_system
            node_type = node_spec.node_type
            requirements_by_os.setdefault(os, {})
            requirements_by_os[os][node_type] = requirements_by_os[os].get(node_type, 0) + 1

        # Validate each OS holistically
        for os, type_requirements in requirements_by_os.items():
            total_available = self._count_nodes_by_os(os)
            total_required = sum(type_requirements.values())

            # Check 1: Total capacity for this OS
            if total_available < total_required:
                msg += f"{os} nodes requested: {total_required}. {os} nodes available: {total_available}. "
                continue  # Already failed, no need for detailed checks

            # Check 2: Each specific type has enough nodes
            for node_type, count_needed in type_requirements.items():
                if node_type is None:
                    continue  # Handle any-type separately
                type_available = self._count_nodes_by_os_and_type(os, node_type)
                if type_available < count_needed:
                    msg += f"{os}/{node_type} nodes requested: {count_needed}. {os}/{node_type} nodes available: {type_available}. "

            # Check 3: After reserving specific types, is there capacity for any-type?
            any_type_demand = type_requirements.get(None, 0)
            if any_type_demand > 0:
                specific_demand = sum(c for t, c in type_requirements.items() if t is not None)
                remaining_capacity = total_available - specific_demand
                if remaining_capacity < any_type_demand:
                    msg += (
                        f"{os} (any type) nodes requested: {any_type_demand}. "
                        f"{os} nodes remaining after typed allocations: {remaining_capacity}. "
                    )

        return msg

    def clone(self) -> "NodeContainer":
        """
        Returns a deep copy of this object.
        """
        container = NodeContainer()
        for key, nodes in self.node_groups.items():
            for node in nodes:
                container.node_groups.setdefault(key, []).append(node)
        return container
