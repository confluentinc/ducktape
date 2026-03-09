.. _topics-test_clusters:

===================
Test Clusters
===================

Ducktape runs on a test cluster with several nodes.  Ducktape will take ownership of the nodes and handle starting, stopping, and running services on them.

Many test environments are possible.  The nodes may be local nodes, running inside Docker.  Or they could be virtual machines running on a public cloud.

Cluster Specifications
======================

A cluster specification-- also called a ClusterSpec-- describes a particular
cluster configuration.  Originally, cluster specifications could only express the
number of nodes of each operating system. Now, with heterogeneous cluster support,
specifications can also include node types (e.g., "small", "large") for more
fine-grained resource allocation. See `Heterogeneous Clusters`_ for more details.

Cluster specifications give us a vocabulary to express what a particular
service or test needs to run.  For example, a service might require a cluster
with three Linux nodes and one Windows node.  We could express that with a
ClusterSpec containing three Linux NodeSpec objects and one Windows NodeSpec
object.

Heterogeneous Clusters
======================

Ducktape supports heterogeneous clusters where nodes can have different types
(e.g., "small", "large", "arm64"). This allows tests to request specific node
types while maintaining backward compatibility with existing tests.

Using Node Types in Tests
-------------------------

Use the ``@cluster`` decorator with ``node_type`` to request specific node types::

    from ducktape.mark.resource import cluster

    @cluster(num_nodes=3, node_type="large")
    def test_with_large_nodes(self):
        # This test requires 3 large nodes
        pass

    @cluster(num_nodes=5)
    def test_any_nodes(self):
        # This test accepts any 5 nodes (backward compatible)
        pass

Cluster Configuration
---------------------

Node types are defined in your ``cluster.json`` file::

    {
      "nodes": [
        {
          "ssh_config": {"host": "worker1", ...},
          "node_type": "small"
        },
        {
          "ssh_config": {"host": "worker2", ...},
          "node_type": "large"
        }
      ]
    }

Backward Compatibility
----------------------

- Tests without ``node_type`` will match **any available node**
- Existing tests and cluster configurations continue to work unchanged
- Node type is optional in both test annotations and cluster configuration
