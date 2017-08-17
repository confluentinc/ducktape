.. _topics-test_clusters:

===================
Test Clusters
===================

Ducktape runs on a test cluster with several nodes.  Ducktape will take ownership of the nodes and handle starting, stopping, and running services on them.

Many test environments are possible.  The nodes may be local nodes, running inside Docker.  Or they could be virtual machines running on a public cloud.

Cluster Specifications
======================

A cluster specification-- also called a ClusterSpec-- describes a particular
cluster configuration.  Currently the cluster specification can express the
number of nodes of each operating system that are required.

Cluster specifications give us a vocabulary to express what a particular
service or test needs to run.  For example, a service might require a cluster
with three Linux nodes and one Windows node.  We could express that with a
ClusterSpec containing three Linux NodeSpec objects and one Windows NodeSpec
object.
