.. _topics-new_services:

New Services
============

``Service`` refers generally to multiple processes, possibly long-running, which you
want to run on the test cluster.

These can be services you would actually deploy (e.g., Kafka brokers, ZK servers, REST proxy) or processes used during testing (e.g. producer/consumer performance processes). Services that are distributed systems can support a variable number of nodes which allow them to handle a variety of tests.

Each service is implemented as a class and should at least implement the following:

    * :meth:`~ducktape.services.service.Service.start_node` - start the service (possibly waiting to ensure it started successfully)

    * :meth:`~ducktape.services.service.Service.stop_node` - kill processes on the given node

    * :meth:`~ducktape.services.service.Service.clean_node` - remove persistent state leftover from testing, e.g. log files

These may block to ensure services start or stop properly, but must *not* block for the full lifetime of the service. If you need to run a blocking process (e.g. run a process via SSH and iterate over its output), this should be done in a background thread. For services that exit after completing a fixed operation (e.g. produce N messages to topic foo), you should also implement ``wait``, which will usually just wait for background worker threads to exit. The ``Service`` base class provides a helper method ``run`` which wraps ``start``, ``wait``, and ``stop`` for tests that need to start a service and wait for it to finish. You can also provide additional helper methods for common test functionality. Normal services might provide a ``bounce`` method.

Most of the code you'll write for a service will just be series of SSH commands and tests of output. You should request the number of nodes you'll need using the ``num_nodes`` or ``node_spec`` parameter to the Service base class's constructor. Then, in your Service's methods you'll have access to ``self.nodes`` to access the nodes allocated to your service. Each node has an associated :class:`~ducktape.cluster.remoteaccount.RemoteAccount` instance which lets you easily perform remote operations such as running commands via SSH or creating files. By default, these operations try to hide output (but provide it to you if you need to extract some subset of it) and *checks status codes for errors* so any operations that fail cause an obvious failure of the entire test.
