.. _topics-new_services:

===================
Create New Services
===================

Writing ducktape services
=============================

``Service`` refers generally to multiple processes, possibly long-running, which you
want to run on the test cluster.

These can be services you would actually deploy (e.g., Kafka brokers, ZK servers, REST proxy) or processes used during testing (e.g. producer/consumer performance processes). Services that are distributed systems can support a variable number of nodes which allow them to handle a variety of tests.

Each service is implemented as a class and should at least implement the following:

    * :meth:`~ducktape.services.service.Service.start_node` - start the service (possibly waiting to ensure it started successfully)

    * :meth:`~ducktape.services.service.Service.stop_node` - kill processes on the given node

    * :meth:`~ducktape.services.service.Service.clean_node` - remove persistent state leftover from testing, e.g. log files

These may block to ensure services start or stop properly, but must *not* block for the full lifetime of the service. If you need to run a blocking process (e.g. run a process via SSH and iterate over its output), this should be done in a background thread. For services that exit after completing a fixed operation (e.g. produce N messages to topic foo), you should also implement ``wait``, which will usually just wait for background worker threads to exit. The ``Service`` base class provides a helper method ``run`` which wraps ``start``, ``wait``, and ``stop`` for tests that need to start a service and wait for it to finish. You can also provide additional helper methods for common test functionality. Normal services might provide a ``bounce`` method.

Most of the code you'll write for a service will just be series of SSH commands and tests of output. You should request the number of nodes you'll need using the ``num_nodes`` or ``node_spec`` parameter to the Service base class's constructor. Then, in your Service's methods you'll have access to ``self.nodes`` to access the nodes allocated to your service. Each node has an associated :class:`~ducktape.cluster.remoteaccount.RemoteAccount` instance which lets you easily perform remote operations such as running commands via SSH or creating files. By default, these operations try to hide output (but provide it to you if you need to extract some subset of it) and *checks status codes for errors* so any operations that fail cause an obvious failure of the entire test.

.. _service-example-ref:

New Service Example
===================

Let’s walk through an example of writing a simple Zookeeper service.

.. code-block:: python

    class ZookeeperService(Service):
        PERSISTENT_ROOT = "/mnt"
        LOG_FILE = os.path.join(PERSISTENT_ROOT, "zk.log")
        DATA_DIR = os.path.join(PERSISTENT_ROOT, "zookeeper")
        CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "zookeeper.properties")

        logs = {
            "zk_log": {
                "path": LOG_FILE,
                "collect_default": True},
            "zk_data": {
                "path": DATA_DIR,
                "collect_default": False}
        }

        def __init__(self, context, num_nodes):
            super(ZookeeperService, self).__init__(context, num_nodes)


Log files will be collected on both successful and failed test runs, while files from the data directory will be collected only on failed test runs. Zookeeper service requests the number of nodes passed to its constructor by passing ``num_nodes`` parameters to the Service base class’s constructor.

.. code-block:: python

        def start_node(self, node):
            idx = self.idx(node)
            self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)

            node.account.ssh("mkdir -p %s" % self.DATA_DIR)
            node.account.ssh("echo %d > %s/myid" % (idx, self.DATA_DIR))

            prop_file = """\n dataDir=%s\n clientPort=2181""" % self.DATA_DIR
            for idx, node in enumerate(self.nodes):
                prop_file += "\n server.%d=%s:2888:3888" % (idx, node.account.hostname)
            self.logger.info("zookeeper.properties: %s" % prop_file)
            node.account.create_file(self.CONFIG_FILE, prop_file)

            start_cmd = "/opt/kafka/bin/zookeeper-server-start.sh %s 1>> %s 2>> %s &" % \
                    (self.CONFIG_FILE, self.LOG_FILE, self.LOG_FILE)

            with node.account.monitor_log(self.LOG_FILE) as monitor:
                node.account.ssh(start_cmd)
                monitor.wait_until(
                    "binding to port",
                    timeout_sec=100,
                    backoff_sec=7,
                    err_msg="Zookeeper service didn't finish startup"
                )
            self.logger.debug("Zookeeper service is successfully started.")


The ``start_node`` method first creates directories and the config file on the given node, and then invokes the start script to start a Zookeeper service. In this simple example, the config file is created from ``prop_file`` string. You can also create config file from a template, as described in :ref:`using-templates-ref`.

A service may take time to start and get to a usable state. Using sleeps to wait for a service to start often leads to a flaky test. The sleep time may be too short, or the service may fail to start altogether. It is useful to verify that the service starts properly before returning from the ``start_node``, and fail the test if the service fails to start. Otherwise, the test will likely fail later, and it would be harder to find the root cause of the failure. One way to check that the service starts successfully is to check whether a service’s process is alive and one additional check that the service is usable such as querying the service or checking some metrics if they are available. Our example checks whether a Zookeeper service is started successfully by searching for a particular output in a log file.

The :class:`~ducktape.cluster.remoteaccount.RemoteAccount` instance associated with each node provides you with :class:`~ducktape.cluster.remoteaccount.LogMonitor` that let you check or wait for a pattern to appear in the log. Our example waits for 100 seconds for “binding to port” string to appear in the ``self.LOG_FILE`` log file, and raises an exception if it does not.

.. code-block:: python

    def pids(self, node):
        try:
            cmd = "ps ax | grep -i zookeeper | grep java | grep -v grep | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    def stop_node(self, node):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        node.account.kill_process("zookeeper", allow_fail=False)

    def clean_node(self, node):
        self.logger.info("Cleaning Zookeeper node %d on %s", self.idx(node), node.account.hostname)
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        node.account.kill_process("zookeeper", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf /mnt/zookeeper /mnt/zookeeper.properties /mnt/zk.log",
                         allow_fail=False)


The ``stop_node`` method uses :meth:`~ducktape.cluster.remoteaccount.RemoteAccount.kill_process` to terminate the service process on the given node. If the remote command to terminate the process fails, :meth:`~ducktape.cluster.remoteaccount.RemoteAccount.kill_process` will raise an ``RemoteCommandError`` exception.

The ``clean_node`` method forcefully kills the process if it is still alive, and then removes persistent state leftover from testing. Make sure to properly cleanup the state to avoid test order dependency and flaky tests. You can assume complete control of the machine, so it is safe to delete an entire temporary working space and kill all java processes, etc.

.. _using-templates-ref:


Using Templates
===============

Both ``Service`` and ``Test`` subclass :class:`~ducktape.template.TemplateRenderer` that lets you render templates directly from strings or from files loaded from *templates/* directory relative to the class. A template contains variables and/or expressions, which are replaced with values when a template is rendered. :class:`~ducktape.template.TemplateRenderer` renders templates using Jinja2 template engine. A good use-case for templates is a properties file that needs to be passed to a service process. In :ref:`service-example-ref`, the properties file is created by building a string and using it as contents as follows::

        prop_file = """\n dataDir=%s\n clientPort=2181""" % self.DATA_DIR
        for idx, node in enumerate(self.nodes):
            prop_file += "\n server.%d=%s:2888:3888" % (idx, node.account.hostname)
        node.account.create_file(self.CONFIG_FILE, prop_file)

A template approach is to add a properties file in *templates/* directory relative to the ZookeeperService class:

.. code-block:: rst

    dataDir={{ DATA_DIR }}
    clientPort=2181
    {% for node in nodes %}
    server.{{ loop.index }}={{ node.account.hostname }}:2888:3888
    {% endfor %}


Suppose we named the file zookeeper.properties. The creation of the config file will look like this:

.. code-block:: python

        prop_file = self.render('zookeeper.properties')
        node.account.create_file(self.CONFIG_FILE, prop_file)
