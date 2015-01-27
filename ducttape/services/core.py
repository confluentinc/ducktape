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

from .service import Service
from .schema_registry_utils import ping_registry
import time, re

class ZookeeperService(Service):
    def __init__(self, cluster, num_nodes):
        super(ZookeeperService, self).__init__(cluster, num_nodes)

    def start(self):
        super(ZookeeperService, self).start()
        config = """
dataDir=/mnt/zookeeper
clientPort=2181
maxClientCnxns=0
initLimit=5
syncLimit=2
quorumListenOnAllIPs=true
"""
        for idx,node in enumerate(self.nodes,1):
            template_params = { 'idx': idx, 'host': node.account.hostname }
            config += "server.%(idx)d=%(host)s:2888:3888\n" % template_params

        for idx,node in enumerate(self.nodes,1):
            self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node, allow_fail=True)
            node.account.ssh("mkdir -p /mnt/zookeeper")
            node.account.ssh("echo %d > /mnt/zookeeper/myid" % idx)
            node.account.create_file("/mnt/zookeeper.properties", config)
            node.account.ssh("/opt/kafka/bin/zookeeper-server-start.sh /mnt/zookeeper.properties 1>> /mnt/zk.log 2>> /mnt/zk.log &")
            time.sleep(5) # give it some time to start


    def stop(self):
        for idx,node in enumerate(self.nodes,1):
            self.logger.info("Stopping ZK node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node)
            node.free()

    def _stop_and_clean(self, node, allow_fail=False):
        # This uses Kafka-REST's stop service script because it's better behaved
        # (knows how to wait) and sends SIGTERM instead of
        # zookeeper-stop-server.sh's SIGINT. We don't actually care about clean
        # shutdown here, so it's ok to use the bigger hammer
        node.account.ssh("/opt/kafka-rest/bin/kafka-rest-stop-service zookeeper", allow_fail=allow_fail)
        node.account.ssh("rm -rf /mnt/zookeeper /mnt/zookeeper.properties /mnt/zk.log")


    def connect_setting(self):
        return ','.join([node.account.hostname + ':2181' for node in self.nodes])


class KafkaService(Service):
    def __init__(self, cluster, num_nodes, zk, topics=None):
        super(KafkaService, self).__init__(cluster, num_nodes)
        self.zk = zk
        self.topics = topics

    def start(self):
        super(KafkaService, self).start()
        template = open('templates/kafka.properties').read()
        zk_connect = self.zk.connect_setting()
        for idx,node in enumerate(self.nodes,1):
            self.logger.info("Starting Kafka node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node, allow_fail=True)
            template_params = {
                'broker_id': idx,
                'hostname': node.account.hostname,
                'zk_connect': zk_connect
            }
            config = template % template_params
            node.account.create_file("/mnt/kafka.properties", config)
            node.account.ssh("/opt/kafka/bin/kafka-server-start.sh /mnt/kafka.properties 1>> /mnt/kafka.log 2>> /mnt/kafka.log &")
            time.sleep(5) # wait for start up

        if self.topics is not None:
            node = self.nodes[0] # any node is fine here
            for topic,settings in self.topics.items():
                if settings is None:
                    settings = {}
                self.logger.info("Creating topic %s with settings %s", topic, settings)
                node.account.ssh(
                    "/opt/kafka/bin/kafka-topics.sh --zookeeper %(zk_connect)s --create "\
                    "--topic %(name)s --partitions %(partitions)d --replication-factor %(replication)d" % {
                        'zk_connect': zk_connect,
                        'name': topic,
                        'partitions': settings.get('partitions', 1),
                        'replication': settings.get('replication-factor', 1)
                    })

    def stop(self):
        for idx,node in enumerate(self.nodes,1):
            self.logger.info("Stopping Kafka node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node)
            node.free()

    def _stop_and_clean(self, node, allow_fail=False):
        node.account.ssh("/opt/kafka/bin/kafka-server-stop.sh", allow_fail=allow_fail)
        time.sleep(5) # the stop script doesn't wait
        node.account.ssh("rm -rf /mnt/kafka-logs /mnt/kafka.properties /mnt/kafka.log")

    def bootstrap_servers(self):
        return ','.join([node.account.hostname + ":9092" for node in self.nodes])


class KafkaRestService(Service):
    def __init__(self, cluster, num_nodes, zk, kafka):
        super(KafkaRestService, self).__init__(cluster, num_nodes)
        self.zk = zk
        self.kafka = kafka

    def start(self):
        super(KafkaRestService, self).start()
        template = open('templates/rest.properties').read()
        zk_connect = self.zk.connect_setting()
        bootstrapServers = self.kafka.bootstrap_servers()
        for idx,node in enumerate(self.nodes,1):
            self.logger.info("Starting REST node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node, allow_fail=True)
            template_params = {
                'id': idx,
                'port': 8080,
                'zk_connect': zk_connect,
                'bootstrap_servers': bootstrapServers
            }
            config = template % template_params
            node.account.create_file("/mnt/rest.properties", config)
            node.account.ssh("/opt/kafka-rest/bin/kafka-rest-start /mnt/rest.properties 1>> /mnt/rest.log 2>> /mnt/rest.log &")

    def stop(self):
        for idx,node in enumerate(self.nodes,1):
            self.logger.info("Stopping REST node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node)
            node.free()

    def _stop_and_clean(self, node, allow_fail=False):
        node.account.ssh("/opt/kafka-rest/bin/kafka-rest-stop", allow_fail=allow_fail)
        node.account.ssh("rm -rf /mnt/rest.properties /mnt/rest.log")

    def url(self, idx=1):
        return "http://" + self.get_node(idx).account.hostname + ":8080"


class SchemaRegistryService(Service):
    def __init__(self, cluster, num_nodes, zk, kafka):
        super(SchemaRegistryService, self).__init__(cluster, num_nodes)
        self.zk = zk
        self.kafka = kafka

    def start(self):
        super(SchemaRegistryService, self).start()

        template = open('templates/schema-registry.properties').read()
        template_params = {
            'kafkastore_topic': '_schemas',
            'kafkastore_url': self.zk.connect_setting(),
            'rest_port': 8080
        }
        config = template % template_params

        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Starting Schema Registry node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node, allow_fail=True)
            self.start_node(node, config)

            # Give the server a little time to become live
            stop = time.time() + 10
            awake = False
            while time.time() < stop:
                try:
                    ping_registry(self.url(idx))
                    awake = True
                    break
                except:
                    time.sleep(.25)
                    pass
            if not awake:
                raise Exception("Timed out trying to contact service on %s. " % self.url(idx) +
                                "Either the service failed to start, or there is a problem with the url. "
                                "You may need to open Vagrantfile.local and add the line 'enable_dns = true'.")

    def stop(self):
        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Stopping Schema Registry node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node, True)
            node.free()

    def _stop_and_clean(self, node, allow_fail=False):
        node.account.ssh("/opt/schema-registry/bin/schema-registry-stop", allow_fail=allow_fail)
        node.account.ssh("rm -rf /mnt/schema-registry.properties /mnt/schema-registry.log")

    def kill_node(self, node, clean_shutdown=True):

        if clean_shutdown:
            node.account.ssh("/opt/schema-registry/bin/schema-registry-stop", allow_fail=True)
        else:
            cmd = """TARGET=`ps ax | grep -i schema-registry | grep java | grep -v grep | awk '{print $1}'`;"""
            cmd += """kill -9 $TARGET"""
            node.account.ssh(cmd, allow_fail=True)

    def start_node(self, node, config=None):
        if config is None:
            template = open('templates/schema-registry.properties').read()
            template_params = {
                'kafkastore_topic': '_schemas',
                'kafkastore_url': self.zk.connect_setting(),
                'rest_port': 8080
            }
            config = template % template_params

        node.account.create_file("/mnt/schema-registry.properties", config)
        cmd = "/opt/schema-registry/bin/schema-registry-start /mnt/schema-registry.properties " \
            + "1>> /mnt/schema-registry.log 2>> /mnt/schema-registry.log &"

        node.account.ssh(cmd)

    def restart_node(self, node, wait_sec=0, clean_shutdown=True):
        self.kill_node(node, clean_shutdown)
        time.sleep(wait_sec)
        self.start_node(node)

    def get_master_node(self):
        node = self.nodes[0]

        cmd = "/opt/kafka/bin/kafka-run-class.sh kafka.tools.ZooKeeperMainWrapper -server %s get /schema-registry-master" \
              % self.zk.connect_setting()

        host = None
        port_str = None
        self.logger.debug("Querying zookeeper to find current schema registry master: \n%s" % cmd)
        for line in node.account.ssh_capture(cmd):
            match = re.match("^{\"host\":\"(.*)\",\"port\":(\d+),", line)
            if match is not None:
                groups = match.groups()
                host = groups[0]
                port_str = groups[1]
                break

        if host is None:
            raise Exception("Could not find schema registry master.")

        base_url = "%s:%s" % (host, port_str)
        self.logger.debug("schema registry master is %s" % base_url)

        # Return the node with this base_url
        for idx, node in enumerate(self.nodes, 1):
            if self.url(idx).find(base_url) >= 0:
                return self.get_node(idx)

    def url(self, idx=1):
        return "http://" + self.get_node(idx).account.hostname + ":8080"
