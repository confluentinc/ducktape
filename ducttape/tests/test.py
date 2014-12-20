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

from ducttape.cluster import VagrantCluster
from ducttape.services.core import ZookeeperService, KafkaService, KafkaRestService
from ducttape.logger import Logger
import logging

class Test(Logger):
    'Base class for tests that provides some minimal helper utilities'

    def __init__(self, cluster):
        self.cluster = cluster

    def log_start(self):
        self.logger.info("Running test %s", self._short_class_name())

    @classmethod
    def run_standalone(cls):
        logging.basicConfig(level=logging.INFO)
        cluster = VagrantCluster()
        test = cls(cluster)

        test.log_start()
        test.run()

class KafkaTest(Test):
    '''Helper class that managest setting up a Kafka cluster. Use this if the
    default settings for Kafka are sufficient for your test; any customization
    needs to be done manually. Your run() method should call tearDown and
    setUp. The Zookeeper and Kafka services are available as the fields
    KafkaTest.zk and KafkaTest.kafka.
    '''

    def __init__(self, cluster, num_zk, num_brokers, topics=None):
        super(KafkaTest, self).__init__(cluster)
        self.num_zk_nodes = num_zk
        self.num_brokers = num_brokers
        self.topics = topics

    def setUp(self):
        self.zk = ZookeeperService(self.cluster, self.num_zk_nodes)
        self.kafka = KafkaService(self.cluster, self.num_brokers, self.zk, topics=self.topics)
        self.zk.start()
        self.kafka.start()

    def tearDown(self):
        self.kafka.stop()
        self.zk.stop()

class RestProxyTest(KafkaTest):
    '''Helper class that manages setting up Kafka and the REST proxy. The REST proxy
    service is available as the field RestProxyTest.rest.
    '''
    def __init__(self, cluster, num_zk, num_brokers, num_rest, topics=None):
        super(RestProxyTest, self).__init__(cluster, num_zk, num_brokers, topics=topics)
        self.num_rest = num_rest

    def setUp(self):
        super(RestProxyTest, self).setUp()
        self.rest = KafkaRestService(self.cluster, self.num_rest, self.zk, self.kafka)
        self.rest.start()

    def tearDown(self):
        self.rest.stop()
        super(RestProxyTest, self).tearDown()
