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
from ducttape.services.core import ZookeeperService, KafkaService, KafkaRestService, SchemaRegistryService
from ducttape.services.register_schemas_service import RegisterSchemasService
from ducttape.services.schema_registry_utils import get_schema_by_id, get_all_versions, get_schema_by_version
from ducttape.logger import Logger
import logging, time


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


class SchemaRegistryTest(KafkaTest):
    '''Helper class that manages setting up Kafka and the Schema Registry proxy. The Schema Registry
    service is available as the field SchemaRegistryTest.schema_registry.
    '''
    def __init__(self, cluster, num_zk=1, num_brokers=1, num_schema_reg=1):
        super(SchemaRegistryTest, self).__init__(cluster, num_zk, num_brokers, topics=None)
        self.num_schema_reg = num_schema_reg

    def setUp(self):
        super(SchemaRegistryTest, self).setUp()
        self.schema_registry = SchemaRegistryService(self.cluster, self.num_schema_reg, self.zk, self.kafka)
        self.schema_registry.start()

    def tearDown(self):
        self.schema_registry.stop()
        super(SchemaRegistryTest, self).tearDown()


class SchemaRegistryFailoverTest(SchemaRegistryTest):
    def __init__(self, cluster, num_zk, num_brokers, num_schema_reg):
        super(SchemaRegistryFailoverTest, self).__init__(cluster, num_zk, num_brokers, num_schema_reg)

        # Time to wait between registration retries
        self.retry_wait_sec = .2

        # Number of attempted retries
        self.num_retries = 10

        # Initialize during setUp
        self.register_driver = None

    def setUp(self):
        super(SchemaRegistryFailoverTest, self).setUp()
        self.register_driver = RegisterSchemasService(self.cluster, 1, self.schema_registry, self.retry_wait_sec,
                                                      self.num_retries, max_time_seconds=180)

    def drive_failures(self):
        raise NotImplementedError("drive_failures must be implemented by a subclass.")

    def report_summary(self):
        # Gather statistics
        summary = "\n-------------------------------------------------------------------\n"
        summary += "Summary\n"
        summary += str(self.register_driver.try_histogram) + "\n"

        attempted = self.register_driver.num_attempted_registrations
        succeeded = sum([1 for record in self.register_driver.registration_data if record["success"]])
        summary += "Attempted to register %d schemas. " % attempted + "\n"
        summary += "Max registration attempts allowed: %d\n" % self.num_retries
        summary += "Retry backoff: %f seconds\n" % self.retry_wait_sec
        summary += "Successful: %d/%d = %f\n" % (succeeded, attempted, succeeded / float(attempted))

        # Verify that all ids reported as successfully registered are in fact registered
        master_id = self.schema_registry.idx(self.schema_registry.get_master_node())
        base_url = self.schema_registry.url(master_id)
        registered_ids = [record["schema_id"] for record in self.register_driver.registration_data if record["success"]]
        registered_schemas = [record["schema_string"]
                              for record in self.register_driver.registration_data if record["success"]]
        summary += "Validating that schemas reported as successful can be fetched by id...\n"
        success = True
        for id in registered_ids:
            try:
                schema = get_schema_by_id(base_url, id)
            except:
                success = False
                summary += "%d was reported successful but actually failed\n" % id
        if success:
            summary += "Success.\n"
        else:
            summary += "Failure.\n"

        # Verify that number of versions fetched matches number of registered ids
        versions = get_all_versions(base_url, self.register_driver.subject)
        summary += \
            "Validating that number of reported successful registrations matches number of versions in subject...\n"
        if len(versions) == len(registered_ids):
            summary += "Success.\n"
        else:
            summary += "Failure.\n"
            success = False

        # Validate by fetching versions
        summary += "Validating schemas fetched by subject/version...\n"
        try:
            reported_registered_ids = set(registered_ids)
            for version in versions:
                schema_info = get_schema_by_version(base_url, self.register_driver.subject, version)
                if schema_info["id"] not in reported_registered_ids:
                    success = False
        except:
            success = False

        if success:
            summary += "Success.\n"
        else:
            summary += "Failure.\n"

        summary += "-------------------------------------------------------------------\n"

        self.logger.info(summary)

    def run(self):
        # set up
        self.setUp()

        # start schema registration in the background
        self.logger.info("Starting registration thread(s)")
        self.register_driver.start()

        # Make sure registrations have started
        while self.register_driver.num_attempted_registrations < 2:
            time.sleep(.5)

        # do the kill or bounce logic
        self.logger.info("Driving failures")
        self.drive_failures()

        # Wait a little before stopping registration
        num_attempted = self.register_driver.num_attempted_registrations
        while self.register_driver.num_attempted_registrations < num_attempted + 2:
            time.sleep(.5)

        self.logger.info("Ending registration...")
        self.register_driver.ready_to_finish = True
        self.register_driver.wait()
        self.register_driver.stop()

        self.report_summary()
        self.tearDown()

