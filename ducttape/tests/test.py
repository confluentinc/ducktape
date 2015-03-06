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
from ducttape.services.register_schemas_service import RegisterSchemasService
from ducttape.services.schema_registry_utils import get_schema_by_id, get_all_versions, get_schema_by_version, \
    get_by_schema
from ducttape.services.core import ZookeeperService, KafkaService, KafkaRestService, SchemaRegistryService, \
    create_hadoop_service
from ducttape.logger import Logger
import logging
import time
import json

class Test(Logger):
    """
    Base class for tests that provides some minimal helper utilities'
    """

    def __init__(self, cluster):
        self.cluster = cluster

    def log_start(self):
        self.logger.info("Running test %s", self._short_class_name())

    def min_cluster_size(self):
        """
        Subclasses implement this to provide a helpful heuristic to prevent trying to run a test on a cluster
        with too few nodes.
        """
        raise NotImplementedError("All tests must implement this method.")

    @classmethod
    def run_standalone(cls):
        logging.basicConfig(level=logging.INFO)
        cluster = VagrantCluster()
        test = cls(cluster)

        if test.min_cluster_size() > cluster.num_available_nodes():
            raise RuntimeError(
                "There are not enough nodes available in the cluster to run this test. Needed: %d, Available: %d" %
                (test.min_cluster_size(), cluster.num_available_nodes()))

        test.log_start()
        test.run()


class KafkaTest(Test):
    """
    Helper class that managest setting up a Kafka cluster. Use this if the
    default settings for Kafka are sufficient for your test; any customization
    needs to be done manually. Your run() method should call tearDown and
    setUp. The Zookeeper and Kafka services are available as the fields
    KafkaTest.zk and KafkaTest.kafka.
    """
    def __init__(self, cluster, num_zk, num_brokers, topics=None):
        super(KafkaTest, self).__init__(cluster)
        self.num_zk = num_zk
        self.num_brokers = num_brokers
        self.topics = topics

    def min_cluster_size(self):
        return self.num_zk + self.num_brokers

    def setUp(self):
        self.zk = ZookeeperService(self.cluster, self.num_zk)
        self.kafka = KafkaService(self.cluster, self.num_brokers, self.zk, topics=self.topics)
        self.zk.start()
        self.kafka.start()

    def tearDown(self):
        self.kafka.stop()
        self.zk.stop()


class RestProxyTest(KafkaTest):
    """
    Helper class that manages setting up Kafka and the REST proxy. The REST proxy
    service is available as the field RestProxyTest.rest.
    """
    def __init__(self, cluster, num_zk, num_brokers, num_rest, topics=None):
        super(RestProxyTest, self).__init__(cluster, num_zk, num_brokers, topics=topics)
        self.num_rest = num_rest

    def min_cluster_size(self):
        return self.num_zk + self.num_brokers + self.num_rest

    def setUp(self):
        super(RestProxyTest, self).setUp()
        self.rest = KafkaRestService(self.cluster, self.num_rest, self.zk, self.kafka)
        self.rest.start()

    def tearDown(self):
        self.rest.stop()
        super(RestProxyTest, self).tearDown()


class SchemaRegistryTest(KafkaTest):
    """
    Helper class that manages setting up Kafka and the Schema Registry proxy. The Schema Registry
    service is available as the field SchemaRegistryTest.schema_registry.
    """
    def __init__(self, cluster, num_zk=1, num_brokers=1, num_schema_registry=1):
        super(SchemaRegistryTest, self).__init__(cluster, num_zk, num_brokers, topics={"_schemas": {
            "name": "_schemas",
            "partitions": 1,
            "replication-factor": min(num_brokers, 3),
            "configs": {
                "min.insync.replicas": 1 if num_brokers < 3 else 2,
                "unclean.leader.election.enable": "false"
            }
        }})

        self.num_schema_registry = num_schema_registry
        # Initialize self.schema_registry in setUp()
        self.schema_registry = None

    def min_cluster_size(self):
        return self.num_zk + self.num_brokers + self.num_schema_registry

    def setUp(self):
        super(SchemaRegistryTest, self).setUp()
        self.schema_registry = SchemaRegistryService(self.cluster, self.num_schema_registry, self.zk, self.kafka)
        self.schema_registry.start()

    def tearDown(self):
        self.schema_registry.stop()
        super(SchemaRegistryTest, self).tearDown()


class SchemaRegistryFailoverTest(SchemaRegistryTest):
    def __init__(self, cluster, num_zk, num_brokers, num_schema_registry):
        super(SchemaRegistryFailoverTest, self).__init__(cluster, num_zk, num_brokers, num_schema_registry)

        # Time to wait between registration retries
        self.retry_wait_sec = .2

        # Number of attempted retries
        self.num_retries = 10

        # Initialize during setUp
        self.register_driver = None

    def setUp(self):
        super(SchemaRegistryFailoverTest, self).setUp()
        self.register_driver = RegisterSchemasService(self.cluster, 1, self.schema_registry, self.retry_wait_sec,
                                                      self.num_retries, max_time_seconds=900)

    def drive_failures(self):
        raise NotImplementedError("drive_failures must be implemented by a subclass.")

    def report_summary(self):
        # Gather statistics
        summary = "\n-------------------------------------------------------------------\n"
        summary += "Summary\n"
        summary += "Histogram of number of attempts needed to successfully register:\n"
        summary += str(self.register_driver.try_histogram) + "\n"

        attempted = self.register_driver.num_attempted_registrations
        succeeded = sum([1 for record in self.register_driver.registration_data if record["success"]])
        summary += "Attempted to register %d schemas. " % attempted + "\n"
        summary += "Max registration attempts allowed: %d\n" % self.num_retries
        summary += "Retry backoff: %f seconds\n" % self.retry_wait_sec
        summary += "Successful: %d/%d = %f\n" % (succeeded, attempted, succeeded / float(attempted))

        success = True

        # Verify that all ids reported as successfully registered can be fetched
        master_id = self.schema_registry.idx(self.schema_registry.get_master_node())
        base_url = self.schema_registry.url(master_id)
        registered_ids = [record["schema_id"] for record in self.register_driver.registration_data if record["success"]]
        registered_schemas = [record["schema_string"]
                              for record in self.register_driver.registration_data if record["success"]]
        summary += "Validating that schemas reported as successful can be fetched by id...\n"
        for id in registered_ids:
            try:
                schema = get_schema_by_id(base_url, id)
            except:
                success = False
                summary += "%d was reported successful but actually failed\n" % id
        summary += "Success.\n" if success else "Failure.\n"

        # Verify that number of versions fetched matches number of registered ids
        versions = get_all_versions(base_url, self.register_driver.subject)
        summary += \
            "Validating that number of reported successful registrations matches number of versions in subject...\n"
        if len(versions) != len(registered_ids):
            success = False
        summary += "Success.\n" if success else "Failure.\n"

        results = self.validate_schema_consistency()
        summary += results["message"] + "\n"
        success = success and results["success"]

        results = self.validate_registered_vs_subjectversion()
        summary += results["message"] + "\n"
        success = success and results["success"]

        results = self.validate_registered_vs_subjectschema()
        summary += results["message"] + "\n"
        success = success and results["success"]

        summary += "-------------------------------------------------------------------\n"
        self.logger.info(summary)

    def normalize_schema_string(self, schema_string):
        return json.dumps(json.loads(schema_string))

    def get_ids_and_schemas_registered(self):
        """
        Return all pairs (id, schema) that reported as successfully registered by the register schemas service.
        """
        registration_data = self.register_driver.registration_data
        return {(record["schema_id"], record["schema_string"])
                for record in registration_data if record["success"]}


    def fetch_ids_and_schemas_by_subjectschema(self, reported_records):
        """
        Return all pairs (id, schema) that can be fetched by subject/schema, for all schemas that we attempted
        to register.
        """
        attempted_schemas = [r["schema_string"] for r in self.register_driver.registration_data]



        stored_records = set()
        master_id = self.schema_registry.idx(self.schema_registry.get_master_node())
        base_url = self.schema_registry.url(master_id)

        for id, schema in reported_records:
            stored_id = get_by_schema(base_url, schema, self.register_driver.subject)["id"]
            stored_records.add((stored_id, schema))
        return stored_records

    def fetch_ids_and_schemas_by_subjectversion(self):
        """
        Return all pairs (id, schema) that can be fetched by subject/version, for all versions listed under the subject.
        """
        master_id = self.schema_registry.idx(self.schema_registry.get_master_node())
        base_url = self.schema_registry.url(master_id)
        versions = get_all_versions(base_url, self.register_driver.subject)

        fetched_ids_and_schemas = []
        failed_versions = []
        for version in versions:
            try:
                fetched_schema_info = get_schema_by_version(base_url, self.register_driver.subject, version)
                fetched_schema_string = self.normalize_schema_string(fetched_schema_info["schema"])
                fetched_ids_and_schemas.append((fetched_schema_info["id"], fetched_schema_string))
            except:
                failed_versions.append(version)
        if len(failed_versions) > 0:
            raise Exception("Failed to fetch versions: " + str(failed_versions))

        return fetched_ids_and_schemas

    def validate_registered_vs_subjectschema(self):
        """
        Check successfully registered against schemas fetched by subject/schema
        """
        registered_ids_and_schemas = self.get_ids_and_schemas_registered()
        fetched_ids_and_schemas = self.fetch_ids_and_schemas_by_subjectschema(registered_ids_and_schemas)

        registered_ids_and_schemas = set(map(lambda r: (r[0], json.loads(r[1])["fields"][0]["name"]), registered_ids_and_schemas))
        fetched_ids_and_schemas = set(map(lambda r: (r[0], json.loads(r[1])["fields"][0]["name"]), fetched_ids_and_schemas))

        message = "Validating successfully registered ids agains ids fetched by subject/schema...\n"
        success = True

        registered_not_fetched = registered_ids_and_schemas - fetched_ids_and_schemas
        if len(registered_not_fetched) > 0:
            success = False
            message += "There are registered ids which were not fetched: " + str(registered_not_fetched) + "\n"

        fetched_not_registered = fetched_ids_and_schemas - registered_ids_and_schemas
        if len(fetched_not_registered) > 0:
            success = False
            message += "There are fetched ids which were not registered: " + str(fetched_not_registered) + "\n"

        message += "Success." if success else "Failure."
        return {"success": success, "message": message}


    def validate_registered_vs_subjectversion(self):
        """
        Check successfully registered against schemas fetched by subject/version
        """

        # Validate by fetching versions
        message = ""
        message += "Validating that successfully registered ids and schemas match ids and schemas " + \
                   "fetched by subject/version...\n"

        fetched_ids_and_schemas = set()
        success = True
        try:
            fetched_ids_and_schemas = set(self.fetch_ids_and_schemas_by_subjectversion())
        except:
            message += "Problem fetching by subject/version"
            success = False

        registered_ids_and_schemas = set(self.get_ids_and_schemas_registered())

        registered_not_fetched = registered_ids_and_schemas - fetched_ids_and_schemas
        if len(registered_not_fetched) > 0:
            message += "Some registered ids were not fetched by subject/version: " + str(registered_not_fetched) + "\n"
            success = False

        fetched_not_registered = fetched_ids_and_schemas - registered_ids_and_schemas
        if len(fetched_not_registered) > 0:
            message += "Some ids fetched by subject/version were not reported as successfully registered: " + str(fetched_not_registered) + "\n"
            success = False

        message += "Success." if success else "Failure."
        return {"success": success, "message": message}

    def validate_schema_consistency(self):
        """
        Much of the use case involves
        a) register a schema, get back an id
        b) sometime later, someone else fetches the schema by id

        Therefore, verify that the id we get back for registering a particular schema still gets us
        back that same particular schema.
        """
        master_id = self.schema_registry.idx(self.schema_registry.get_master_node())
        base_url = self.schema_registry.url(master_id)

        registration_data = self.register_driver.registration_data
        message = "Validating that registered schemas match fetched schemas...\n"
        discrepencies = []
        success = True
        for datum in registration_data:
            id = datum["schema_id"]
            schema = self.normalize_schema_string(datum["schema_string"])

            try:
                found_schema = self.normalize_schema_string(get_schema_by_id(base_url, id)["schema"])
            except:
                success = False
                message += "Failed to fetch id %d. " % id

            if found_schema != schema:
                discrepencies.append((id, schema, found_schema))

        success = success and len(discrepencies) == 0
        if len(discrepencies) > 0:
            message += "Found discrepencies between registered schemas and fetched schemas (id, registered, fetched). "
            message += str(discrepencies)

        message += "Success." if success else "Failure."
        return {"success": success, "message": message}

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

            if self.register_driver.ready_to_finish:
                self.logger.info("May have reached maximum registration time or maximum number of " +
                                 "registered schemas before finishing drive_failures.")
                break

        self.logger.info("Ending registration...")
        self.register_driver.ready_to_finish = True
        self.register_driver.wait()
        self.register_driver.stop()

        self.report_summary()

        time.sleep(10)
        self.tearDown()


class HadoopTest(Test):
    """
    Helper class that manages setting up a Hadoop cluster. Your run() method should
    call tearDown and setUp.
    """
    def __init__(self, cluster, num_hadoop, hadoop_distro='cdh', hadoop_version=2):
        super(HadoopTest, self).__init__(cluster)
        self.num_hadoop = num_hadoop
        self.hadoop = create_hadoop_service(cluster, num_hadoop, hadoop_distro, hadoop_version)

    def min_cluster_size(self):
        return self.num_hadoop

    def setUp(self):
        self.hadoop.start()

    def tearDown(self):
        self.hadoop.stop()


class CamusTest(Test):
    def __init__(self, cluster, num_zk, num_brokers, num_hadoop, num_schema_registry, num_rest,
                 hadoop_distro='cdh', hadoop_version=2, topics=None):
        super(CamusTest, self).__init__(cluster)
        self.num_zk = num_zk
        self.num_brokers = num_brokers
        self.num_hadoop = num_hadoop
        self.num_schema_registry = num_schema_registry
        self.num_rest = num_rest
        self.topics = topics
        self.hadoop_distro = hadoop_distro
        self.hadoop_version = hadoop_version

    def min_cluster_size(self):
        return self.num_zk + self.num_brokers + self.num_hadoop + self.num_schema_registry + self.num_rest

    def setUp(self):
        self.zk = ZookeeperService(self.cluster, self.num_zk)
        self.kafka = KafkaService(self.cluster, self.num_brokers, self.zk, topics=self.topics)
        self.hadoop = create_hadoop_service(self.cluster, self.num_hadoop, self.hadoop_distro, self.hadoop_version)
        self.schema_registry = SchemaRegistryService(self.cluster, self.num_schema_registry, self.zk, self.kafka)
        self.rest = KafkaRestService(self.cluster, self.num_rest, self.zk, self.kafka, self.schema_registry)

        self.zk.start()
        self.kafka.start()
        self.hadoop.start()
        self.schema_registry.start()
        self.rest.start()

    def tearDown(self):
        self.zk.stop()
        self.kafka.stop()
        self.hadoop.stop()
        self.schema_registry.stop()
        self.rest.stop()
