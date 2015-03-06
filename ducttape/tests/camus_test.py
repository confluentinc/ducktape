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
from .test import CamusTest
from ducttape.services.performance import CamusPerformanceService


class CamusHadoopV1Test(CamusTest):
    """
    7 machines are required at minimum: Zookeeper 1, Kafka 1, Hadoop 2,
    SchemaRegistry 1,  KafkaRest 1,
    CamusPerformance 1
    """

    def __init__(self, cluster):
        super(CamusHadoopV1Test, self).__init__(
            cluster,
            num_zk=1,
            num_brokers=1,
            num_hadoop=2,
            num_schema_registry=1,
            num_rest=1,
            hadoop_distro='cdh',
            hadoop_version=1,
            topics={"testAvro": None})

        self.num_camus_perf = 1

    def min_cluster_size(self):
        return self.num_camus_perf + super(CamusHadoopV1Test, self).min_cluster_size()

    def run(self):
        self.setUp()
        self.logger.info("Running Camus example on Hadoop distribution %s, Hadoop version %d",
                         self.hadoop_distro, self.hadoop_version)
        camus_perf = CamusPerformanceService(
            self.cluster, self.num_camus_perf, self.kafka, self.hadoop, self.schema_registry, self.rest, settings={})
        camus_perf.run()
        self.tearDown()

if __name__ == "__main__":
    CamusHadoopV1Test.run_standalone()


class CamusHadoopV2Test(CamusTest):
    """
    7 machines are required at minimum: Zookeeper 1, Kafka 1, Hadoop 2,
    SchemaRegistry 1,  KafkaRest 1,
    CamusPerformance 1
    """

    def __init__(self, cluster):
        super(CamusHadoopV2Test, self).__init__(
            cluster,
            num_zk=1,
            num_brokers=1,
            num_hadoop=2,
            num_schema_registry=1,
            num_rest=1,
            hadoop_distro='cdh',
            hadoop_version=2,
            topics={"testAvro": None})

        self.num_camus_perf = 1

    def min_cluster_size(self):
        return self.num_camus_perf + super(CamusHadoopV2Test, self).min_cluster_size()

    def run(self):
        self.setUp()
        self.logger.info("Running Camus example on Hadoop distribution %s, Hadoop version %d",
                         self.hadoop_distro, self.hadoop_version)
        camus_perf = CamusPerformanceService(
            self.cluster, self.num_camus_perf, self.kafka, self.hadoop, self.schema_registry, self.rest, settings={})
        camus_perf.run()
        self.tearDown()

if __name__ == "__main__":
    CamusHadoopV2Test.run_standalone()


class CamusHDPTest(CamusTest):
    """
    7 machines are required at minimum: Zookeeper 1, Kafka 1, Hadoop 2,
    SchemaRegistry 1,  KafkaRest 1,
    CamusPerformance 1
    """

    def __init__(self, cluster):
        super(CamusHDPTest, self).__init__(
            cluster,
            num_zk=1,
            num_brokers=1,
            num_hadoop=2,
            num_schema_registry=1,
            num_rest=1,
            hadoop_distro='hdp',
            hadoop_version=2,
            topics={"testAvro": None})

        self.num_camus_perf = 1

    def min_cluster_size(self):
        return self.num_camus_perf + super(CamusHDPTest, self).min_cluster_size()

    def run(self):
        self.setUp()
        self.logger.info("Running Camus example on Hadoop distribution %s, Hadoop version %d",
                         self.hadoop_distro, self.hadoop_version)
        camus_perf = CamusPerformanceService(
            self.cluster, self.num_camus_perf, self.kafka, self.hadoop, self.schema_registry, self.rest, settings={})
        camus_perf.run()
        self.tearDown()

if __name__ == "__main__":
    CamusHDPTest.run_standalone()