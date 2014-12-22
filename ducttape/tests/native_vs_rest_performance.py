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

from .test import RestProxyTest
from ducttape.services.performance import ProducerPerformanceService, RestProducerPerformanceService,\
    ConsumerPerformanceService, RestConsumerPerformanceService
import time, logging

class NativeVsRestProducerPerformance(RestProxyTest):
    def __init__(self, cluster):
        super(NativeVsRestProducerPerformance, self).__init__(cluster, num_zk=1, num_brokers=1, num_rest=1, topics={
            'test-rep-one' : { 'partitions': 6, 'replication-factor': 1 },
        })

    def run(self):
        self.setUp()

        msgs = 50000000
        msg_size = 100
        batch_size = 8196
        acks = 1 # default for REST proxy, which isn't yet configurable
        # These settings will work in the default local Vagrant VMs, useful for testing
        if False:
            msgs = 1000000
            msg_size = 100
            batch_size = 8196

        producer_perf = ProducerPerformanceService(
            self.cluster, 1, self.kafka,
            topic="test-rep-one", num_records=msgs, record_size=msg_size, throughput=-1,
            settings={'batch.size':batch_size, 'acks':acks}
        )
        rest_producer_perf = RestProducerPerformanceService(
            self.cluster, 1, self.rest,
            topic="test-rep-one", num_records=msgs, record_size=msg_size, batch_size=batch_size, throughput=-1
        )

        producer_perf.run()
        rest_producer_perf.run()

        self.tearDown()

        self.logger.info("Producer performance: %f per sec, %f ms", producer_perf.results[0]['records_per_sec'], producer_perf.results[0]['latency_99th_ms'])
        self.logger.info("REST Producer performance: %f per sec, %f ms", rest_producer_perf.results[0]['records_per_sec'], rest_producer_perf.results[0]['latency_99th_ms'])

class NativeVsRestConsumerPerformance(RestProxyTest):
    def __init__(self, cluster):
        super(NativeVsRestConsumerPerformance, self).__init__(cluster, num_zk=1, num_brokers=1, num_rest=1, topics={
            'test-rep-one' : { 'partitions': 6, 'replication-factor': 1 }
        })

    def run(self):
        self.setUp()

        msgs = 5000000
        msg_size = 100
        batch_size = 8196
        acks = 1 # default for REST proxy, which isn't yet configurable
        nthreads = 1 # not configurable for REST proxy
        # These settings will work in the default local Vagrant VMs, useful for testing
        if False:
            msgs = 1000000
            msg_size = 100
            batch_size = 8196

        # Seed data. FIXME currently the REST consumer isn't properly finishing
        # unless we have some extra messages -- the last set isn't getting
        # properly returned for some reason.
        producer = ProducerPerformanceService(
            self.cluster, 1, self.kafka,
            topic="test", num_records=msgs+1000, record_size=msg_size, throughput=-1,
            settings={'batch.size':batch_size, 'acks':acks}
        )
        producer.run()

        consumer_perf = ConsumerPerformanceService(
            self.cluster, 1, self.kafka,
            topic="test", num_records=msgs, throughput=-1, threads=nthreads
        )
        rest_consumer_perf = RestConsumerPerformanceService(
            self.cluster, 1, self.rest,
            topic="test", num_records=msgs, throughput=-1
        )
        consumer_perf.run()
        rest_consumer_perf.run()

        self.tearDown()

        self.logger.info("Consumer performance: %f MB/s, %f msg/sec", consumer_perf.results[0]['mbps'], consumer_perf.results[0]['records_per_sec'])
        self.logger.info("REST Consumer performance: %f MB/s, %f msg/sec", rest_consumer_perf.results[0]['mbps'], rest_consumer_perf.results[0]['records_per_sec'])

if __name__ == "__main__":
    NativeVsRestProducerPerformance.run_standalone()
    NativeVsRestConsumerPerformance.run_standalone()
