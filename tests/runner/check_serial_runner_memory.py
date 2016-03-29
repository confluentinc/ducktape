# Copyright 2015 Confluent Inc.
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

from ducktape.tests.test import Test
from ducktape.tests.runner import SerialTestRunner
from ducktape.services.service import Service
from ducktape.mark import MarkedFunctionExpander
from ducktape.cluster.localhost import LocalhostCluster
from ducktape.mark import matrix


import multiprocessing
import os
from memory_profiler import _get_memory
import statistics

import tests.ducktape_mock

from mock import Mock


MEMORY_EATER_LIST_SIZE = 10000000
N_TEST_CASES = 5


class MemoryEater(Service):
    """Simple service that has a reference to a list with many elements"""

    def __init__(self, context):
        super(MemoryEater, self).__init__(context, 1)
        self.items = []

    def start_node(self, node):
        self.items = [x for x in range(MEMORY_EATER_LIST_SIZE)]

    def stop_node(self, node):
        pass

    def clean_node(self, node):
        pass


class MemoryLeakTest(Test):
    """A group of identical "memory-hungry" ducktape tests.
    Each test holds a reference to a service which itself holds a reference to a large (memory intensive) object.
    """
    def __init__(self, test_context):
        super(MemoryLeakTest, self).__init__(test_context)
        self.memory_eater = MemoryEater(test_context)

    @matrix(x=[i for i in range(N_TEST_CASES)])
    def test_leak(self, x):
        self.memory_eater.start()


class InstrumentedSerialTestRunner(SerialTestRunner):
    """Identical to SerialTestRunner, except dump memory used by the current process
    before running each test.
    """
    def __init__(self, *args, **kwargs):
        self.queue = kwargs.get("queue")
        del kwargs["queue"]
        super(InstrumentedSerialTestRunner, self).__init__(*args, **kwargs)

    def run_single_test(self):
        # write current memory usage to file before running the test
        pid = os.getpid()
        current_memory = _get_memory(pid)
        self.queue.put(current_memory)

        data = super(InstrumentedSerialTestRunner, self).run_single_test()
        return data


class CheckMemoryUsage(object):
    def setup_method(self, _):
        mock_cluster = LocalhostCluster()
        self.session_context = tests.ducktape_mock.session_context(mock_cluster)

    def check_for_inter_test_memory_leak(self):
        """Until v0.3.10, ducktape had a serious source of potential memory leaks.

        Because test_context objects held a reference to all services for the duration of a test run, the memory
        used by any individual service would not be garbage-collected until well after *all* tests had run.

        This memory leak was discovered in Kafka system tests, where many long-running system tests were enough
        to cumulatively use up the memory on the test machine, causing a cascade of test failures due to
        inability to allocate any more memory.

        This test provides a regression check against this type of memory leak; it fails without the fix, and passes
        with it.
        """
        # Get a list of test_context objects for the test runner
        ctx_list = []
        test_methods = [MemoryLeakTest.test_leak]
        for f in test_methods:
            ctx_list.extend(MarkedFunctionExpander(session_context=self.session_context, cls=MemoryLeakTest, function=f).expand())
        assert len(ctx_list) == N_TEST_CASES  # Sanity check

        # Run all tests in another process
        queue = multiprocessing.Queue()
        runner = InstrumentedSerialTestRunner(self.session_context, ctx_list, queue=queue)
        runner.log = Mock()

        proc = multiprocessing.Process(target=runner.run_all_tests)
        proc.start()
        proc.join()

        measurements = []
        while not queue.empty():
            measurements.append(queue.get())
        self.validate_memory_measurements(measurements)

    def validate_memory_measurements(self, measurements):
        """A rough heuristic to check that stair-case style memory leak is not present.

        The idea is that when well-behaved, in this specific test, the maximum memory usage should be near the "middle".
         Here we check that the maximum usage is within 5% of the median memory usage.

        What is meant by stair-case? When the leak was present in its most blatant form,
         each repetition of MemoryLeak.test_leak run by the test runner adds approximately a fixed amount of memory
         without freeing much, resulting in a memory usage profile that looks like a staircase going up to the right.
        """
        median_usage = statistics.median(measurements)
        max_usage = max(measurements)

        usage_stats = "\nmax: %s,\nmedian: %s,\nall: %s\n" % (max_usage, median_usage, measurements)

        # we want to make sure that max usage doesn't exceed median usage by very much
        relative_diff = (max_usage - median_usage) / median_usage
        assert relative_diff <= .05, "max usage exceeded median usage by too much; there may be a memory leak: %s" % usage_stats

