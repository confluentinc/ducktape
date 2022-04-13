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

from ducktape.tests.runner import TestRunner
from ducktape.mark.mark_expander import MarkedFunctionExpander
from ducktape.cluster.localhost import LocalhostCluster

from .resources.test_memory_leak import MemoryLeakTest

import math
from memory_profiler import memory_usage
import os
from six.moves import queue
import statistics
from statistics import mean

import tests.ducktape_mock

from mock import Mock


N_TEST_CASES = 5


MEMORY_LEAK_TEST_FILE = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "resources/test_memory_leak.py"
    )
)


class InstrumentedTestRunner(TestRunner):
    """Identical to TestRunner, except dump memory used by the current process
    before running each test.
    """

    def __init__(self, *args, **kwargs):
        self.queue = kwargs.get("queue")
        del kwargs["queue"]
        super(InstrumentedTestRunner, self).__init__(*args, **kwargs)

    def _run_single_test(self, test_context):
        # write current memory usage to file before running the test
        pid = os.getpid()
        current_memory = memory_usage(pid)[0]
        self.queue.put(current_memory)

        super(InstrumentedTestRunner, self)._run_single_test(test_context)


class CheckMemoryUsage(object):
    def setup_method(self, _):
        self.cluster = LocalhostCluster(num_nodes=100)
        self.session_context = tests.ducktape_mock.session_context()

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
            ctx_list.extend(MarkedFunctionExpander(session_context=self.session_context, cls=MemoryLeakTest, function=f,
                                                   file=MEMORY_LEAK_TEST_FILE, cluster=self.cluster).expand())
        assert len(ctx_list) == N_TEST_CASES  # Sanity check

        q = queue.Queue()
        runner = InstrumentedTestRunner(self.cluster, self.session_context, Mock(), ctx_list, 1, queue=q)
        runner.run_all_tests()

        measurements = []
        while not q.empty():
            measurements.append(q.get())
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
        slope = self._linear_regression_slope(measurements)

        if slope > 0:
            # check max memory usage iff the memory measurements seem to be increasing overall
            assert relative_diff <= .05, "max usage exceeded median usage by too much; there may " \
                                         "be a memory leak: %s" % usage_stats

    def _linear_regression_slope(self, arr):
        """Return the sign of the slope of the least squares fit line.
        """
        assert len(arr) > 0

        x_vals = [i for i in range(len(arr))]
        mean_x = mean(x_vals)
        mean_y = mean(arr)

        #            mean([x_i * y_i]) - mean_x * mean_y
        # slope =    -----------------------------------
        #                       variance([x_i])
        #
        # where variance is (1/N) * sum([(x_i - mean_x)^2])
        #
        # the denominator in regression formula is always positive, so it's enough to compute the numerator

        slope_numerator = mean([i * arr[i] for i in x_vals])
        slope_numerator = slope_numerator - (mean_x * mean_y)

        # return the sign
        return math.copysign(slope_numerator, 1)
