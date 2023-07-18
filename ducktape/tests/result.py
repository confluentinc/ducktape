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

import json
import os
import time

from ducktape.tests.test import TestContext
from ducktape.json_serializable import DucktapeJSONEncoder
from ducktape.tests.reporter import SingleResultFileReporter
from ducktape.utils.local_filesystem_utils import mkdir_p
from ducktape.utils.util import ducktape_version
from ducktape.tests.status import FLAKY, PASS, FAIL, IGNORE


class TestResult(object):
    """Wrapper class for a single result returned by a single test."""

    def __init__(self,
                 test_context,
                 test_index,
                 session_context,
                 test_status=PASS,
                 summary="",
                 data=None,
                 start_time=-1,
                 stop_time=-1):
        """
        @param test_context  standard test context object
        @param test_status   did the test pass or fail, etc?
        @param summary       summary information
        @param data          data returned by the test, e.g. throughput
        """
        self.nodes_allocated = len(test_context.cluster)
        self.nodes_used = test_context.cluster.max_used_nodes
        if hasattr(test_context, "services"):
            self.services = test_context.services.to_json()
        else:
            self.services = {}

        self.test_id = test_context.test_id
        self.module_name = test_context.module_name
        self.cls_name = test_context.cls_name
        self.function_name = test_context.function_name
        self.injected_args = test_context.injected_args
        self.description = test_context.description
        self.results_dir = TestContext.results_dir(test_context, test_index)

        self.test_index = test_index

        self.session_context = session_context
        self.test_status = test_status
        self.summary = summary
        self.data = data
        self.file_name = test_context.file

        self.base_results_dir = session_context.results_dir
        if not self.results_dir.endswith(os.path.sep):
            self.results_dir += os.path.sep
        if not self.base_results_dir.endswith(os.path.sep):
            self.base_results_dir += os.path.sep
        assert self.results_dir.startswith(self.base_results_dir)
        self.relative_results_dir = self.results_dir[len(self.base_results_dir):]

        # For tracking run time
        self.start_time = start_time
        self.stop_time = stop_time

    def __repr__(self):
        return "<%s - test_status:%s, data:%s>" % (self.__class__.__name__, self.test_status, str(self.data))

    @property
    def run_time_seconds(self):
        if self.start_time < 0:
            return -1
        if self.stop_time < 0:
            return time.time() - self.start_time

        return self.stop_time - self.start_time

    def report(self):
        if not os.path.exists(self.results_dir):
            mkdir_p(self.results_dir)

        self.dump_json()
        test_reporter = SingleResultFileReporter(self)
        test_reporter.report()

    def dump_json(self):
        """Dump this object as json to the given location. By default, dump into self.results_dir/report.json"""
        with open(os.path.join(self.results_dir, "report.json"), "w") as fd:
            json.dump(self, fd, cls=DucktapeJSONEncoder, sort_keys=True, indent=2)

    def to_json(self):
        return {
            "test_id": self.test_id,
            "module_name": self.module_name,
            "cls_name": self.cls_name,
            "function_name": self.function_name,
            "injected_args": self.injected_args,
            "description": self.description,
            "results_dir": self.results_dir,
            "relative_results_dir": self.relative_results_dir,
            "base_results_dir": self.base_results_dir,
            "test_status": self.test_status,
            "summary": self.summary,
            "data": self.data,
            "start_time": self.start_time,
            "stop_time": self.stop_time,
            "run_time_seconds": self.run_time_seconds,
            "nodes_allocated": self.nodes_allocated,
            "nodes_used": self.nodes_used,
            "services": self.services
        }


class TestResults(object):
    """Class used to aggregate individual TestResult objects from many tests."""

    def __init__(self, session_context, cluster, client_status):
        """
        :type session_context: ducktape.tests.session.SessionContext
        """
        self._results = []
        self.session_context = session_context
        self.cluster = cluster

        # For tracking total run time
        self.start_time = -1
        self.stop_time = -1
        self.client_status = client_status

    def append(self, obj):
        return self._results.append(obj)

    def __len__(self):
        return len(self._results)

    def __iter__(self):
        return iter(self._results)

    @property
    def num_passed(self):
        return len([r for r in self._results if r.test_status == PASS])

    @property
    def num_failed(self):
        return len([r for r in self._results if r.test_status == FAIL])

    @property
    def num_ignored(self):
        return len([r for r in self._results if r.test_status == IGNORE])

    @property
    def num_flaky(self):
        return len([r for r in self._results if r.test_status == FLAKY])

    @property
    def run_time_seconds(self):
        if self.start_time < 0:
            return -1
        if self.stop_time < 0:
            self.stop_time = time.time()

        return self.stop_time - self.start_time

    def get_aggregate_success(self):
        """Check cumulative success of all tests run so far
        :rtype: bool
        """
        for result in self._results:
            if result.test_status == FAIL:
                return False
        return True

    def _stats(self, num_list):
        if len(num_list) == 0:
            return {
                "mean": None,
                "min": None,
                "max": None
            }

        return {
            "mean": sum(num_list) / float(len(num_list)),
            "min": min(num_list),
            "max": max(num_list)
        }

    def to_json(self):
        if self.run_time_seconds == 0 or len(self.cluster) == 0:
            # If things go horribly wrong, the test run may be effectively instantaneous
            # Let's handle this case gracefully, and avoid divide-by-zero
            cluster_utilization = 0
            parallelism = 0
        else:
            cluster_utilization = (1.0 / len(self.cluster)) * (1.0 / self.run_time_seconds) * \
                sum([r.nodes_used * r.run_time_seconds for r in self])
            parallelism = sum([r.run_time_seconds for r in self._results]) / self.run_time_seconds
        result = {
            "ducktape_version": ducktape_version(),
            "session_context": self.session_context,
            "run_time_seconds": self.run_time_seconds,
            "start_time": self.start_time,
            "stop_time": self.stop_time,
            "run_time_statistics": self._stats([r.run_time_seconds for r in self]),
            "cluster_nodes_used": self._stats([r.nodes_used for r in self]),
            "cluster_nodes_allocated": self._stats([r.nodes_allocated for r in self]),
            "cluster_utilization": cluster_utilization,
            "cluster_num_nodes": len(self.cluster),
            "num_passed": self.num_passed,
            "num_failed": self.num_failed,
            "num_ignored": self.num_ignored,
            "parallelism": parallelism,
            "client_status": {str(key): value for key, value in self.client_status.items()},
            "results": [r for r in self._results]
        }
        if self.num_flaky:
            result['num_flaky'] = self.num_flaky
        return result
