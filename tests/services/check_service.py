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

import threading

import pytest

from ducktape.services.service import Service
from tests.ducktape_mock import test_context, session_context
from ducktape.cluster.localhost import LocalhostCluster


class DummyService(Service):
    """Simple fake service class."""

    def __init__(self, context, num_nodes):
        super(DummyService, self).__init__(context, num_nodes)
        self.started_count = 0
        self.cleaned_count = 0
        self.stopped_count = 0

        self.started_kwargs = {}
        self.cleaned_kwargs = {}
        self.stopped_kwargs = {}

    def idx(self, node):
        return 1

    def start_node(self, node, **kwargs):
        super(DummyService, self).start_node(node, **kwargs)
        self.started_count += 1
        self.started_kwargs = kwargs

    def clean_node(self, node, **kwargs):
        super(DummyService, self).clean_node(node, **kwargs)
        self.cleaned_count += 1
        self.cleaned_kwargs = kwargs

    def stop_node(self, node, **kwargs):
        super(DummyService, self).stop_node(node, **kwargs)
        self.stopped_count += 1
        self.stopped_kwargs = kwargs


class DifferentDummyService(Service):
    """Another fake service class."""

    def __init__(self, context, num_nodes):
        super(DifferentDummyService, self).__init__(context, num_nodes)

    def idx(self, node):
        return 1


class CheckAllocateFree(object):
    def setup_method(self, _):
        self.cluster = LocalhostCluster()
        self.session_context = session_context()
        self.context = test_context(self.session_context, cluster=self.cluster)

    def check_allocate_free(self):
        """Check that allocating and freeing nodes works.

        This regression test catches the error with Service.free() introduced in v0.3.3 and fixed in v0.3.4
        """

        # Node allocation takes place during service instantiation
        initial_cluster_size = len(self.cluster)
        self.service = DummyService(self.context, 10)
        assert self.cluster.num_available_nodes() == initial_cluster_size - 10

        self.service.free()
        assert self.cluster.num_available_nodes() == initial_cluster_size

    def check_order(self):
        """Check expected behavior with service._order method"""
        self.dummy0 = DummyService(self.context, 4)
        self.diffDummy0 = DifferentDummyService(self.context, 100)
        self.dummy1 = DummyService(self.context, 1)
        self.diffDummy1 = DifferentDummyService(self.context, 2)
        self.diffDummy2 = DifferentDummyService(self.context, 5)

        assert self.dummy0._order == 0
        assert self.dummy1._order == 1
        assert self.diffDummy0._order == 0
        assert self.diffDummy1._order == 1
        assert self.diffDummy2._order == 2


class CheckStartStop(object):
    def setup_method(self, _):
        self.cluster = LocalhostCluster()
        self.session_context = session_context()
        self.context = test_context(self.session_context, cluster=self.cluster)

    def check_start_stop_clean(self):
        """
        Checks that start, stop, and clean invoke the expected per-node calls, and that start also runs stop and
        clean
        """
        service = DummyService(self.context, 2)

        service.start()
        assert service.started_count == 2
        assert service.stopped_count == 2
        assert service.cleaned_count == 2

        service.stop()
        assert service.stopped_count == 4

        service.start(clean=False)
        assert service.started_count == 4
        assert service.stopped_count == 6
        assert service.cleaned_count == 2

        service.stop()
        assert service.stopped_count == 8

        service.clean()
        assert service.cleaned_count == 4

    def check_kwargs_support(self):
        """Check that start, stop, and clean, and their per-node versions, can accept keyword arguments"""
        service = DummyService(self.context, 2)

        kwargs = {"foo": "bar"}
        service.start(**kwargs)
        assert service.started_kwargs == kwargs
        service.stop(**kwargs)
        assert service.stopped_kwargs == kwargs
        service.clean(**kwargs)
        assert service.cleaned_kwargs == kwargs


class RecordingService(Service):
    """Fake service that records which thread ran each per-node call, and can require the calls to overlap."""

    def __init__(self, context, num_nodes):
        super(RecordingService, self).__init__(context, num_nodes)
        self.lock = threading.Lock()
        self.calls = []
        self.barriers = {}
        self.failing = {}

    def idx(self, node):
        return self.nodes.index(node) + 1

    def _record(self, step, node):
        with self.lock:
            self.calls.append((step, self.idx(node), threading.current_thread()))
        if step in self.barriers:
            # Passes only if every node reaches this point while the others are still inside the same step
            self.barriers[step].wait(timeout=10)
        if (step, self.idx(node)) in self.failing:
            raise self.failing[(step, self.idx(node))]

    def start_node(self, node, **kwargs):
        self._record("start", node)

    def stop_node(self, node, **kwargs):
        self._record("stop", node)

    def clean_node(self, node, **kwargs):
        self._record("clean", node)

    def steps(self, step):
        return [idx for s, idx, _ in self.calls if s == step]

    def threads(self, step):
        return {thread for s, _, thread in self.calls if s == step}


class CheckParallelNodeOperations(object):
    def setup_method(self, _):
        self.cluster = LocalhostCluster()
        self.session_context = session_context()
        self.context = test_context(self.session_context, cluster=self.cluster)

    def check_serial_by_default(self):
        """Without opting in, every per-node call runs on the calling thread, in node order."""
        service = RecordingService(self.context, 3)

        service.start()
        service.stop()
        service.clean()

        assert {thread for _, _, thread in service.calls} == {threading.current_thread()}
        assert service.steps("start") == [1, 2, 3]
        assert service.steps("stop") == [1, 2, 3, 1, 2, 3]
        assert service.steps("clean") == [1, 2, 3, 1, 2, 3]

    def check_parallel_pre_start_cleanup(self):
        """The pre-start stop and clean overlap across nodes, while start_node stays serial."""
        service = RecordingService(self.context, 3)
        service.parallel_pre_start_cleanup = True
        service.barriers = {"stop": threading.Barrier(3), "clean": threading.Barrier(3)}

        service.start()

        assert sorted(service.steps("stop")) == [1, 2, 3]
        assert sorted(service.steps("clean")) == [1, 2, 3]
        assert len(service.threads("stop")) == 3
        # On each node, the stop still comes before the clean
        for idx in (1, 2, 3):
            node_calls = [s for s, i, _ in service.calls if i == idx]
            assert node_calls == ["stop", "clean", "start"]
        assert service.steps("start") == [1, 2, 3]
        assert service.threads("start") == {threading.current_thread()}

    def check_parallel_pre_start_cleanup_ignores_errors_and_clean_flag(self):
        service = RecordingService(self.context, 2)
        service.parallel_pre_start_cleanup = True
        service.failing = {("stop", 1): RuntimeError("nothing to stop")}

        service.start(clean=False)

        assert sorted(service.steps("stop")) == [1, 2]
        assert service.steps("clean") == []
        assert service.steps("start") == [1, 2]

    def check_parallel_start_stop_clean(self):
        service = RecordingService(self.context, 3)
        service.parallel_start = True
        service.parallel_stop = True
        service.parallel_clean = True

        service.start()
        service.calls = []
        service.barriers = {step: threading.Barrier(3) for step in ("start", "stop", "clean")}
        service.parallel_pre_start_cleanup = True
        service.start()
        service.stop()
        service.clean()

        for step in ("start", "stop", "clean"):
            assert sorted(set(service.steps(step))) == [1, 2, 3]
            assert threading.current_thread() not in service.threads(step)

    def check_parallel_failure_visits_all_nodes_and_raises_first(self):
        """A failing node does not keep the other nodes from being stopped; the earliest node's error is raised."""
        service = RecordingService(self.context, 3)
        service.parallel_stop = True
        first, second = RuntimeError("node 1"), RuntimeError("node 3")
        service.failing = {("stop", 1): first, ("stop", 3): second}

        with pytest.raises(RuntimeError) as raised:
            service.stop()

        assert raised.value is first
        assert sorted(service.steps("stop")) == [1, 2, 3]

    def check_serial_failure_stops_at_first_node(self):
        service = RecordingService(self.context, 3)
        service.failing = {("stop", 1): RuntimeError("node 1")}

        with pytest.raises(RuntimeError):
            service.stop()

        assert service.steps("stop") == [1]

    def check_single_node_runs_on_calling_thread(self):
        service = RecordingService(self.context, 1)
        service.parallel_stop = True

        service.stop()

        assert service.threads("stop") == {threading.current_thread()}
