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
