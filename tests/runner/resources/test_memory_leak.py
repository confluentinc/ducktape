# Copyright 2016 Confluent Inc.
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
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.services.service import Service
from ducktape.mark import matrix


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

    @property
    def num_nodes(self):
        return 1


class MemoryLeakTest(Test):
    """A group of identical "memory-hungry" ducktape tests.
    Each test holds a reference to a service which itself holds a reference to a large (memory intensive) object.
    """

    def __init__(self, test_context):
        super(MemoryLeakTest, self).__init__(test_context)
        self.memory_eater = MemoryEater(test_context)

    @cluster(num_nodes=100)
    @matrix(x=[i for i in range(N_TEST_CASES)])
    def test_leak(self, x):
        self.memory_eater.start()
