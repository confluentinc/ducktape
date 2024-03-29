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

from __future__ import print_function

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.mark import matrix

"""All tests in this module fail"""


class FailingTest(Test):
    def __init__(self, test_context):
        super(FailingTest, self).__init__(test_context)

    @cluster(num_nodes=1000)
    @matrix(x=[_ for _ in range(2)])
    def test_fail(self, x):
        print("Test %s fails!" % x)
        raise RuntimeError("This test throws an error!")
