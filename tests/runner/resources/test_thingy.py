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

import time
from ducktape.tests.test import Test
from ducktape.mark import ignore, parametrize
from ducktape.mark.resource import cluster


_flake = False


class TestThingy(Test):
    """Fake ducktape test class"""

    @cluster(num_nodes=1000)
    def test_pi(self):
        return {"data": 3.14159}

    @cluster(num_nodes=1000)
    def test_delayed(self):
        time.sleep(1)

    @cluster(num_nodes=1000)
    @ignore
    def test_ignore1(self):
        pass

    @cluster(num_nodes=1000)
    @ignore(x=5)
    @parametrize(x=5)
    def test_ignore2(self, x=2):
        pass

    @cluster(num_nodes=1000)
    def test_failure(self):
        raise Exception("This failed")

    @cluster(num_nodes=1000)
    def test_flaky(self):
        global _flake
        flake, _flake = _flake, not _flake
        assert flake


class ClusterTestThingy(Test):
    """Fake ducktape test class"""

    @cluster(num_nodes=10)
    def test_bad_num_nodes(self):
        pass
