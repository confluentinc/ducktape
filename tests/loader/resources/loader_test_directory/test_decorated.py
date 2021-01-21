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
from ducktape.mark import matrix
from ducktape.mark import parametrize

NUM_TESTS = 17


class TestMatrix(Test):
    """8 tests here"""
    @matrix(x=[1, 2], y=["I'm", " a ", "test ", "matrix!"])
    def test_thing(self, x, y):
        pass


class TestStackedMatrix(Test):
    """4 tests"""
    @matrix(x=[1, 2], y=[-1, 0])
    def test_thing(self, x, y):
        pass


class TestParametrized(Test):
    @parametrize(x=10)
    def test_single_decorator(self, x=1, y="hi"):
        """1 test"""
        pass

    @parametrize(x=1, y=2)
    @parametrize(x="abc", y=[])
    def test_thing(self, x, y):
        """2 tests"""
        pass


class TestObjectParameters(Test):
    @parametrize(d={'a': 'A'}, lst=['whatever'])
    @parametrize(d={'z': 'Z'}, lst=['something'])
    def test_thing(self, d, lst):
        """2 tests"""
        pass
