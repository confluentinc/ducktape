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

NUM_TESTS = 3


class TestB(Test):
    """Loader should discover this."""

    def test_b(self):
        pass


class TestBB(Test):
    """Loader should discover this with 2 tests."""
    test_not_callable = 3

    def test_bb_one(self):
        pass

    def bb_two_test(self):
        pass

    def other_method(self):
        pass


class TestInvisible(object):
    """Loader should not discover this."""

    def test_invisible(self):
        pass
