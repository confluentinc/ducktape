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

from ducktape.tests.test import TestContext, Test
from ducktape.tests.runner import SerialTestRunner

import tests.ducktape_mock

from mock import MagicMock, Mock

import pytest


class CheckSerialRunner(object):

    def check_insufficient_cluster_resources(self):
        """The test runner should behave sensibly when the cluster is too small to run a given test."""
        mock_cluster = MagicMock()
        mock_cluster.__len__.return_value = 1
        session_context = tests.ducktape_mock.session_context(mock_cluster)

        test_context = TestContext(session_context, module=None, cls=TestThingy, function=TestThingy.test_pi)
        runner = SerialTestRunner(session_context, [test_context])
        runner.log = Mock()

        # Even though the cluster is too small, the test runner should this handle gracefully without raising an error
        runner.run_all_tests()

        # Check that setup of TestThingy raises error
        runner.current_test = TestThingy(test_context)
        runner.current_test_context = test_context
        with pytest.raises(RuntimeError):
            # The cluster should be too small to setup the test
            runner.setup_single_test()


class TestThingy(Test):
    """Fake ducktape test class"""

    def min_cluster_size(self):
        """ This test uses many nodes, wow!"""
        return 1000

    def test_pi(self):
        return {"data": 3.14159}