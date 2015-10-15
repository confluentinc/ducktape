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

from ducktape.tests.test import Test, TestContext
from ducktape.tests.result_store import TestKey
from ducktape.mark._parametrize import _inject

import json
import statistics


class RegressionTest(Test):
    pass


class DefaultRegressionTest(RegressionTest):
    """
    Only run if some test with matching TestKey gets run
    """

    @classmethod
    def create_test_context(cls, session_context, target_test_context, variable_selector):

        injected_args = {"target_test_id": target_test_context.test_id, "variable_selector": variable_selector}
        test_function =_inject(**injected_args)(cls.test)

        return TestContext(session_context, module=DefaultRegressionTest.__module__, cls=cls, function=test_function,
                           injected_args=injected_args)

    @staticmethod
    def fetch_variable(variable_selector, datum):
        value = datum
        for selector in variable_selector:
            value = value[selector]
            if value is None:
                return None

        return value

    def __init__(self, test_context, *args, **kwargs):
        super(DefaultRegressionTest, self).__init__(test_context, *args, **kwargs)

    def test(self, target_test_id, variable_selector):
        # get data for up to 20? previous runs
        result_store = self.test_context.session_context.result_store
        test_data = result_store.test_data(target_test_id)

        test_data = [d for d in test_data if d["status"].lower() == "pass" and d is not None]
        test_data = [DefaultRegressionTest.fetch_variable(variable_selector, datum) for datum in test_data]

        num_data_points = min(20, len(test_data))
        test_data = test_data[-num_data_points:]
        return test_data
