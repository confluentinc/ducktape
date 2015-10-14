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

import json
import statistics


class RegressionTest(Test):

    """
    Only run if some test with matching TestKey gets run
    """

    @staticmethod
    def fetch_variable(variable_selector, datum):
        value = datum
        for selector in variable_selector:
            value = value[selector]
            if value is None:
                return None

        return value

    def __init__(self, regression_test_context):
        super(RegressionTest, self).__init__(regression_test_context)
        self.variable_selector = regression_test_context.variable_selector
        self.target_test_context = regression_test_context.target_test_context
        self.test_key = TestKey.from_test_context(self.target_test_context)

    def test(self):
        # get data for up to 20? previous runs
        result_store = self.test_context.session_context.result_store
        test_data = result_store.test_data(self.test_key)
        print "-"*50
        print test_data
        print "-"*50

        test_data = [d for d in test_data if d["status"].lower() == "pass"]
        test_data = [RegressionTest.fetch_variable(self.variable_selector, datum) for datum in test_data]

        test_data = [d for d in test_data if d is not None]

        num_data_points = min(20, len(test_data))
        test_data = test_data[-num_data_points:]
        return test_data


class RegressionTestContext(TestContext):
    def __init__(self, session_context, target_test_context=None, variable_selector=[]):
        self.target_test_context = target_test_context
        self.variable_selector = variable_selector

        super(RegressionTestContext, self).__init__(session_context, self.__module__, cls=RegressionTest, function=RegressionTest.test, injected_args={})

    @property
    def test_id(self):
        tid = super(RegressionTestContext, self).test_id
        parts = [
            tid,
            self.target_test_context.test_id,
            json.dumps(self.variable_selector, separators=(',', ':'))]
        return TestContext._TEST_ID_DELIMITER.join(parts)
