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

from ducktape.tests.test import Test
from ducktape.mark import matrix

"""All tests in this module fail"""


class FailsToInitTest(Test):
    """All tests in this class fail to initialize due to an exception in constructor
    """
    def __init__(self, test_context):
        super(FailsToInitTest, self).__init__(test_context)
        x = None
        x.split(':')

    @matrix(x=[_ for _ in range(2)])
    def test_nothing(self):
        self.logger.info('NOTHING')
