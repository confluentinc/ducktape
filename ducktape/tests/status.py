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


class TestStatus(object):
    def __init__(self, status):
        self._status = str(status).lower()

    def __eq__(self, other):
        return str(self).lower() == str(other).lower()

    def __str__(self):
        return self._status

    def to_json(self):
        return str(self).upper()


PASS = TestStatus("pass")
FLAKY = TestStatus("flaky")
FAIL = TestStatus("fail")
IGNORE = TestStatus("ignore")
