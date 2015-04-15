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

import random


def mock_test_method():
    def test_method(self_obj):
        success = random.randint(0, 100) > 20
        if not success:
            raise AssertionError("Something bad happened!")
    return test_method


def mock_setup():
    def setup(self_obj):
        print "Setting up!"
    return setup


def mock_teardown():
    def teardown(self_obj):
        print "Tearing down!"
    return teardown


def swap_in_mock_run(test_classes):
    for tc in test_classes:
        tc.run = mock_test_method()


def swap_in_mock_fixtures(test_classes):
    for tc in test_classes:
        if hasattr(tc, "setUp"):
            tc.setUp = mock_setup()
        if hasattr(tc, "tearDown"):
            tc.tearDown = mock_teardown()