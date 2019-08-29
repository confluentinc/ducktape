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


from ducktape.utils.util import wait_until
import time


class CheckUtils(object):

    def check_wait_until(self):
        """Check normal wait until behavior"""
        start = time.time()

        wait_until(lambda: time.time() > start + .5, timeout_sec=2, backoff_sec=.1)

    def check_wait_until_timeout(self):
        """Check that timeout throws exception"""
        start = time.time()

        try:
            wait_until(lambda: time.time() > start + 5, timeout_sec=.5, backoff_sec=.1, err_msg="Hello world")
            raise Exception("This should have timed out")
        except Exception as e:
            assert str(e) == "Hello world"

    def check_wait_until_timeout_callable_msg(self):
        """Check that timeout throws exception and the error message is generated via a callable"""
        start = time.time()

        try:
            wait_until(lambda: time.time() > start + 5, timeout_sec=.5, backoff_sec=.1, err_msg=lambda: "Hello world")
            raise Exception("This should have timed out")
        except Exception as e:
            assert str(e) == "Hello world"
