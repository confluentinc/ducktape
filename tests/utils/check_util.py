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
import pytest

from ducktape.errors import TimeoutError
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

        with pytest.raises(TimeoutError, match="Hello world"):
            wait_until(lambda: time.time() > start + 5, timeout_sec=.5, backoff_sec=.1, err_msg="Hello world")

    def check_wait_until_timeout_callable_msg(self):
        """Check that timeout throws exception and the error message is generated via a callable"""
        start = time.time()

        with pytest.raises(TimeoutError, match="Hello world"):
            wait_until(lambda: time.time() > start + 5, timeout_sec=.5, backoff_sec=.1, err_msg=lambda: "Hello world")

    def check_wait_until_with_exception(self):
        def condition_that_raises():
            raise Exception("OG")
        with pytest.raises(TimeoutError) as exc_info:
            wait_until(condition_that_raises, timeout_sec=.5, backoff_sec=.1, err_msg="Hello world",
                       retry_on_exc=True)
        exc_chain = exc_info.getrepr(chain=True).chain
        # 2 exceptions in the chain - OG and Hello world
        assert len(exc_chain) == 2

        # each element of a chain is a tuple of traceback, "crash" (which is the short message)
        # and optionally descr, which is None for the bottom of the chain
        # and "The exception above is ..." for all the others
        # We're interested in crash, since that one is a one-liner with the actual exception message.
        og_message = str(exc_chain[0][1])
        hello_message = str(exc_chain[1][1])
        assert "OG" in og_message
        assert "Hello world" in hello_message

    def check_wait_until_with_exception_on_first_step_only_but_still_fails(self):
        start = time.time()

        def condition_that_raises_before_3():
            if time.time() < start + .3:
                raise Exception("OG")
            else:
                return False
        with pytest.raises(TimeoutError) as exc_info:
            wait_until(condition_that_raises_before_3, timeout_sec=.5, backoff_sec=.1, err_msg="Hello world",
                       retry_on_exc=True)
        exc_chain = exc_info.getrepr(chain=True).chain
        assert len(exc_chain) == 1
        hello_message = str(exc_chain[0][1])
        assert "Hello world" in hello_message

    def check_wait_until_exception_which_succeeds_eventually(self):
        start = time.time()

        def condition_that_raises_before_3_but_then_succeeds():
            if time.time() < start + .3:
                raise Exception("OG")
            else:
                return True
        wait_until(condition_that_raises_before_3_but_then_succeeds,
                   timeout_sec=.5, backoff_sec=.1, err_msg="Hello world", retry_on_exc=True)

    def check_wait_until_breaks_early_on_exception(self):
        def condition_that_raises():
            raise Exception("OG")
        with pytest.raises(Exception, match="OG") as exc_info:
            wait_until(condition_that_raises, timeout_sec=.5, backoff_sec=.1, err_msg="Hello world")
        assert "Hello world" not in str(exc_info)
