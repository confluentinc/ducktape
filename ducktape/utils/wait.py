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

#
# Conscious waiting
#
#
#

import time
import traceback
import os
import errno
import signal

class TimeoutError (Exception):
    pass

time_saved_sum = 0

def time_saved ():
    """ Returns the amount of time saved using wait.until() instead of time.sleep() """
    global time_saved_sum
    return time_saved_sum


def sleep (seconds):
    """ Simple wrapper for time.sleep() but indicates that the programmer
        made a conscious decisions not to use wait.until() """
    return time.sleep(seconds)

def until0 (timeout, expect_result, test_func, *args):
    """ Wait up to 'timeout' seconds for 'test_func(*args)' to return 'expect_result'.
        Raises TimeoutError() when 'timeout' is reached, else True if function
        returned expected results in time.
        'test_func(*args)' is called repeatedly for the duration of the 'timeout' period,
        with a small pause between each invocation to avoid busy looping.
    """
    start_time = time.time()
    abs_tmout = start_time + timeout

    interval = timeout / 50.0

    while time.time() < abs_tmout:
        r = False
        try:
            r = test_func(*args)
        except:
            traceback.print_exc()
            r = False
        if r == expect_result:
            global time_saved_sum
            time_saved_sum += abs_tmout - time.time()
            return True
        time.sleep(interval)

    raise TimeoutError('Timed out (%ds) waiting for %s(%s)' % (timeout, test_func, args))


def until (timeout, test_func, *args):
    """ Wait up to 'timeout' seconds for 'test_func(*args)' to return True.
        Raises TimeoutError() when 'timeout' is reached, else True if function
        returned expected results in time.
    """
    return until0(timeout, True, test_func, *args)

def until_not (timeout, test_func, *args):
    """ Wait up to 'timeout' seconds for 'test_func(*args)' to return False.
        Raises TimeoutError() when 'timeout' is reached, else True if function
        returned expected results in time.
    """
    return until0(timeout, False, test_func, *args)
