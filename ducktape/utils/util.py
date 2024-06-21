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

from ducktape import __version__ as __ducktape_version__
from ducktape.errors import TimeoutError

import importlib
import time


def wait_until(condition, timeout_sec, backoff_sec=.1, err_msg="", retry_on_exc=False):
    """Block until condition evaluates as true or timeout expires, whichever comes first.

    :param condition: callable that returns True if the condition is met, False otherwise
    :param timeout_sec: number of seconds to check the condition for before failing
    :param backoff_sec: number of seconds to back off between each failure to meet the condition before checking again
    :param err_msg: a string or callable returning a string that will be included as the exception message if the
                    condition is not met
    :param retry_on_exc: if True, will retry if condition raises an exception. If condition raised exception on last
                         iteration, that exception will be raised as a cause of TimeoutError.
                         If False and condition raises an exception, that exception will be forwarded to the caller
                         immediately.
                         Defaults to False (original ducktape behavior).
                         # TODO: [1.0.0] flip this to True
    :return: silently if condition becomes true within the timeout window, otherwise raise Exception with the given
    error message.
    """
    start = time.time()
    stop = start + timeout_sec
    last_exception = None
    while time.time() < stop:
        try:
            if condition():
                return
            else:
                # reset last_exception if last iteration didn't raise any exception, but simply returned False
                last_exception = None
        except BaseException as e:
            # save last raised exception for logging it later
            last_exception = e
            if not retry_on_exc:
                raise e
        finally:
            time.sleep(backoff_sec)

    # it is safe to call Exception from None - will be just treated as a normal exception
    raise TimeoutError(err_msg() if callable(err_msg) else err_msg) from last_exception


def package_is_installed(package_name):
    """Return true iff package can be successfully imported."""
    try:
        importlib.import_module(package_name)
        return True
    except Exception:
        return False


def ducktape_version():
    """Return string representation of current ducktape version."""
    return __ducktape_version__


def load_function(func_module_path):
    """Loads and returns a function from a module path seperated by '.'s"""
    module, function = func_module_path.rsplit(".", 1)
    try:
        return getattr(importlib.import_module(module), function)
    except AttributeError:
        raise Exception("Function could not be loaded from the module path {}, "
                        "verify that it is '.' seperated".format(func_module_path))
