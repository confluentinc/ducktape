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


import os
import time


class TestSessionContext(object):
    """Wrapper class for 'global' variables. A call to ducktape generates a single shared TestSessionContext object
    which helps route logging and reporting, etc.
    """

    def __init__(self, session_id, results_dir):
        """
        :type session_id: str   Global session identifier
        :type results_dir: str  All test results go here
        """
        self.session_id = session_id
        self.results_dir = os.path.abspath(results_dir)


def generate_session_id(session_id_file):
    """Generate a new session id based on the previous session id found in session_id_file
    :type session_id_file: str  Last-used session_id is in this file
    :rtype str                  New session_id
    """

    def get_id(day, num):
        return day + "--%03d" % num

    def split_id(an_id):
        day = an_id[:10]
        num = int(an_id[12:])
        return day, num

    def today():
        return time.strftime("%Y-%m-%d")

    def next_id(prev_id):
        if prev_id is None:
            prev_day = today()
            prev_num = 0
        else:
            prev_day, prev_num = split_id(prev_id)

        if prev_day == today():
            next_day = prev_day
            next_num = prev_num + 1
        else:
            next_day = today()
            next_num = 1

        return get_id(next_day, next_num)


    if os.path.isfile(session_id_file):
        with open(session_id_file, "r") as fp:
            session_id = next_id(fp.read())
    else:
        session_id = next_id(None)

    with open(session_id_file, "w") as fp:
        fp.write(session_id)

    return session_id


def generate_results_dir(session_id):
    """Results from a single run of ducktape are assigned a session_id and put together in this directory.

    :type session_id: str
    :rtype: str
    """
    return session_id + "-test-results"