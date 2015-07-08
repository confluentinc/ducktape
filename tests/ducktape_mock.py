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

from ducktape.tests.session import SessionContext

import os
import tempfile


class MockArgs(object):
    """A mock command-line argument object"""

    def __init__(self):
        self.test_path = []
        self.collect_only = False
        self.debug = False
        self.exit_first = False
        self.no_teardown = False


def session_context(cluster=None, args=MockArgs()):
    """Return a SessionContext object."""

    tmp = tempfile.mkdtemp()
    session_dir = os.path.join(tmp, "test_dir")
    os.mkdir(session_dir)
    return SessionContext("test_session", session_dir, cluster=cluster, args=args)
