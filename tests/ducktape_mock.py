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
from ducktape.tests.test import TestContext
from ducktape.cluster.remoteaccount import RemoteAccount
from mock import MagicMock


import os
import tempfile


def mock_cluster():
    return MagicMock()


def session_context(cluster=mock_cluster(), **kwargs):
    """Return a SessionContext object"""

    if "results_dir" not in kwargs.keys():
        tmp = tempfile.mkdtemp()
        session_dir = os.path.join(tmp, "test_dir")
        os.mkdir(session_dir)
        kwargs["results_dir"] = session_dir

    return SessionContext(session_id="test_session", cluster=cluster, **kwargs)


def test_context(session_context=session_context()):
    """Return a TestContext object"""
    return TestContext(session_context=session_context)


class MockNode(object):
    """Mock cluster node"""
    def __init__(self):
        self.account = MockAccount()


class MockAccount(RemoteAccount):
    """Mock node.account object"""
    def __init__(self, hostname="localhost", user=None, ssh_args=None, ssh_hostname=None, logger=None):
        super(MockAccount, self).__init__(hostname, user, ssh_args, ssh_hostname, logger)
        self.externally_routable_ip = "localhost"
