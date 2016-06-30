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

from ducktape.tests.session import generate_results_dir, SessionContext
from ducktape.cluster.localhost import LocalhostCluster

import os.path
import pickle
import shutil
import tempfile


class CheckGenerateResultsDir(object):
    def setup_method(self, _):
        self.tempdir = tempfile.mkdtemp()

    def check_generate_results_root(self):
        """Check the generated results directory has the specified path as its root"""
        results_root = os.path.abspath(tempfile.mkdtemp())
        results_dir = generate_results_dir(results_root, "my_session_id")
        assert results_dir.find(results_root) == 0

    def check_pickleable(self):
        """Check that session_context object is pickleable
        This is necessary so that SessionContext objects can be shared between processes when using python
        multiprocessing module.
        """

        kwargs = {
            "session_id": "hello-123",
            "results_dir": self.tempdir,
            "cluster": LocalhostCluster(),
            "globals": {}
        }
        session_context = SessionContext(**kwargs)
        pickle.dumps(session_context)

    def teardown_method(self, _):
        if os.path.exists(self.tempdir):
            shutil.rmtree(self.tempdir)
