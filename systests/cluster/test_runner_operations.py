# Copyright 2022 Confluent Inc.
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
from ducktape.services.service import Service
from ducktape.tests.test import Test
from ducktape.mark.resource import cluster
import time


class SimpleEchoService(Service):
    """Simple service that allocates one node for performing tests of RemoteAccount functionality"""
    logs = {
        "my_log": {
            "path": "/tmp/log",
            "collect_default": True
        },
    }

    def __init__(self, context):
        super(SimpleEchoService, self).__init__(context, num_nodes=1)
        self.count = 0

    def echo(self):
        self.nodes[0].account.ssh("echo {} >> /tmp/log".format(self.count))
        self.count += 1


class SimpleRunnerTest(Test):
    def setup(self):
        self.service = SimpleEchoService(self.test_context)

    @cluster(num_nodes=1)
    def timeout_test(self):
        """
        a simple longer running test to test special run flags agaisnt.
        """
        self.service.start()

        while self.service.count < 100000000:
            self.service.echo()
            time.sleep(.2)
