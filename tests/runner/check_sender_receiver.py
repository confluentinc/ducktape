# Copyright 2021 Confluent Inc.
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

from ducktape.cluster.localhost import LocalhostCluster
from tests.ducktape_mock import test_context, session_context

import logging
import pytest
from ducktape.tests.runner_client import Sender
from ducktape.tests.runner import Receiver
from ducktape.tests.event import ClientEventFactory, EventResponseFactory
from ducktape.errors import TimeoutError

import multiprocessing as mp
import os


class CheckSenderReceiver(object):
    def ready_response(self, client_id, port):
        sender_event_factory = ClientEventFactory("test_1", 0, client_id)
        sender = Sender(server_host='localhost', server_port=port,
                        message_supplier=sender_event_factory, logger=logging)
        sender.send(sender_event_factory.ready())

    def check_simple_messaging(self):
        s_context = session_context()
        cluster = LocalhostCluster(num_nodes=1000)
        t_context = test_context(s_context, cluster)

        client_id = "test-runner-{}-{}".format(os.getpid(), id(self))
        receiver_response_factory = EventResponseFactory()

        receiver = Receiver(5556, 5656)
        receiver.start()
        port = receiver.port

        try:
            p = mp.Process(target=self.ready_response, args=(client_id, port))
            p.start()

            event = receiver.recv(timeout=10000)
            assert event["event_type"] == ClientEventFactory.READY
            logging.info('replying to client')
            receiver.send(receiver_response_factory.ready(event, s_context, t_context, cluster))
        finally:
            p.join()

    def check_timeout(self):
        client_id = "test-runner-{}-{}".format(os.getpid(), id(self))

        receiver = Receiver(5556, 5656)
        receiver.start()
        port = receiver.port

        try:
            p = mp.Process(target=self.ready_response, args=(client_id, port))
            p.start()
            with pytest.raises(TimeoutError):
                receiver.recv(timeout=0)
        finally:
            p.join()
