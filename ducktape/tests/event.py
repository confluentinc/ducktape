# Copyright 2016 Confluent Inc.
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

import copy
import os
import time


class ClientEventFactory(object):
    """Used by test runner clients to generate events."""

    READY = "READY"  # reply: {test_metadata, cluster, session_context}
    SETTING_UP = "SETTING_UP"
    RUNNING = "RUNNING"
    TEARING_DOWN = "TEARING_DOWN"
    FINISHED = "FINISHED"
    LOG = "LOG"

    # Types of messages available
    TYPES = {READY, SETTING_UP, RUNNING, TEARING_DOWN, FINISHED, LOG}

    def __init__(self, test_id, test_index, source_id):
        self.test_id = test_id
        # id of event source
        self.test_index = test_index
        self.source_id = source_id
        self.event_id = 0

    def _event(self, event_type, payload=None):
        """Create a message object with certain base fields, and augmented by the payload.

        :param event_type: type of message this is
        :param payload: a dict containing extra fields for the message. Key names should not conflict with keys
            in the base event.
        """
        assert event_type in ClientEventFactory.TYPES, "Unknown event type"
        if payload is None:
            payload = {}

        event = {
            "test_id": self.test_id,
            "source_id": self.source_id,
            "test_index": self.test_index,
            "event_id": self.event_id,
            "event_type": event_type,
            "event_time": time.time()
        }

        assert len(set(event.keys()).intersection(set(payload.keys()))) == 0, \
            "Payload and base event should not share keys. base event: %s, payload: %s" % (str(event), str(payload))

        event. update(payload)
        self.event_id += 1
        return event

    def copy(self, event):
        """Copy constructor: return a copy of the original message, but with a unique message id."""
        new_event = copy.copy(event)
        new_event["message_id"] = self.event_id
        self.event_id += 1

        return new_event

    def running(self):
        return self._event(
            event_type=ClientEventFactory.RUNNING,
            payload={
                "pid": os.getpid(),
                "pgroup_id": os.getpgrp()
            }
        )

    def ready(self):
        return self._event(
            event_type=ClientEventFactory.READY,
            payload={
                "pid": os.getpid(),
                "pgroup_id": os.getpgrp()
            }
        )

    def setting_up(self):
        return self._event(
            event_type=ClientEventFactory.SETTING_UP
        )

    def finished(self, result):
        return self._event(
            event_type=ClientEventFactory.FINISHED,
            payload={
                "result": result
            }
        )

    def log(self, message, level):
        return self._event(
            event_type=ClientEventFactory.LOG,
            payload={
                "message": message,
                "log_level": level
            }
        )


class EventResponseFactory(object):
    """Used by the test runner to create responses to events from client processes."""

    def _event_response(self, client_event, payload=None):
        if payload is None:
            payload = {}

        event_response = {
            "ack": True,
            "source_id": client_event["source_id"],
            "event_id": client_event["event_id"]
        }

        assert len(set(event_response.keys()).intersection(set(payload.keys()))) == 0, \
            "Payload and base event should not share keys. base event: %s, payload: %s" % (
                str(event_response), str(payload))

        event_response.update(payload)
        return event_response

    def running(self, client_event):
        return self._event_response(client_event)

    def ready(self, client_event, session_context, test_context, cluster):
        payload = {
            "session_context": session_context,
            "test_metadata": test_context.test_metadata,
            "cluster": cluster
        }

        return self._event_response(client_event, payload)

    def setting_up(self, client_event):
        return self._event_response(client_event)

    def finished(self, client_event):
        return self._event_response(client_event)

    def log(self, client_event):
        return self._event_response(client_event)
