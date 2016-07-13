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

import os
import time


class Request(object):
    READY = "READY"  # reply: {test_metadata, cluster, session_context}
    SETTING_UP = "SETTING_UP"
    RUNNING = "RUNNING"
    TEARING_DOWN = "TEARING_DOWN"
    FINISHED = "FINISHED"
    LOG = "LOG"

    def __init__(self, source_id):
        # id of event source
        self.source_id = source_id

    def create_event(self, event_type, payload=None):
        if payload is None:
            payload = {}

        event = {
            "source_id": self.source_id,
            "event_type": event_type,
            "event_time": time.time()
        }

        assert len(set(event.keys()).intersection(set(payload.keys()))) == 0, \
            "Payload and base event should not share keys. base event: %s, payload: %s" % (str(event), str(payload))

        event. update(payload)
        return event

    def running(self):
        return self.create_event(
            event_type=Request.RUNNING,
            payload={
                "pid": os.getpid(),
                "pgroup_id": os.getpgrp()
            }
        )

    def ready(self):
        return self.create_event(
            event_type=Request.READY,
            payload={
                "pid": os.getpid(),
                "pgroup_id": os.getpgrp()
            }
        )

    def setting_up(self):
        return self.create_event(
            event_type=Request.SETTING_UP
        )

    def finished(self, result):
        return self.create_event(
            event_type=Request.FINISHED,
            payload={
                "result": result
            }
        )

    def log(self, message, level):
        return self.create_event(
            event_type=Request.LOG,
            payload={
                "message": message,
                "log_level": level
            }
        )


def ready_reply(session_context, test_context, cluster):
    ready_reply = {
        "session_context": session_context,
        "test_metadata": test_context.test_metadata,
        "cluster": cluster
    }

    return ready_reply
