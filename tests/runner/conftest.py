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

from unittest.mock import patch

import pytest

from ducktape.tests.runner import Receiver


@pytest.fixture(autouse=True)
def destroy_receiver_contexts():
    """Destroy the zmq context of every Receiver built during a test.

    A TestRunner that is built but never run leaves its context bound with an open socket, and
    only run_all_tests() closes it. Collecting such a context later blocks forever in
    Context.term(), which stalls whichever unrelated test happens to trigger the collection.
    """
    receivers = []
    real_init = Receiver.__init__

    def tracking_init(receiver, *args, **kwargs):
        real_init(receiver, *args, **kwargs)
        receivers.append(receiver)

    with patch.object(Receiver, "__init__", tracking_init):
        yield

    for receiver in receivers:
        try:
            receiver.zmq_context.destroy(linger=0)
        except Exception:
            pass
