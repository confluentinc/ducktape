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

import socket


def find_available_port(min_port=8000, max_port=9000):
    """Return first available port in the range [min_port, max_port], inclusive.

    Note that this actually isn't a 100% reliable way of getting a port, but it's probably good enough -- once
    you close a socket you cannot be sure of its availability. This was the source of a bunch of issues in
    the Apache Kafka unit tests.
    """
    for p in range(min_port, max_port + 1):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("localhost", p))
            s.close()
            return p
        except socket.error:
            pass

    raise Exception("No available port found in range [%d, %d]" % (min_port, max_port))
