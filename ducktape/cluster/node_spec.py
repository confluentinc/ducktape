# Copyright 2017 Confluent Inc.
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

from __future__ import absolute_import

import json

from .consts import LINUX, SUPPORTED_OS_TYPES


class NodeSpec(object):
    """
    The specification for a ducktape cluster node.

    :param operating_system:    The operating system of the node.
    """

    def __init__(self, operating_system=LINUX):
        self.operating_system = operating_system
        if self.operating_system not in SUPPORTED_OS_TYPES:
            raise RuntimeError("Unsupported os type %s" % self.operating_system)

    def __str__(self):
        dict = {
            "os": self.operating_system,
        }
        return json.dumps(dict, sort_keys=True)
