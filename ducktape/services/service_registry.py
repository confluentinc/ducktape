# Copyright 2014 Confluent Inc.
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
from ducktape.utils.local_filesystem_utils import mkdir_p

from collections import OrderedDict

import os


class ServiceRegistry(list):

    def stop_all(self):
        """Stop all currently registered services in the reverse of the order in which they were added.

        Note that this does not clean up persistent state or free the nodes back to the cluster.
        """
        keyboard_interrupt = None
        for service in reversed(self):
            try:
                service.stop()
            except BaseException as e:
                if isinstance(e, KeyboardInterrupt):
                    keyboard_interrupt = e
                service.logger.warn("Error stopping service %s: %s" % (service, e.message))

        if keyboard_interrupt is not None:
            raise keyboard_interrupt

    def clean_all(self):
        """Clean all services. This should only be called after services are stopped."""
        keyboard_interrupt = None
        for service in self:
            try:
                service.clean()
            except BaseException as e:
                if isinstance(e, KeyboardInterrupt):
                    keyboard_interrupt = e
                service.logger.warn("Error cleaning service %s: %s" % (service, e.message))

        if keyboard_interrupt is not None:
            raise keyboard_interrupt

    def free_all(self):
        """Release nodes back to the cluster."""
        keyboard_interrupt = None
        for service in self:
            try:
                service.free()
            except BaseException as e:
                if isinstance(e, KeyboardInterrupt):
                    keyboard_interrupt = e
                service.logger.warn("Error cleaning service %s: %s" % (service, e.message))

        if keyboard_interrupt is not None:
            raise keyboard_interrupt

    def num_nodes(self):
        return sum([service.num_nodes for service in self])
