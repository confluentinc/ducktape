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


from collections import OrderedDict

from ducktape.cluster.cluster_spec import ClusterSpec


class ServiceRegistry(object):

    def __init__(self):
        self._services = OrderedDict()
        self._nodes = {}

    def __contains__(self, item):
        return id(item) in self._services

    def __iter__(self):
        return iter(self._services.values())

    def __repr__(self):
        return str(self._services.values())

    def append(self, service):
        self._services[id(service)] = service
        self._nodes[id(service)] = [str(n.account) for n in service.nodes]

    def to_json(self):
        return [self._services[k].to_json() for k in self._services]

    def stop_all(self):
        """Stop all currently registered services in the reverse of the order in which they were added.

        Note that this does not clean up persistent state or free the nodes back to the cluster.
        """
        keyboard_interrupt = None
        for service in reversed(self._services.values()):
            try:
                service.stop()
            except BaseException as e:
                if isinstance(e, KeyboardInterrupt):
                    keyboard_interrupt = e
                service.logger.warn("Error stopping service %s: %s", service, e)

        if keyboard_interrupt is not None:
            raise keyboard_interrupt

    def clean_all(self):
        """Clean all services. This should only be called after services are stopped."""
        keyboard_interrupt = None
        for service in self._services.values():
            try:
                service.clean()
            except BaseException as e:
                if isinstance(e, KeyboardInterrupt):
                    keyboard_interrupt = e
                service.logger.warn("Error cleaning service %s: %s" % (service, e))

        if keyboard_interrupt is not None:
            raise keyboard_interrupt

    def free_all(self):
        """Release nodes back to the cluster."""
        keyboard_interrupt = None
        for service in self._services.values():
            try:
                service.free()
            except BaseException as e:
                if isinstance(e, KeyboardInterrupt):
                    keyboard_interrupt = e
                service.logger.warn("Error cleaning service %s: %s" % (service, e))

        if keyboard_interrupt is not None:
            raise keyboard_interrupt

    def min_cluster_spec(self):
        """
        Returns the minimum cluster specification that would be required to run all the currently
        extant services.
        """
        cluster_spec = ClusterSpec()
        for service in self._services.values():
            cluster_spec.add(service.cluster_spec)
        return cluster_spec

    def errors(self):
        """
        Gets a printable string containing any errors produced by the services.
        """
        all_errors = []
        for service in self._services.values():
            if hasattr(service, 'error') and service.error:
                all_errors.append("%s: %s" % (service.who_am_i(), service.error))
        return '\n\n'.join(all_errors)
