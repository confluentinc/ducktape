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

from collections import OrderedDict


class ServiceRegistry(OrderedDict):

    def start_all(self):
        """Start all currently registered services in the same order in which they were added."""
        for service in self.values():
            service.start()

    def stop_all(self):
        """Stop all currently registered services in the reverse of the order in which they were added."""
        for service in reversed(self.values()):
            try:
                service.stop()
            except Exception as e:
                service.logger.debug("Error stopping service %s: %s" % (service, e.message))

    def pull_logs(self, service_name, destination_directory):
        """Pull logs from service. Service can be service name or the original service object."""
        service = self[service_name]
        log_directories = service.logs

        for node in service.nodes:
            for log_name in log_directories.keys():
                if self.should_collect_log(log_name, service_name, node):
                    try:
                        node.account.scp_from(log_directories[log_name], destination_directory, recursive=True)
                    except Exception as e:
                        service.logger.debug(
                            "Error copying log %(log_name)s from %(source)s to %(dest)s. \
                            service %(service)s: %(message)s" %
                            {'log_name': log_name,
                             'source': log_directories[log_name],
                             'dest': destination_directory,
                             'service': service,
                             'message': e.message})

    def should_collect_log(self, log_name, service, node):
        return True

    def clean_all(self):
        """Clean all services. This should only be called after services are stopped."""
        for service in self.values():
            try:
                service.clean()
            except Exception as e:
                service.logger.debug("Error cleaning service %s: %s" % (service, e.message))

    def free_all(self):
        """Release nodes back to the cluster."""
        for service in self.values():
            try:
                service.free()
            except Exception as e:
                service.logger.debug("Error cleaning service %s: %s" % (service, e.message))
