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

    def pull_logs(self, service_name, test_context):
        """Pull logs from service. Service can be service name or the original service object.

        :type service_name: str
        :type test_context: ducktape.tests.test.TestContext
        """
        service = self[service_name]

        if not hasattr(service, 'logs'):
            test_context.logger.debug("Won't collect service logs from %s - no 'logs' attribute." %
                service.__class__.__name__)

        log_dirs = service.logs

        for node in service.nodes:
            for log_name in log_dirs.keys():
                if self.should_collect_log(log_name, service_name, node):
                    dest = os.path.join(test_context.results_dir, service.__class__.__name__, node.account.hostname)
                    if os.path.isfile(dest):
                        pass # error!

                    if not os.path.isdir(dest):
                        mkdir_p(dest)

                    try:
                        node.account.scp_from(log_dirs[log_name], dest, recursive=True)
                    except Exception as e:
                        test_context.logger.warn(
                            "Error copying log %(log_name)s from %(source)s to %(dest)s. \
                            service %(service)s: %(message)s" %
                            {'log_name': log_name,
                             'source': log_dirs[log_name],
                             'dest': dest,
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
