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

from .service import Service
from .schema_registry_utils import *
import time, threading


class RegisterSchemasService(Service):

    def __init__(self, cluster, num_nodes, schema_registry, num_schemas, retry_wait_sec, num_tries):
        super(RegisterSchemasService, self).__init__(cluster, num_nodes)

        self.subject = "test_subject"
        self.schema_registry = schema_registry
        self.num_schemas = num_schemas
        self.retry_wait_sec = retry_wait_sec
        self.num_tries = num_tries

        # Used to control round-robin approach to rest requests
        self.request_target_idx = 1

        self.successfully_registered = {}
        self.registered_ids = []
        # Indices of the schemas that failed to register
        self.failed = []

        self.worker_threads = []

    def start(self):
        super(RegisterSchemasService, self).start()

        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Running %s node %d on %s", self.__class__.__name__, idx, node.account.hostname)
            worker = threading.Thread(
                name=self.__class__.__name__ + "-worker-" + str(idx),
                target=self._worker,
                args=(idx, node)
            )
            worker.daemon = True
            worker.start()
            self.worker_threads.append(worker)

    def wait(self):
        super(RegisterSchemasService, self).wait()
        for idx, worker in enumerate(self.worker_threads, 1):
            self.logger.debug("Waiting for %s worker %d to finish", self.__class__.__name__, idx)
            worker.join()
        self.worker_threads = None

    def stop(self):
        super(RegisterSchemasService, self).stop()
        assert self.worker_threads is None, "%s.stop should only be called after wait" % self.__class__.__name__
        for idx, node in enumerate(self.nodes,1):
            self.logger.debug("Stopping %s node %d on %s", self.__class__.__name__, idx, node.account.hostname)
            node.free()

    def _worker(self, idx, node):
        # Set global schema compatibility requirement to NONE
        self.logger.debug("Changing compatibility requirement on %s" % self.schema_registry.url(1))
        self.logger.debug(self.schema_registry.url(1))
        update_config(self.schema_registry.url(1), ConfigUpdateRequest(Compatibility.NONE))

        for i in range(self.num_schemas):
            self.try_register(i, idx, node)

    def try_register(self, num, idx, node):
        """
        Try to register schema with the schema registry, rotating through the servers if
         necessary.

        Currently idx and node are not used because the registration requests happen locally. But it's conceivable
        that we might want a setup where requests come in concurrently from different nodes.
        """

        self.logger.debug("Attempting to register schema number %d." % num)
        schema_string = make_schema_string(num)
        start = time.time()
        n_tries = 0
        for i in range(self.num_tries):
            n_tries += 1

            # Rotate to next server in the schema registry
            self.request_target_idx %= self.schema_registry.num_nodes
            self.request_target_idx += 1

            target_url = self.schema_registry.url(self.request_target_idx)

            try:
                schema_id = register_schema(target_url, RegisterSchemaRequest(schema_string), self.subject)
                elapsed = time.time() - start
                self.successfully_registered[num] = {
                    "elapsed": elapsed,
                    "n_tries": n_tries,
                    "schema": schema_string,
                    "schema_id": schema_id
                }
                self.registered_ids.append(schema_id)
                self.logger.debug("Successfully registered " + str(self.successfully_registered[num]))
                return

            except Exception as e:
                # TODO - use more specific exception
                # Ignore and try again
                pass

            # sleep a little and try again
            time.sleep(self.retry_wait_sec)

        # Failed to register this schema
        self.failed.append(num)






