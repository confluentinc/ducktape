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

    """ This class is meant to register a bunch of schemas in one or more background threads.
    To date, it is used in several different failover tests to concurrently send registration requests to a
    schema-registry cluster while the master is killed, bounced etc.

    It takes a round robin approach to schema registry, and if registration fails will retry with some backoff
    between attempts. If registration still fails after num_tries attempts, the registration will be recorded
    as a failure and the service will move on to the next schema.

    A test using RegisterSchemasService can end registration early by setting ready_to_finish flag to true.

    Attributes:
        num_nodes:                      Number of nodes to use for this service
        subject                         Register schemas under this subject
        schema_registry:                The schema registry service to use for registration
        retry_wait_sec:                 The backoff time between registration retries
        num_tries:                      Number of times to attempt to register any given schema
        max_time_seconds:               Don't run the background registration thread any longer than this
        max_schemas:                    Register at most this many schemas
        ready_to_finish:                When this flag is set to true, the worker thread(s) will stop registering schemas
        num_attempted_registrations:    Tracks number of attempted registrations so far
        request_target_idx:             Track id of target node in schema registry service - send requests to this node.
                                        Used to control round-robin approach to registration.
        registration_data               One record for each schema we try to register
        try_histogram                   Histogram of number of tries needed to register schemas. Failures are associated
                                        with -1 tries.
        worker_threads                  Background registration threads
    """

    def __init__(self, cluster, num_nodes, schema_registry, retry_wait_sec, num_tries, max_time_seconds=60, max_schemas=float("inf")):
        super(RegisterSchemasService, self).__init__(cluster, num_nodes)

        self.subject = "test_subject"
        self.schema_registry = schema_registry
        self.max_schemas = max_schemas
        self.max_time_seconds = max_time_seconds
        self.retry_wait_sec = retry_wait_sec
        self.num_tries = num_tries

        self.ready_to_finish = False
        self.num_attempted_registrations = 0

        # Used to control round-robin approach to rest requests
        self.request_target_idx = 1

        # Track success/failure of registration attempts
        self.registration_data = []

        # Keep a histogram of number of POST requests required for registration
        self.try_histogram = {}

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
        for idx, node in enumerate(self.nodes, 1):
            self.logger.debug("Stopping %s node %d on %s", self.__class__.__name__, idx, node.account.hostname)
            node.free()

    def _worker(self, idx, node):
        # Set global schema compatibility requirement to NONE
        self.logger.debug("Changing compatibility requirement on %s" % self.schema_registry.url(1))
        self.logger.debug(self.schema_registry.url(1))
        update_config(self.schema_registry.url(1), Compatibility.NONE)

        start = time.time()
        i = 0
        while True:
            elapsed = time.time() - start
            self.ready_to_finish = self.ready_to_finish or elapsed > self.max_time_seconds or i >= self.max_schemas
            if self.ready_to_finish:
                break

            self.try_register(i, idx, node)
            self.num_attempted_registrations += 1
            i += 1

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
        stop = -1
        schema_id = -1
        success = False
        for i in range(self.num_tries):
            n_tries += 1

            # Rotate to next server in the schema registry
            self.request_target_idx %= self.schema_registry.num_nodes
            self.request_target_idx += 1
            target_url = self.schema_registry.url(self.request_target_idx)

            try:
                self.logger.debug("Trying to register schema " + str(num))
                schema_id = register_schema(target_url, schema_string, self.subject)
                stop = time.time()
                success = True
                break

            except Exception as e:
                # TODO - use more specific exception
                # Ignore and try again
                pass

            # sleep a little and try again
            time.sleep(self.retry_wait_sec)

        # Record some data about this registration attempt
        if not success:
            stop = time.time()

        self.registration_data.append({
            "success": success,
            "start": start,
            "stop": stop,
            "elapsed": stop - start,
            "n_tries": n_tries,
            "schema_string": schema_string,
            "schema_id": schema_id
        })

        if not success:
            # In the histogram of number of tries, record failures on -1 tries
            n_tries = -1

        if self.try_histogram.has_key(n_tries):
            self.try_histogram[n_tries] += 1
        else:
            self.try_histogram[n_tries] = 1






