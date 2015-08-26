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

from ducktape.services.service import Service
from ducktape.errors import TimeoutError

import threading
import time
import traceback


class BackgroundThreadService(Service):

    def __init__(self, context, num_nodes):
        super(BackgroundThreadService, self).__init__(context, num_nodes)
        self.worker_threads = []
        self.worker_errors = {}
        self.lock = threading.RLock()

    def _protected_worker(self, idx, node):
        """Protected worker captures exceptions and makes them available to the main thread.

        This gives us the ability to propagate exceptions thrown in background threads, if desired.
        """
        try:
            self._worker(idx, node)
        except BaseException as e:
            with self.lock:
                self.logger.info("BackgroundThreadService threw exception: ")
                self.logger.info(traceback.format_exc(limit=16))
                self.worker_errors[threading.currentThread().name] = e

            raise e

    def start_node(self, node):
        idx = self.idx(node)

        self.logger.info("Running %s node %d on %s", self.__class__.__name__, idx, node.account.hostname)
        worker = threading.Thread(
            name=self.__class__.__name__ + "-worker-" + str(idx),
            target=self._protected_worker,
            args=(idx, node)
        )
        worker.daemon = True
        worker.start()
        self.worker_threads.append(worker)

    def wait(self, timeout_sec=600):
        """Wait no more than timeout_sec for all worker threads to finish.

        raise TimeoutException if all worker threads do not finish within timeout_sec
        """
        super(BackgroundThreadService, self).wait()

        start = time.time()
        end = start + timeout_sec
        for idx, worker in enumerate(self.worker_threads, 1):

            if end > time.time():
                self.logger.debug("Waiting for worker thread %s finish", worker.name)
                current_timeout = end - time.time()
                worker.join(current_timeout)

        alive_workers = [worker.name for worker in self.worker_threads if worker.is_alive()]
        if len(alive_workers) > 0:
            raise TimeoutError("Timed out waiting %s seconds for background threads to finish. " % str(timeout_sec) +
                                "These threads are still alive: " + str(alive_workers))

        # Propagate exceptions thrown in background threads
        with self.lock:
            if len(self.worker_errors) > 0:
                raise Exception(str(self.worker_errors))

    def stop(self):
        alive_workers = [worker for worker in self.worker_threads if worker.is_alive()]
        if len(alive_workers) > 0:
            self.logger.debug(
                "Called stop with at least one worker thread is still running: " + str(alive_workers))

            self.logger.debug("%s" % str(self.worker_threads))

        super(BackgroundThreadService, self).stop()

    def stop_node(self, node):
        # do nothing
        pass

    def clean_node(self, node):
        # do nothing
        pass
