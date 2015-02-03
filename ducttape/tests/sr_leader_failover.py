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

from .test import SchemaRegistryTest
from ducttape.services.register_schemas_service import RegisterSchemasService
from ducttape.services.schema_registry_utils import get_schema_by_id
from ducttape.services.schema_registry_utils import SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES
import time

# Specify retry frequency and retry window
# For a clean kill of master, where master election
# should be very fast, expect about 10 retries w/.02 seconds between tries
# should be sufficient

# For a kill -9, master reelection won't take place until zookeeper timeout, or about 4 seconds

class FailoverTest(SchemaRegistryTest):
    def __init__(self, cluster, num_zk, num_brokers, num_schema_reg):
        super(FailoverTest, self).__init__(cluster, num_zk, num_brokers, num_schema_reg)

        # # Try to register this many schemas
        # self.num_schemas = 100

        # Time to wait between registration retries
        self.retry_wait_sec = .2

        # Number of attempted retries
        self.num_retries = 10

        # Initialize during setUp
        self.register_driver = None

    def setUp(self):
        super(FailoverTest, self).setUp()
        self.register_driver = RegisterSchemasService(self.cluster, 1, self.schema_registry, self.retry_wait_sec,
                                                      self.num_retries, max_time_seconds=180)

    def drive_failures(self):
        raise NotImplementedError("drive_failures must be implemented by a subclass.")

    def report_summary(self):
        # Gather statistics
        summary = "\n-------------------------------------------------------------------\n"
        summary += "Summary\n"
        summary += str(self.register_driver.try_histogram) + "\n"
        #
        # for record in self.register_driver.registration_data:
        #     print record

        # attempted = self.register_driver.max_schemas
        # succeeded = len(self.register_driver.registered_ids)
        # failed = attempted - succeeded
        # n_tries = self.register_driver.num_tries
        # try_time = (n_tries - 1) * self.register_driver.retry_wait_sec
        #
        # summary += "Attempted to register %d schemas.\n" % attempted
        # summary += "%d out of %d were reported successful.\n" % (succeeded, attempted)
        # summary += "%d out of %d failed with %d attempts over %f seconds.\n" % (failed, attempted, n_tries, try_time)
        #
        # # Max number of retries
        # # Number which required multiple retries
        # multiple_retries = [r for r in self.register_driver.successfully_registered.values() if r["n_tries"] > 1]
        # summary += "%d schemas required multiple registration attempts.\n" % len(multiple_retries)
        #
        # max_tries = max([r["n_tries"] for r in self.register_driver.successfully_registered.values()])
        # summary += "Max number of register attempts for successful registration of a single schema: %d\n" % max_tries
        # summary += "Max time to register a schema: %f(seconds)\n" % ((max_tries - 1) * self.retry_wait_sec)
        #
        # # Get a node that's still alive
        # master_id = self.schema_registry.idx(self.schema_registry.get_master_node())
        #
        # summary += "Validating that schemas reported as successful can be fetched by id...\n"
        # registered_ids = self.register_driver.registered_ids
        # success = True
        # for id in registered_ids:
        #     try:
        #         schema = get_schema_by_id(self.schema_registry.url(master_id), id)
        #     except:
        #         success = False
        #         summary += "%d was reported successful but actually failed\n" % id
        # if success:
        #     summary += "Validation successful.\n"
        #
        # # Validate by fetching versions
        #
        # # fetch versions
        # # fetch each version
        summary += "-------------------------------------------------------------------\n"

        self.logger.info(summary)

    def run(self):
        # set up
        self.setUp()

        # start schema registration in the background
        self.logger.info("Starting registration thread(s)")
        self.register_driver.start()

        # Make sure registrations have started
        while self.register_driver.num_attempted_registrations < 2:
            time.sleep(.5)

        # do the kill or bounce logic
        self.logger.info("Driving failures")
        self.drive_failures()

        # Wait a little before stopping registration
        num_attempted = self.register_driver.num_attempted_registrations
        while self.register_driver.num_attempted_registrations < num_attempted + 2:
            time.sleep(.5)

        self.logger.info("Ending registration...")
        self.register_driver.ready_to_finish = True
        self.register_driver.wait()
        self.register_driver.stop()

        self.report_summary()
        self.tearDown()


class LeaderCleanFailover(FailoverTest):
    """
    Begin registering schemas; part way through, cleanly kill the master.
    """
    def __init__(self, cluster):
        super(LeaderCleanFailover, self).__init__(cluster, num_zk=1, num_brokers=1, num_schema_reg=3)

        # Expect leader reelection to take less than .2 sec in a clean shutdown
        self.retry_wait_sec = .02
        self.num_retries = 10

    def drive_failures(self):
        """
        Wait a bit, and then kill the master node cleanly.
        """
        time.sleep(3)
        master_node = self.schema_registry.get_master_node()
        self.schema_registry.stop_node(master_node)


class LeaderHardFailover(FailoverTest):
    """
    Begin registering schemas; part way through, hard kill the master (kill -9)
    """
    def __init__(self, cluster):
        super(LeaderHardFailover, self).__init__(cluster, num_zk=1, num_brokers=1, num_schema_reg=3)

        # Default zookeeper session timeout is 10 seconds
        self.retry_wait_sec = .1
        self.num_retries = 110

    def drive_failures(self):
        """
        Wait a bit, and then kill -9 the master
        """
        time.sleep(3)
        master_node = self.schema_registry.get_master_node()
        self.schema_registry.stop_node(master_node, clean_shutdown=False)


class CleanBounce(FailoverTest):
    def __init__(self, cluster):
        super(CleanBounce, self).__init__(cluster, num_zk=1, num_brokers=1, num_schema_reg=3)

        # self.num_schemas = 1000
        # Expect leader reelection to take less than .2 sec in a clean shutdown
        self.retry_wait_sec = .02
        self.num_retries = 10

    def drive_failures(self):
        """
        Bounce master several times - i.e. kill master with SIGTERM aka kill aka kill -15 and restart
        """
        # Bounce leader several times with some wait in-between
        for i in range(5):
            prev_master_node = self.schema_registry.get_master_node()
            self.schema_registry.restart_node(prev_master_node, wait_sec=5)

            # Don't restart the new master until the previous master is running again
            prev_master_node.account.wait_for_http_service(
                self.schema_registry.port, headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)


class HardBounce(FailoverTest):
    def __init__(self, cluster):
        super(HardBounce, self).__init__(cluster, num_zk=1, num_brokers=1, num_schema_reg=3)

        # Expect leader reelection to take less than .2 sec in a clean shutdown
        self.retry_wait_sec = .3
        self.num_retries = 50

    def drive_failures(self):
        """
        Bounce master several times - i.e. kill master with SIGKILL aka kill -9 and restart
        """
        # Bounce leader several times with some wait in-between
        for i in range(5):
            prev_master_node = self.schema_registry.get_master_node()
            self.schema_registry.restart_node(prev_master_node, wait_sec=5, clean_shutdown=False)

            # Don't restart the new master until the previous master is running again
            prev_master_node.account.wait_for_http_service(
                self.schema_registry.port, headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)


if __name__ == "__main__":
    LeaderCleanFailover.run_standalone()
    LeaderHardFailover.run_standalone()
    CleanBounce.run_standalone()
    HardBounce.run_standalone()
