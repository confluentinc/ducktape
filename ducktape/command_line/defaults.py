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

import os


class ConsoleDefaults(object):
    # Directory for project-specific ducktape configs and runtime data
    DUCKTAPE_DIR = ".ducktape"

    # Store various bookkeeping data here
    METADATA_DIR = os.path.join(DUCKTAPE_DIR, "metadata")

    # Default path, relative to current project directory, to the project's ducktape config file
    PROJECT_CONFIG_FILE = os.path.join(DUCKTAPE_DIR, "config")

    # Default path to the user-specific config file
    USER_CONFIG_FILE = os.path.join('~', DUCKTAPE_DIR, 'config')

    # Default cluster implementation
    CLUSTER_TYPE = "ducktape.cluster.vagrant.VagrantCluster"

    # Default path, relative to current project directory, to the cluster file
    CLUSTER_FILE = os.path.join(DUCKTAPE_DIR, "cluster.json")

    # Track the last-used session_id here
    SESSION_ID_FILE = os.path.join(METADATA_DIR, "session_id")

    # Folders with test reports, logs, etc all are created in this directory
    RESULTS_ROOT_DIRECTORY = "./results"

    SESSION_LOG_FORMATTER = '[%(levelname)s:%(asctime)s]: %(message)s'
    TEST_LOG_FORMATTER = '[%(levelname)-5s - %(asctime)s - %(module)s - %(funcName)s - lineno:%(lineno)s]: %(message)s'

    # Log this to indicate a test is misbehaving to help end user find which test is at fault
    BAD_TEST_MESSAGE = "BAD_TEST"

    # Ducktape will try to pick a random open port in the range [TEST_DRIVER_PORT_MIN, TEST_DRIVER_PORT_MAX]
    # this range is *inclusive*
    TEST_DRIVER_MIN_PORT = 5556
    TEST_DRIVER_MAX_PORT = 5656
