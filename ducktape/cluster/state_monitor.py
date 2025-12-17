# Copyright 2024 Confluent Inc.
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

"""
State monitor for sharing cluster state with external orchestrators via file system.
"""

import json
import fcntl
import os
import time
import logging
from typing import List, Optional, Dict, Any


class StateMonitor:
    """Monitors and persists cluster state to a shared file.

    This class manages a JSON file that contains the current cluster state,
    including orphaned nodes that can be safely terminated by external systems.
    Uses file locking to ensure safe concurrent access.
    """

    def __init__(self, state_file_path: str, logger: Optional[logging.Logger] = None):
        """Initialize the state monitor.

        Args:
            state_file_path: Path to the state file
            logger: Logger instance for logging events
        """
        self.state_file_path = state_file_path
        self.logger = logger or logging.getLogger(__name__)
        self.session_id = None
        self._initialize_state_file()

    def _initialize_state_file(self):
        """Create initial state file if it doesn't exist."""
        if not os.path.exists(self.state_file_path):
            initial_state = {
                'session_id': None,
                'status': 'initializing',
                'cluster': {
                    'total': 0,
                    'available': 0,
                    'in_use': 0,
                    'orphaned': 0
                },
                'orphaned_nodes': [],
                'last_updated': time.time()
            }
            self._write_state(initial_state)
            self.logger.info(f"Initialized state file: {self.state_file_path}")

    def set_session_id(self, session_id: str):
        """Set the session ID for this ducktape run.

        Args:
            session_id: Unique identifier for this test session
        """
        self.session_id = session_id
        state = self._read_state()
        state['session_id'] = session_id
        state['status'] = 'running'
        state['last_updated'] = time.time()
        self._write_state(state)
        self.logger.info(f"State monitor tracking session: {session_id}")

    def update_cluster_state(self, total: int, available: int, in_use: int):
        """Update cluster capacity statistics.

        Args:
            total: Total number of nodes in cluster
            available: Number of available nodes
            in_use: Number of nodes currently in use
        """
        state = self._read_state()
        state['cluster']['total'] = total
        state['cluster']['available'] = available
        state['cluster']['in_use'] = in_use
        state['last_updated'] = time.time()
        self._write_state(state)

    def mark_orphaned(self, nodes: List[Any]):
        """Mark nodes as orphaned (will never be used again).

        Args:
            nodes: List of ClusterNode objects that are orphaned
        """
        if not nodes:
            return

        state = self._read_state()

        # Convert nodes to serializable format
        orphaned_list = state.get('orphaned_nodes', [])
        existing_hostnames = {n['hostname'] for n in orphaned_list}

        for node in nodes:
            hostname = node.account.hostname
            if hostname not in existing_hostnames:
                node_info = {
                    'hostname': hostname,
                    'ssh_hostname': node.account.ssh_hostname,
                    'user': node.account.user,
                    'operating_system': str(node.account.operating_system),
                    'externally_routable_ip': node.account.externally_routable_ip,
                    'marked_at': time.time()
                }
                orphaned_list.append(node_info)
                existing_hostnames.add(hostname)
                self.logger.info(f"Marked node as orphaned: {hostname}")

        state['orphaned_nodes'] = orphaned_list
        state['cluster']['orphaned'] = len(orphaned_list)
        state['last_updated'] = time.time()
        self._write_state(state)

    def mark_session_complete(self):
        """Mark the test session as complete."""
        state = self._read_state()
        state['status'] = 'completed'
        state['last_updated'] = time.time()
        self._write_state(state)
        self.logger.info("Marked session as complete")

    def _read_state(self) -> Dict[str, Any]:
        """Read state from file with shared lock.

        Returns:
            Dictionary containing current state
        """
        try:
            with open(self.state_file_path, 'r') as f:
                # Acquire shared lock for reading
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    return json.load(f)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception as e:
            self.logger.error(f"Failed to read state file: {e}")
            # Return default state on error
            return {
                'session_id': self.session_id,
                'status': 'error',
                'cluster': {'total': 0, 'available': 0, 'in_use': 0, 'orphaned': 0},
                'orphaned_nodes': [],
                'last_updated': time.time()
            }

    def _write_state(self, state: Dict[str, Any]):
        """Write state to file with exclusive lock.

        Args:
            state: State dictionary to write
        """
        try:
            with open(self.state_file_path, 'w') as f:
                # Acquire exclusive lock for writing
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    json.dump(state, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())  # Ensure data is written to disk
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception as e:
            self.logger.error(f"Failed to write state file: {e}")

    def cleanup(self):
        """Clean up resources (optional, for explicit cleanup)."""
        # Mark as completed if not already
        state = self._read_state()
        if state.get('status') == 'running':
            self.mark_session_complete()
