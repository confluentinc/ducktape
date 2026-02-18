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

from ducktape.services.service import Service
from ducktape.jvm_logging import JVMLogger
from tests.ducktape_mock import test_context, session_context
from ducktape.cluster.localhost import LocalhostCluster
from unittest.mock import Mock
import os


class JavaService(Service):
    """Mock Java service for testing JVM logging."""

    def __init__(self, context, num_nodes):
        super(JavaService, self).__init__(context, num_nodes)
        self.start_called = False
        self.clean_called = False
        self.ssh_commands = []

    def idx(self, node):
        return 1

    def start_node(self, node, **kwargs):
        super(JavaService, self).start_node(node, **kwargs)
        self.start_called = True
        # Simulate a Java command being run
        node.account.ssh("java -version")

    def clean_node(self, node, **kwargs):
        super(JavaService, self).clean_node(node, **kwargs)
        self.clean_called = True


def create_mock_node():
    """Create a mock node with a mock account that tracks SSH calls."""
    node = Mock()
    node.account = Mock()
    node.account.ssh = Mock(return_value=None)
    node.account.hostname = "mock-host"
    node.account.externally_routable_ip = "127.0.0.1"
    return node


class CheckJVMLogging(object):
    def setup_method(self, _):
        self.cluster = LocalhostCluster()
        self.session_context = session_context()
        self.context = test_context(self.session_context, cluster=self.cluster)
        self.jvm_logger = JVMLogger()

    def check_enable_for_service(self):
        """Check that JVM logging can be enabled for a service."""
        service = JavaService(self.context, 1)

        # Enable JVM logging
        self.jvm_logger.enable_for_service(service)

        # Verify logs dict was updated
        assert hasattr(service, "logs")
        assert "jvm_gc_log" in service.logs
        assert "jvm_stdout_stderr" in service.logs
        assert "jvm_heap_dump" in service.logs

        # Verify helper methods were added
        assert hasattr(service, "JVM_LOG_DIR")
        assert hasattr(service, "jvm_options")
        assert hasattr(service, "setup_jvm_logging")
        assert hasattr(service, "clean_jvm_logs")

    def check_jvm_options_format(self):
        """Check that JVM options string is properly formatted."""
        jvm_opts = self.jvm_logger._get_jvm_options()

        # Should start with -Xlog:disable to prevent console pollution
        assert jvm_opts.startswith("-Xlog:disable")

        # Should contain key logging options
        assert "gc*:file=" in jvm_opts
        assert "HeapDumpOnOutOfMemoryError" in jvm_opts
        assert "safepoint=info" in jvm_opts
        assert "class+load=info" in jvm_opts
        assert "jit+compilation=info" in jvm_opts
        assert "NativeMemoryTracking=summary" in jvm_opts

    def check_ssh_wrapping(self):
        """Check that all SSH methods are wrapped to inject JDK_JAVA_OPTIONS."""
        # Create a mock node first
        mock_node = create_mock_node()
        # Add ssh_capture and ssh_output methods to mock
        mock_node.account.ssh_capture = Mock(return_value=iter([]))
        mock_node.account.ssh_output = Mock(return_value="")

        # Enable JVM logging for a service
        service = JavaService(self.context, 1)
        self.jvm_logger.enable_for_service(service)

        # Manually call the wrapped start_node with our mock node
        # This simulates what happens during service.start()
        service.start_node(mock_node)

        # Verify all SSH methods were wrapped
        assert hasattr(mock_node.account, "original_ssh")
        assert hasattr(mock_node.account, "original_ssh_capture")
        assert hasattr(mock_node.account, "original_ssh_output")

        # The current methods should be callables (the wrapper functions)
        assert callable(mock_node.account.ssh)
        assert callable(mock_node.account.ssh_capture)
        assert callable(mock_node.account.ssh_output)
        assert service.start_called

    def check_ssh_methods_inject_options(self):
        """Check that wrapped SSH methods actually inject JDK_JAVA_OPTIONS."""
        service = JavaService(self.context, 1)
        node = service.nodes[0]

        # Track what commands are actually executed
        executed_commands = []

        def track_ssh(cmd, allow_fail=False):
            executed_commands.append(("ssh", cmd))
            return 0

        def track_ssh_capture(cmd, allow_fail=False, callback=None, combine_stderr=True, timeout_sec=None):
            executed_commands.append(("ssh_capture", cmd))
            return iter([])

        def track_ssh_output(cmd, allow_fail=False, combine_stderr=True, timeout_sec=None):
            executed_commands.append(("ssh_output", cmd))
            return ""

        # Set the tracking functions as the original methods
        node.account.ssh = track_ssh
        node.account.ssh_capture = track_ssh_capture
        node.account.ssh_output = track_ssh_output

        # Enable JVM logging - this wraps start_node
        self.jvm_logger.enable_for_service(service)

        # Start node - this wraps the SSH methods
        service.start_node(node)

        # Clear the setup commands (from mkdir, etc)
        executed_commands.clear()

        # Now execute commands through the wrapped methods
        node.account.ssh("java -version")
        node.account.ssh_capture("java -jar app.jar")
        node.account.ssh_output("java -cp test.jar Main")

        # Verify JDK_JAVA_OPTIONS was injected in all commands
        assert len(executed_commands) == 3, f"Expected 3 commands, got {len(executed_commands)}"
        for method, cmd in executed_commands:
            assert "JDK_JAVA_OPTIONS=" in cmd, f"{method} didn't inject JDK_JAVA_OPTIONS: {cmd}"
            assert "-Xlog:disable" in cmd, f"{method} didn't include JVM options: {cmd}"

    def check_ssh_wrap_idempotent(self):
        """Check that SSH wrapping is idempotent (handles restarts)."""
        mock_node = create_mock_node()
        mock_node.account.ssh_capture = Mock(return_value=iter([]))
        mock_node.account.ssh_output = Mock(return_value="")

        service = JavaService(self.context, 1)

        # Enable JVM logging
        self.jvm_logger.enable_for_service(service)

        # Get the wrapped start_node method
        wrapped_start_node = service.start_node

        # Call it twice with the same node
        wrapped_start_node(mock_node)

        # Verify wrapping happened
        assert hasattr(mock_node.account, "original_ssh")
        assert hasattr(mock_node.account, "original_ssh_capture")
        assert hasattr(mock_node.account, "original_ssh_output")

        # Call again (simulating restart)
        wrapped_start_node(mock_node)

        # SSH should still have the original_* attributes (idempotent)
        assert hasattr(mock_node.account, "original_ssh")
        assert hasattr(mock_node.account, "original_ssh_capture")
        assert hasattr(mock_node.account, "original_ssh_output")

    def check_clean_node_behavior(self):
        """Check that clean_node properly cleans up and restores SSH methods."""
        mock_node = create_mock_node()
        mock_node.account.ssh_capture = Mock(return_value=iter([]))
        mock_node.account.ssh_output = Mock(return_value="")

        service = JavaService(self.context, 1)

        # Enable JVM logging
        self.jvm_logger.enable_for_service(service)

        # Get the wrapped methods
        wrapped_start_node = service.start_node
        wrapped_clean_node = service.clean_node

        # Start node to wrap SSH methods
        wrapped_start_node(mock_node)
        assert hasattr(mock_node.account, "original_ssh")
        assert hasattr(mock_node.account, "original_ssh_capture")
        assert hasattr(mock_node.account, "original_ssh_output")

        # Clean node to restore SSH methods
        wrapped_clean_node(mock_node)

        # Verify SSH methods were restored
        assert not hasattr(mock_node.account, "original_ssh")
        assert not hasattr(mock_node.account, "original_ssh_capture")
        assert not hasattr(mock_node.account, "original_ssh_output")

    def check_log_paths(self):
        """Check that log paths are correctly configured."""
        service = JavaService(self.context, 1)
        self.jvm_logger.enable_for_service(service)

        log_dir = self.jvm_logger.log_dir

        # Verify log paths
        assert service.logs["jvm_gc_log"]["path"] == os.path.join(log_dir, "gc.log")
        assert service.logs["jvm_stdout_stderr"]["path"] == os.path.join(log_dir, "jvm.log")
        assert service.logs["jvm_heap_dump"]["path"] == os.path.join(log_dir, "heap_dump.hprof")

        # Verify collection flags
        assert service.logs["jvm_gc_log"]["collect_default"] is True
        assert service.logs["jvm_stdout_stderr"]["collect_default"] is True
        assert service.logs["jvm_heap_dump"]["collect_default"] is False

    def check_kwargs_preserved(self):
        """Check that start_node and clean_node preserve kwargs after wrapping."""
        service = JavaService(self.context, 1)

        # Replace the node with a mock node
        mock_node = create_mock_node()
        service.nodes = [mock_node]

        self.jvm_logger.enable_for_service(service)

        # This should not raise an error even with extra kwargs
        service.start(timeout_sec=30, clean=False)
        assert service.start_called
