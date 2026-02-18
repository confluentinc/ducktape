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
        assert hasattr(service, 'logs')
        assert 'jvm_gc_log' in service.logs
        assert 'jvm_stdout_stderr' in service.logs
        assert 'jvm_heap_dump' in service.logs
        
        # Verify helper methods were added
        assert hasattr(service, 'JVM_LOG_DIR')
        assert hasattr(service, 'jvm_options')
        assert hasattr(service, 'setup_jvm_logging')
        assert hasattr(service, 'clean_jvm_logs')

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
        """Check that SSH method is wrapped to inject JDK_JAVA_OPTIONS."""
        # Create a mock node first
        mock_node = create_mock_node()
        
        # Enable JVM logging for a service
        service = JavaService(self.context, 1)
        self.jvm_logger.enable_for_service(service)
        
        # Manually call the wrapped start_node with our mock node
        # This simulates what happens during service.start()
        service.start_node(mock_node)
        
        # Verify SSH was wrapped (original_ssh attribute is set)
        assert hasattr(mock_node.account, 'original_ssh')
        # The current ssh should be a callable (the wrapper function)
        assert callable(mock_node.account.ssh)
        assert service.start_called

    def check_ssh_wrap_idempotent(self):
        """Check that SSH wrapping is idempotent (handles restarts)."""
        service = JavaService(self.context, 1)
        
        # Replace the node with a mock node
        mock_node = create_mock_node()
        service.nodes = [mock_node]
        
        # Enable JVM logging
        self.jvm_logger.enable_for_service(service)
        
        # Start service twice (simulating restart)
        service.start()
        first_wrapped_ssh = mock_node.account.ssh
        
        service.stop()
        service.start()
        second_wrapped_ssh = mock_node.account.ssh
        
        # SSH should still be wrapped and should be the same wrapper
        assert hasattr(mock_node.account, 'original_ssh')
        assert first_wrapped_ssh == second_wrapped_ssh

    def check_clean_node_behavior(self):
        """Check that clean_node properly cleans up."""
        service = JavaService(self.context, 1)
        
        # Replace the node with a mock node
        mock_node = create_mock_node()
        service.nodes = [mock_node]
        
        # Enable JVM logging
        self.jvm_logger.enable_for_service(service)
        
        # Start and clean
        service.start()
        service.clean()
        
        assert service.clean_called

    def check_log_paths(self):
        """Check that log paths are correctly configured."""
        service = JavaService(self.context, 1)
        self.jvm_logger.enable_for_service(service)
        
        log_dir = self.jvm_logger.log_dir
        
        # Verify log paths
        assert service.logs['jvm_gc_log']['path'] == os.path.join(log_dir, "gc.log")
        assert service.logs['jvm_stdout_stderr']['path'] == os.path.join(log_dir, "jvm.log")
        assert service.logs['jvm_heap_dump']['path'] == os.path.join(log_dir, "heap_dump.hprof")
        
        # Verify collection flags
        assert service.logs['jvm_gc_log']['collect_default'] is True
        assert service.logs['jvm_stdout_stderr']['collect_default'] is True
        assert service.logs['jvm_heap_dump']['collect_default'] is False

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
