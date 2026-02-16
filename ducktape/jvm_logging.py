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
JVM logging support for Ducktape.

This module provides automatic JVM log collection for Java-based services
without requiring any code changes to services or tests.
"""

import os


class JVMLogger:
    """Handles JVM logging configuration and enablement for services."""

    ENV_FILE_PATH = "/tmp/ducktape_jvm_env.sh"
    
    def __init__(self, log_dir="/mnt/jvm_logs"):
        """
        Initialize JVM logger.
        
        :param log_dir: Directory for JVM logs on worker nodes
        """
        self.log_dir = log_dir
    
    def enable_for_service(self, service):
        """
        Enable JVM logging for a service instance.
        Adds JVM log definitions and helper methods to the service.
        Automatically wraps start_node and clean_node to setup/cleanup JVM logging.
        :param service: Service instance to enable JVM logging for
        """
        # Add JVM log definitions
        jvm_logs = {
            "jvm_gc_log": {
                "path": os.path.join(self.log_dir, "gc.log"),
                "collect_default": True
            },
            "jvm_stdout_stderr": {
                "path": os.path.join(self.log_dir, "jvm.log"),
                "collect_default": True
            },
            "jvm_heap_dump": {
                "path": os.path.join(self.log_dir, "heap_dump.hprof"),
                "collect_default": False  # Only on failure
            }
        }

        # Initialize logs dict if needed
        if not hasattr(service, 'logs') or service.logs is None:
            service.logs = {}

        # Merge with existing logs
        service.logs.update(jvm_logs)

        # Add helper methods
        service.JVM_LOG_DIR = self.log_dir
        service.jvm_options = lambda node: self._get_jvm_options()
        service.setup_jvm_logging = lambda node: self._setup_on_node(node)
        service.clean_jvm_logs = lambda node: self._cleanup_on_node(node)

        # Wrap start_node to automatically setup JVM logging and wrap SSH
        original_start_node = service.start_node

        def wrapped_start_node(node, **kwargs):
            # Setup JVM log directory and environment file
            self._setup_on_node(node)

            # Wrap the node's ssh method to automatically source JVM env
            if not hasattr(node.account, 'original_ssh'):
                original_ssh = node.account.ssh
                node.account.original_ssh = original_ssh

                def wrapped_ssh(cmd, allow_fail=False):
                    # Automatically source JVM env file if it exists
                    wrapped_cmd = f"[ -f {self.ENV_FILE_PATH} ] && source {self.ENV_FILE_PATH}; {cmd}"
                    return original_ssh(wrapped_cmd, allow_fail=allow_fail)

                node.account.ssh = wrapped_ssh

            # Call the original start_node
            try:  # Try with kwargs first, fall back to without kwargs for compatibility
                return original_start_node(node, **kwargs)
            except TypeError:
                # Original start_node doesn't accept **kwargs, call without them
                return original_start_node(node)

        service.start_node = wrapped_start_node

        # Wrap clean_node to automatically cleanup JVM logs and restore SSH
        original_clean_node = service.clean_node

        def wrapped_clean_node(node, **kwargs):
            try:
                result = original_clean_node(node, **kwargs)
            except TypeError:
                result = original_clean_node(node)

            # Restore original ssh method if it was wrapped
            if hasattr(node.account, 'original_ssh'):
                node.account.ssh = node.account.original_ssh
                delattr(node.account, 'original_ssh')

            # Then cleanup JVM logs and env file
            self._cleanup_on_node(node)
            return result
        
        service.clean_node = wrapped_clean_node
    
    def _get_jvm_options(self):
        """Generate JVM options string for logging."""
        gc_log = os.path.join(self.log_dir, "gc.log")
        heap_dump = os.path.join(self.log_dir, "heap_dump.hprof")
        error_log = os.path.join(self.log_dir, "hs_err_pid%p.log")
        jvm_log = os.path.join(self.log_dir, "jvm.log")

        jvm_logging_opts = [
            f"-Xlog:gc*:file={gc_log}:time,uptime,level,tags",  # GC activity with timestamps
            "-Xlog:gc*=info",  # Set GC log level to info
            "-XX:+HeapDumpOnOutOfMemoryError",  # Generate heap dump on OOM
            f"-XX:HeapDumpPath={heap_dump}",  # Heap dump file location
            f"-Xlog:safepoint=info:file={jvm_log}:time,uptime,level,tags",  # Safepoint pause events
            f"-Xlog:class+load=info:file={jvm_log}:time,uptime,level,tags",  # Class loading events
            f"-XX:ErrorFile={error_log}",  # Fatal error log location (JVM crashes)
            "-XX:NativeMemoryTracking=summary",  # Track native memory usage
            f"-Xlog:jit+compilation=info:file={jvm_log}:time,uptime,level,tags",  # JIT compilation events
        ]
        
        return " ".join(jvm_logging_opts)
    
    def _setup_on_node(self, node):
        """Create JVM log directory and environment file on worker node."""
        node.account.ssh(f"mkdir -p {self.log_dir}")
        node.account.ssh(f"chmod 755 {self.log_dir}")
        
        # Create environment file with JVM options
        jvm_opts = self._get_jvm_options()
        env_file_content = f"""
#!/bin/bash
# Auto-generated JVM logging environment variables by Ducktape
export KAFKA_OPTS="{jvm_opts}"
export JAVA_OPTS="{jvm_opts}"
"""
        node.account.create_file(self.ENV_FILE_PATH, env_file_content)
        node.account.ssh(f"chmod 644 {self.ENV_FILE_PATH}")
    
    def _cleanup_on_node(self, node):
        """Clean JVM logs and environment file from worker node."""
        node.account.ssh(f"rm -rf {self.log_dir}", allow_fail=True)
        node.account.ssh(f"rm -f {self.ENV_FILE_PATH}", allow_fail=True)
