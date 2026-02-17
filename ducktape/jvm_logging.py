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
import types


class JVMLogger:
    """Handles JVM logging configuration and enablement for services."""
    
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
        # Store reference to JVMLogger instance for use in closures
        jvm_logger = self
        
        # Add JVM log definitions
        jvm_logs = {
            "jvm_gc_log": {
                "path": os.path.join(jvm_logger.log_dir, "gc.log"),
                "collect_default": True
            },
            "jvm_stdout_stderr": {
                "path": os.path.join(jvm_logger.log_dir, "jvm.log"),
                "collect_default": True
            },
            "jvm_heap_dump": {
                "path": os.path.join(jvm_logger.log_dir, "heap_dump.hprof"),
                "collect_default": False  # Only on failure
            }
        }

        # Initialize logs dict if needed
        if not hasattr(service, 'logs') or service.logs is None:
            service.logs = {}

        # Merge with existing logs
        service.logs.update(jvm_logs)

        # Add helper methods
        service.JVM_LOG_DIR = jvm_logger.log_dir
        service.jvm_options = lambda node: jvm_logger._get_jvm_options()
        service.setup_jvm_logging = lambda node: jvm_logger._setup_on_node(node)
        service.clean_jvm_logs = lambda node: jvm_logger._cleanup_on_node(node)

        # Wrap start_node to automatically setup JVM logging and wrap SSH
        original_start_node = service.start_node

        def wrapped_start_node(self, node, *args, **kwargs):
            # Setup JVM log directory
            jvm_logger._setup_on_node(node)

            # Wrap the node's ssh method to inject JDK_JAVA_OPTIONS
            # Combined with -Xlog:disable in the options, this prevents any console output pollution
            # Wrap once and keep active for the entire service lifecycle
            if not hasattr(node.account, 'original_ssh'):
                original_ssh = node.account.ssh
                node.account.original_ssh = original_ssh

                def wrapped_ssh(cmd, allow_fail=False):
                    jvm_opts = jvm_logger._get_jvm_options()
                    return original_ssh(f'export JDK_JAVA_OPTIONS="{jvm_opts}"; {cmd}', allow_fail=allow_fail)

                node.account.ssh = wrapped_ssh

            return original_start_node(node, *args, **kwargs)

        # Bind the wrapper function to the service object
        service.start_node = types.MethodType(wrapped_start_node, service)

        # Wrap clean_node to cleanup JVM logs
        original_clean_node = service.clean_node

        def wrapped_clean_node(self, node, *args, **kwargs):
            result = original_clean_node(node, *args, **kwargs)
            jvm_logger._cleanup_on_node(node)
            return result

        # Bind the wrapper function to the service instance
        service.clean_node = types.MethodType(wrapped_clean_node, service)
    
    def _get_jvm_options(self):
        """Generate JVM options string for logging."""
        gc_log = os.path.join(self.log_dir, "gc.log")
        heap_dump = os.path.join(self.log_dir, "heap_dump.hprof")
        error_log = os.path.join(self.log_dir, "hs_err_pid%p.log")
        jvm_log = os.path.join(self.log_dir, "jvm.log")

        jvm_logging_opts = [
            "-Xlog:disable",  # Suppress all default JVM console logging to prevent output pollution
            f"-Xlog:gc*:file={gc_log}:time,uptime,level,tags",  # GC activity with timestamps
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
        """Create JVM log directory on worker node."""
        node.account.ssh(f"mkdir -p {self.log_dir}")
        node.account.ssh(f"chmod 755 {self.log_dir}")
    
    def _cleanup_on_node(self, node):
        """Clean JVM logs from worker node."""
        node.account.ssh(f"rm -rf {self.log_dir}", allow_fail=True)
