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
    
    def __init__(self, log_dir="/mnt/jvm_logs"):
        """
        Initialize JVM logger.
        
        :param log_dir: Directory for JVM logs on worker nodes
        :param java_version: Java version (8 or 9+) for appropriate logging options
        """
        self.log_dir = log_dir
    
    def enable_for_service(self, service):
        """
        Enable JVM logging for a service instance.
        Adds JVM log definitions and helper methods to the service.
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
    
    def _get_jvm_options(self):
        """Generate JVM options string for logging."""
        gc_log = os.path.join(self.log_dir, "gc.log")
        heap_dump = os.path.join(self.log_dir, "heap_dump.hprof")
        error_log = os.path.join(self.log_dir, "hs_err_pid%p.log")
        jvm_log = os.path.join(self.log_dir, "jvm.log")

        jvm_logging_opts = [
            f"-Xlog:gc*:file={gc_log}:time,uptime,level,tags",  # GC logging
            "-Xlog:gc*=info",  # GC log level
            "-XX:+HeapDumpOnOutOfMemoryError",  # Heap dump on OOM
            f"-XX:HeapDumpPath={heap_dump}",  # Heap dump location
            f"-Xlog:safepoint=info:file={jvm_log}:time,uptime,level,tags",  # Safepoint pauses
            f"-Xlog:class+load=info:file={jvm_log}:time,uptime,level,tags",  # Class loading events
            f"-XX:ErrorFile={error_log}",  # Fatal error log location
            "-XX:NativeMemoryTracking=summary",  # Native memory tracking
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
