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
Post-test hook mechanism for ducktape.

This module provides a lightweight hook system that allows registering callbacks
to run after each test's logs are collected but before the test result is finalized.

Usage:
    Configure hooks via globals.json:
    {
        "post_test_hooks": [
            "mypackage.hooks.my_hook_function",
            "another.module.another_hook"
        ]
    }

    Each hook function should have the signature:
        def my_hook(results_dir, test_id, test_status, logger, globals) -> PostTestHookResult
"""

import importlib
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable


@dataclass
class PostTestHookResult:
    """Result returned by a post-test hook.

    Attributes:
        success: Whether the hook executed successfully
        fail_test: If True, mark the test as FAIL (only applies if test was PASS)
        summary_additions: Lines to append to the test summary
        data: Data to merge into the test result's data field
    """
    success: bool = True
    fail_test: bool = False
    summary_additions: Optional[List[str]] = None
    data: Optional[Dict[str, Any]] = None


def import_hook(hook_path: str) -> Callable:
    """Import a hook function from a dotted module path.

    Args:
        hook_path: Dotted path to the hook function, e.g., 'mypackage.hooks.my_hook'

    Returns:
        The hook function

    Raises:
        ImportError: If the module cannot be imported
        AttributeError: If the function doesn't exist in the module
    """
    module_path, func_name = hook_path.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


def run_post_test_hooks(hook_paths: List[str],
                        results_dir: str,
                        test_id: str,
                        test_status: 'TestStatus',
                        logger: logging.Logger,
                        globals_dict: Optional[Dict[str, Any]] = None) -> List[PostTestHookResult]:
    """Execute all configured post-test hooks.

    Args:
        hook_paths: List of dotted paths to hook functions
        results_dir: Path to the test's results directory
        test_id: Full test identifier
        test_status: Current test status (PASS/FAIL/FLAKY/IGNORE)
        logger: Logger for output
        globals_dict: Session globals dictionary

    Returns:
        List of PostTestHookResult from each hook
    """
    results = []

    for hook_path in hook_paths:
        try:
            logger.debug(f"Running post-test hook: {hook_path}")
            hook_fn = import_hook(hook_path)
            result = hook_fn(
                results_dir=results_dir,
                test_id=test_id,
                test_status=test_status,
                logger=logger,
                globals=globals_dict or {}
            )

            if not isinstance(result, PostTestHookResult):
                logger.warning(f"Hook {hook_path} returned {type(result)}, expected PostTestHookResult. "
                             "Wrapping in default result.")
                result = PostTestHookResult(success=True)

            results.append(result)

            if result.fail_test:
                logger.info(f"Hook {hook_path} requests test failure")
            if result.summary_additions:
                logger.debug(f"Hook {hook_path} adds to summary: {result.summary_additions}")

        except Exception as e:
            logger.error(f"Error running post-test hook {hook_path}: {e}")
            results.append(PostTestHookResult(
                success=False,
                summary_additions=[f"Post-test hook error ({hook_path}): {str(e)}"]
            ))

    return results
