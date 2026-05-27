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

from ducktape.tests.test import Test
from ducktape.tests.test_context import TestContext
from ducktape.services.service import Service
from ducktape.mark import parametrize
from ducktape.mark.mark_expander import MarkedFunctionExpander

from tests.ducktape_mock import session_context

from mock import MagicMock


class CheckTestContext(object):
    def check_copy_constructor(self):
        """Regression test against a bug introduced in 0.3.7
        The TestContext copy constructor was copying the ServiceRegistry object by reference.
        As a result, services registering themselves with one test context would be registered with the copied
        context as well, resulting in the length of the service registry to grow additively from test to test.

        This problem cropped up in particular with parametrized tests.
        """
        expander = MarkedFunctionExpander(
            session_context=session_context(),
            cls=DummyTest,
            function=DummyTest.test_me,
            cluster=MagicMock(),
        )
        ctx_list = expander.expand()

        for ctx in ctx_list:
            # Constructing an instance of the test class causes a service to be registered with the test context
            ctx.cls(ctx)

        # Ensure that each context.services object is a unique reference
        assert len(set(id(ctx.services) for ctx in ctx_list)) == len(ctx_list)


class CheckResultsDirLayout(object):
    """Verify the on-disk layout produced by TestContext.results_dir() in both the legacy
    flat-basename form and the nested per-parameter form gated by --nested-result-dirs.
    """

    def _ctx(self, injected_args=None, nested=False):
        sc = session_context(nested_result_dirs=nested)
        return TestContext(
            session_context=sc,
            file="tests/ducktape_mock.py",
            module=__name__,
            cls=DummyTest,
            function=DummyTest.test_me,
            cluster=MagicMock(),
            injected_args=injected_args,
        )

    def _suffix(self, ctx, test_index=None):
        """Return the path under the session root."""
        full = TestContext.results_dir(ctx, test_index)
        base = ctx.session_context.results_dir
        return os.path.relpath(full, base)

    def check_flat_default(self):
        ctx = self._ctx(injected_args={"x": 1, "y": 2}, nested=False)
        assert self._suffix(ctx) == os.path.join("DummyTest", "test_me", "x=1.y=2")

    def check_flat_preserves_insertion_order(self):
        # Flat form keeps the historical insertion order (used by test_id/test_name).
        ctx = self._ctx(injected_args={"b": 1, "a": 2}, nested=False)
        assert self._suffix(ctx) == os.path.join("DummyTest", "test_me", "b=1.a=2")

    def check_flat_no_args(self):
        # injected_args=None must not append any segment under the method dir.
        ctx = self._ctx(injected_args=None, nested=False)
        assert self._suffix(ctx) == os.path.join("DummyTest", "test_me")

    def check_flat_empty_args(self):
        # injected_args={} must also be a no-op (preserves legacy behavior even
        # though the empty-dict path goes through injected_args_name -> "").
        ctx = self._ctx(injected_args={}, nested=False)
        assert self._suffix(ctx) == os.path.join("DummyTest", "test_me")

    def check_nested_basic(self):
        ctx = self._ctx(injected_args={"a": 1, "b": 2, "c": 3}, nested=True)
        assert self._suffix(ctx) == os.path.join("DummyTest", "test_me", "a=1", "b=2", "c=3")

    def check_nested_sorts_keys(self):
        # Insertion order must not affect on-disk layout when nesting.
        ctx_in = self._ctx(injected_args={"c": 3, "a": 1, "b": 2}, nested=True)
        ctx_out = self._ctx(injected_args={"a": 1, "b": 2, "c": 3}, nested=True)
        assert self._suffix(ctx_in) == self._suffix(ctx_out)

    def check_nested_no_args(self):
        ctx = self._ctx(injected_args=None, nested=True)
        assert self._suffix(ctx) == os.path.join("DummyTest", "test_me")

    def check_nested_empty_args(self):
        ctx = self._ctx(injected_args={}, nested=True)
        assert self._suffix(ctx) == os.path.join("DummyTest", "test_me")

    def check_nested_single_arg(self):
        ctx = self._ctx(injected_args={"x": 1}, nested=True)
        assert self._suffix(ctx) == os.path.join("DummyTest", "test_me", "x=1")

    def check_nested_with_test_index(self):
        ctx = self._ctx(injected_args={"a": 1, "b": 2}, nested=True)
        assert self._suffix(ctx, test_index=3) == os.path.join("DummyTest", "test_me", "a=1", "b=2", "3")

    def check_nested_escapes_per_segment(self):
        # Slashes and whitespace inside a value must be sanitized within a single segment,
        # not allowed to split into extra directory levels.
        ctx = self._ctx(injected_args={"path": "/etc/foo bar"}, nested=True)
        # _escape_pathname collapses whitespace and replaces '/' with '.'
        assert self._suffix(ctx) == os.path.join("DummyTest", "test_me", "path=.etc.foobar")

    def check_injected_args_name_unaffected_by_flag(self):
        # The test_id-bearing dotted string must be identical regardless of layout flag.
        ctx_flat = self._ctx(injected_args={"b": 1, "a": 2}, nested=False)
        ctx_nest = self._ctx(injected_args={"b": 1, "a": 2}, nested=True)
        assert ctx_flat.injected_args_name == ctx_nest.injected_args_name == "b=1.a=2"


class DummyTest(Test):
    def __init__(self, test_context):
        super(DummyTest, self).__init__(test_context)
        self.service = DummyService(test_context)

    @parametrize(x=1)
    @parametrize(x=2)
    def test_me(self):
        pass


class DummyService(Service):
    def __init__(self, context):
        super(DummyService, self).__init__(context, 1)
