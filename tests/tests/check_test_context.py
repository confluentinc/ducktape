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

from ducktape.tests.test import Test, TestContext
from ducktape.services.service import Service
from ducktape.mark import parametrize
from ducktape.mark import MarkedFunctionExpander

from tests.ducktape_mock import session_context


class CheckTestContext(object):
    def check_copy_constructor(self):
        """Regression test against a bug introduced in 0.3.7
        The TestContext copy constructor was copying the ServiceRegistry object by reference.
        As a result, services registering themselves with one test context would be registered with the copied
        context as well, resulting in the length of the service registry to grow additively from test to test.

        This problem cropped up in particular with parametrized tests.
        """
        expander = MarkedFunctionExpander(session_context=session_context(), cls=DummyTest, function=DummyTest.test_me)
        ctx_list = expander.expand()

        for ctx in ctx_list:
            # Constructing an instance of the test class causes a service to be registered with the test context
            ctx.cls(ctx)

        # Ensure that each context.services object is a unique reference
        assert len(set(id(ctx.services) for ctx in ctx_list)) == len(ctx_list)


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